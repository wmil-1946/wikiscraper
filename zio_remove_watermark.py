from PIL import Image
from dataflows import PackageWrapper, load, dump_to_path
from collections import defaultdict
from os import path
import filecmp
from os import makedirs
from os import environ
from dataflows_serverless.flow import ServerlessFlow, serverless_step


max_year = 1947
data_dir = path.join(environ.get('DATAFLOWS_WORKDIR', '.'), 'data')
photo_path_template = data_dir + '/zio/download_images/files/rownum_{rownum}.png'
thumb_path_template = data_dir + '/zio/download_thumbnails/files/rownum_{rownum}.jpg'
final_path_template = './data/zio/remove_watermark/files/rownum_{rownum}.png'
if 'DATAFLOWS_WORKDIR' in environ:
    missing_image = path.join(data_dir, 'zio-missing-image.png')
    mask_file = path.join(data_dir, 'zio-mask.gif')
else:
    missing_image = 'zio-missing-image.png'
    mask_file = 'zio-mask.gif'
makedirs(path.dirname(final_path_template.format(rownum=0)), exist_ok=True)


def remove_watermark(rows):
    stats = defaultdict(int)
    for rownum, row in enumerate(rows):
        photo_path = photo_path_template.format(rownum=rownum)
        thumb_path = thumb_path_template.format(rownum=rownum)
        final_path = final_path_template.format(rownum=rownum)
        if row['year'] > max_year:
            stats['invalid year'] += 1
        elif not row['thumb_url']:
            stats['missing thumb_url'] += 1
        elif not path.exists(thumb_path):
            stats['thumbnail missing'] += 1
        elif not path.exists(photo_path):
            stats['full size photo missing'] += 1
        elif filecmp.cmp(photo_path, missing_image, shallow=False):
            stats['photo is missing_image photo'] += 1
        else:
            stats['valid photos'] += 1
            mask_image = Image.open(mask_file).convert('RGBA')
            photo_image = Image.open(photo_path)
            thumb_image = Image.open(thumb_path).resize((500, 500), Image.LANCZOS)
            Image.composite(thumb_image, photo_image, mask_image).save(final_path)
            row['final_photo_path'] = final_path
        yield row
    print(stats)


def modify_datapackage(package: PackageWrapper):
    package.pkg.descriptor['resources'][0]['schema']['fields'].append({'name': 'final_photo_path', 'type': 'string'})
    yield package.pkg
    yield from package


ServerlessFlow(load('final-data/zio/scrape_details/datapackage.json', resources=['zio_details']),
               modify_datapackage,
               serverless_step(remove_watermark, 'zio_details'),
               dump_to_path('./data/zio/remove_watermark')).serverless().process()
