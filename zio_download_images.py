from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMED_FROM
from os import path
import filecmp
from collections import defaultdict


def main():
    parameters, datapackage, resources, stats = ingest() + (defaultdict(int),)
    max_year = parameters.get('max-year')
    file_path_template = parameters.get('file-path-template')
    missing_image = parameters.get('missing-image')
    datapackage['resources'] = []
    for resource in resources:
        for rownum, row in enumerate(resource):
            if max_year and row['year'] > max_year:
                stats['invalid year'] += 1
                continue
            if parameters.get('download-thumbnails'):
                if not row['thumb_url']:
                    stats['missing thumb_url'] += 1
                    continue
                name = 'rownum_{}'.format(rownum)
                if file_path_template:
                    photo_filename = file_path_template.format(rownum=rownum)
                    if not path.exists(photo_filename):
                        stats['full size photo missing'] += 1
                        continue
                    if missing_image:
                        if filecmp.cmp(photo_filename, missing_image, shallow=False):
                            stats['photo is missing_image photo'] += 1
                            continue
                stats['valid thumbnail'] += 1
                url = row['thumb_url']
                datapackage['resources'].append({PROP_STREAMED_FROM: url,
                                                 'name': name,
                                                 'path': ['files/' + name + '.jpg'],})
            else:
                if row['image_url']:
                    url = parameters['image_url_prefix'] + row['image_url']
                    name = 'rownum_{}'.format(rownum)
                    datapackage['resources'].append({PROP_STREAMED_FROM: url,
                                                     'name': name,
                                                     'path': ['files/' + name + '.png'],})

    spew(datapackage, [], stats)


if __name__ == '__main__':
    main()
