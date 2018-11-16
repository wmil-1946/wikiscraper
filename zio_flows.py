from glob import glob
from collections import defaultdict
from dataflows import Flow, load, checkpoint, printer
from datapackage import Package


def details_removed_watermark(scrape_details_datapackage, removed_watermark_files_path):
    DETAILS_URL_PREFIX = 'http://www.zionistarchives.org.il/Pages/'

    def process_rows(rows):
        valid_rownums = [int(f.split('_')[2].split('.')[0]) for f in glob(f'{removed_watermark_files_path}/*')
                         if f.startswith(f'{removed_watermark_files_path}/rownum_')]
        stats = defaultdict(int)
        do_it = rows.res.name == 'zio_details'
        for rownum,row in enumerate(rows):
            if do_it:
                if rownum in valid_rownums:
                    row['rownum'] = rownum
                    details_url = row['details_url']
                    row['details_url'] = f'{DETAILS_URL_PREFIX}{details_url}'
                    row['removed_watermark_file'] = f'{removed_watermark_files_path}/rownum_{rownum}.png'
                    stats['rows_with_valid_photo'] += 1
                    yield row
                else:
                    stats['rows_with_invalid_photo'] += 1
            else:
                yield row
        print(dict(stats))

    def update_descriptor(package):
        package_descriptor = package.pkg.descriptor
        for resource_descriptor in package_descriptor['resources']:
            if resource_descriptor['name'] == 'zio_details':
                resource_descriptor['schema']['fields'].append({'name': 'removed_watermark_file', 'type': 'string'})
        yield Package(package_descriptor)
        yield from package

    return Flow(load(scrape_details_datapackage),
                process_rows,
                update_descriptor)
