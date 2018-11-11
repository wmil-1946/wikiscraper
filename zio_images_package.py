from datapackage_pipelines.wrapper import process
import logging, filecmp, shutil, os


DESCRIPTION = 'תיאור'
YEAR = 'שנה'
ID = 'מזהה'
AUTHOR = 'שם צלם/מוסד'
COPYRIGHT = 'זכויות יוצרים'


all_image_ids = []


def process_row(row, row_index, spec, resource_index, parameters, stats):
    if spec['name'] == 'zio_package':
        filename = parameters['file-path-template'].format(rownum=row_index)
        missing_image = parameters['missing-image']
        if not os.path.exists(filename) or filecmp.cmp(filename, missing_image, shallow=False):
            row = None
        else:
            tmp_image_id = image_id = row['id'].replace('\\', '-').replace('.', '-')
            i = 2
            while tmp_image_id in all_image_ids:
                tmp_image_id = image_id + '-' + str(i)
                i+=1
            image_id = tmp_image_id
            all_image_ids.append(image_id)
            if os.path.exists(filename):
                shutil.copyfile(filename, parameters['out-path'] + image_id + '.jpg')
            row = {DESCRIPTION: row['description'],
                   YEAR: row['year'],
                   ID: image_id,
                   AUTHOR: row['author'],
                   COPYRIGHT: row['copyright']}
    return row


def modify_datapackage(datapackage, parameters, stats):
    for descriptor in datapackage['resources']:
        if descriptor['name'] == 'zio_details':
            descriptor.update(name='zio_package', path='zio_package.csv')
            fields = [{'name': DESCRIPTION, 'type': 'string'},
                      {'name': YEAR, 'type': 'integer'},
                      {'name': ID, 'type': 'string'},
                      {'name': AUTHOR, 'type': 'string'},
                      {'name': COPYRIGHT, 'type': 'string'},]
            descriptor['schema']['fields'] = fields
            os.makedirs(parameters['out-path'], exist_ok=True)
    return datapackage


if __name__ == '__main__':
    process(modify_datapackage, process_row)
