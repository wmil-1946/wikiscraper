from datapackage_pipelines.wrapper import process
from pyquery import PyQuery as pq
import logging, requests


def process_row(row, row_index, spec, resource_index, parameters, stats):
    if spec['name'] == 'zio_details':
        if row['details_url']:
            author, copyright, photo_type, errors, dedication = None, None, None, [], None
            try:
                url = parameters['details_url_prefix'] + row['details_url']
                res = requests.get(url)
                res.raise_for_status()
                page = pq(res.content)
                row['item_path'] = pq(page.find('#Path')[0]).text()
                row['image_url'] = pq(page.find('.ImgQueryResultContainer img')[0]).attr('src')
                for div in map(pq, pq(page('#leftQueryResult')).find('div')):
                    text = div.text()
                    if text.startswith('שם צלם/מוסד:'):
                        author = text.replace('שם צלם/מוסד:', '').strip()
                    elif text.startswith('זכויות יוצרים:'):
                        copyright = text.replace('זכויות יוצרים:', '').strip()
                    elif text.startswith('סוג תצלום:'):
                        photo_type = text.replace('סוג תצלום:', '').strip()
                    elif text.startswith('הקדשה:'):
                        dedication = text.replace('הקדשה:', '').strip()
                    else:
                        errors.append('unknown text: {}'.format(text))
            except Exception as e:
                errors.append(str(e))
            row.update(author=author, copyright=copyright, photo_type=photo_type,
                       error=', '.join(errors), dedication=dedication)
        else:
            logging.warning('skipping invalid row {}'.format(row_index))
            row = None
    return row


def modify_datapackage(datapackage, parameters, stats):
    for descriptor in datapackage['resources']:
        if descriptor['name'] == 'zio':
            descriptor.update(name='zio_details', path='zio_details.csv')
            descriptor['schema']['fields'] += [{'name': 'author', 'type': 'string'},
                                               {'name': 'copyright', 'type': 'string'},
                                               {'name': 'item_path', 'type': 'string'},
                                               {'name': 'photo_type', 'type': 'string'},
                                               {'name': 'image_url', 'type': 'string'},
                                               {'name': 'error', 'type': 'string'},
                                               {'name': 'dedication', 'type': 'string'}]
    return datapackage


if __name__ == '__main__':
    process(modify_datapackage, process_row)
