from datapackage_pipelines.wrapper import ingest, spew
import logging, requests


parameters, datapackage, resources, stats = ingest() + ({'children': 0},)


def get_resource():
    for row in next(resources):
        split_url = row['attachmentUrl'].split('/')
        json_url = '/'.join(split_url[:-1] + ['FILE-' + split_url[-2] + '.jsn'])
        res = requests.get(json_url)
        if res.status_code == 200:
            data = res.json()
            child_items = []
            for child in data.get('objectHierarchy', {}).get('children', []):
                if child['objectType'] == 'ITEM':
                    child_items.append(child)
            if len(child_items) > 1:
                for child in child_items:
                    childData = requests.get('http://storage.archives.gov.il/' + child['objectUrl']).json()
                    actual_file_attachment = None
                    for attachment in childData['objectHierarchy']['attachment']:
                        if attachment.get('isActualFile'):
                            actual_file_attachment = attachment
                    assert actual_file_attachment
                    yield {'archiveName': data['objectHierarchy']['archiveName'],
                           'archive_objectReference': data['objectHierarchy']['objectReference'] + child['objectId'],
                           'attachmentUrl': actual_file_attachment['attachmentUrl'],
                           'descriptionText': child['objectName'][0]['displayValue'],
                           'gcs_url': None,
                           'name': child['objectName'][0]['displayValue'],
                           'objectDatingEnd': row['objectDatingEnd'],
                           'objectDatingStart': row['objectDatingStart'],
                           'object_shemMekoriHe': child['objectName'][0]['displayValue'],
                           'file_size_bytes': None,
                           'width_px': None,
                           'height_px': None}
                    stats['children'] += 1


spew(datapackage, [get_resource()], stats)
