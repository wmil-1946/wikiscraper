from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import logging, requests


parameters, datapackage, resources, stats = ingest() + ({'children': 0},)


def get_resource():
    parents = {}
    for parent in next(resources):
        assert parent['archive_objectReference'] not in parents
        parents[parent['archive_objectReference']] = parent
    for child in next(resources):
        child_parent = None
        for parent_objectReference, parent in parents.items():
            if child['archive_objectReference'].startswith(parent_objectReference):
                assert not child_parent
                child_parent = parent
        if child['attachmentUrl'].split('.')[-1] == 'pdf':
            continue
        assert child['attachmentUrl'].split('.')[-1] == 'jpg', 'invalid extension: {}'.format(child['attachmentUrl'])
        collection = None
        kluger_negative_numbers = None, None
        if child_parent['name'].startswith('אוסף צילומים של הצלם זולטן קלוגר טווח מספרי התשלילים  -'):
            collection = 'kluger'
        elif child_parent['name'].startswith('אוסף צילומים של הצלם זולטן קלוגר טווח מספרי התשלילים'):
            negs = child_parent['name'].replace('אוסף צילומים של הצלם זולטן קלוגר טווח מספרי התשלילים', '').strip()
            collection = 'kluger'
            kluger_negative_numbers = negs.split(' - ')
            assert len(kluger_negative_numbers) == 2
        elif child_parent['name'] == 'ספינת מעפילים בחוף תל אביב':
            collection = 'kluger'
        elif child_parent['name'] == 'אלבום מצפה':
            collection = 'mitzpeh'
        else:
            raise Exception(child_parent['name'])
        yield {'archiveName': child_parent['archiveName'],
               'objectReference': child_parent['archive_objectReference'],
               'objectId': child['archive_objectReference'].replace(child_parent['archive_objectReference'], ''),
               'attachmentUrl': 'http://storage.archive.gov.il/' + child['attachmentUrl'],
               'albumName': child_parent['name'],
               'name': child['descriptionText'],
               'objectDatingStart': child_parent['objectDatingStart'],
               'objectDatingEnd': child_parent['objectDatingEnd'],
               'collection': collection,
               'klugerNegativeFrom': kluger_negative_numbers[0],
               'klugerNegativeTo': kluger_negative_numbers[1]
               }

spew(dict(datapackage, resources=[{PROP_STREAMING: True,
                                   'name': 'joined_albums',
                                   'path': 'joined_albums.csv',
                                   'schema': {'fields': [{'name': 'archiveName', 'type': 'string'},
                                                         {'name': 'objectReference', 'type': 'string'},
                                                         {'name': 'objectId', 'type': 'string'},
                                                         {'name': 'attachmentUrl', 'type': 'string'},
                                                         {'name': 'albumName', 'type': 'string'},
                                                         {'name': 'name', 'type': 'string'},
                                                         {'name': 'objectDatingStart', 'type': 'string'},
                                                         {'name': 'objectDatingEnd', 'type': 'string'},
                                                         {'name': 'collection', 'type': 'string'},
                                                         {'name': 'klugerNegativeFrom', 'type': 'string'},
                                                         {'name': 'klugerNegativeTo', 'type': 'string'},]}}]),
     [get_resource()], stats)
