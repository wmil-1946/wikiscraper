from datapackage_pipelines.wrapper import ingest, spew
import logging
import template_functions


parameters, datapackage, resources, stats = ingest() + ({},)


invalid_item_ids = [row["invalid_isa_num"] for row in next(resources)]


def is_valid_row(row):
    if len([iid for iid in invalid_item_ids if iid in row['archive_objectReference']]) > 0:
        return False
    return True


def get_resource():
    all_object_references = []
    for row in next(resources):
        if is_valid_row(row) and row['archive_objectReference'] not in all_object_references:
            all_object_references.append(row['archive_objectReference'])
            yield row


datapackage['resources'][0] = datapackage['resources'][1]
del datapackage['resources'][1]


spew(datapackage, [get_resource()], stats)
