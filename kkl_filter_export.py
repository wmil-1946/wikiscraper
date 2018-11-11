from datapackage_pipelines.wrapper import ingest, spew
import logging
from pipeline_params import get_pipeline_param_rows
from google.cloud import storage


parameters, datapackage, resources = ingest()
aggregations = {"stats": {}}
parameters = next(get_pipeline_param_rows(parameters["pipeline-id"], parameters["pipeline-parameters"]))
consts = next(get_pipeline_param_rows('constants', 'kkl-parameters.csv'))


def get_resource(resource):
    if consts.get("gcs_bucket"):
        # initialize google
        logging.info("syncing to google storage bucket {}".format(consts["gcs_bucket"]))
        gcs = storage.Client.from_service_account_json(consts["gcs_secret_file"])
        gcs_bucket = gcs.get_bucket(consts["gcs_bucket"])
    else:
        # initialize filesystem
        gcs, gcs_bucket = None, None
    for i, row in enumerate(resource):
        if row["date"] and row["date"].year <= int(parameters["max_year"]):
            assert row["image_path"].startswith("/ArchiveTazlumim/TopSmlPathArc/Do")
            assert row["image_path"].endswith(".jpeg")
            donum = row["image_path"].replace("/ArchiveTazlumim/TopSmlPathArc/Do", "").replace(".jpeg", "")
            export_file_path = parameters["out_path"] + "/" + parameters["export_image_path"].format(
                year=row["date"].year,
                source=row["source"],
                description=row["description"],
                donum=donum)
            logging.info("Exporting image to {}".format(export_file_path))
            input_file_path = parameters["image_in_path"] + row["image_path"]
            if gcs_bucket:
                input_blob = gcs_bucket.blob(input_file_path)
                gcs_bucket.copy_blob(input_blob, gcs_bucket, export_file_path)
            else:
                raise NotImplemented()
            yield row


def get_resources(target_resource_num):
    for resource_num, resource in enumerate(resources):
        if resource_num == target_resource_num:
            yield get_resource(resource)
        else:
            yield (row for row in resource)


target_resource_num = None
for resource_num, descriptor in enumerate(datapackage["resources"]):
    if descriptor["name"] == "kkl":
        target_resource_num = resource_num

spew(datapackage, get_resources(target_resource_num), aggregations["stats"])
