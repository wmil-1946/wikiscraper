from datapackage_pipelines.wrapper import ingest, spew
import os, requests, time, logging
from pipeline_params import get_pipeline_param_rows
from google.cloud import storage


parameters, datapackage, resources = ingest()
aggregations = {"stats": {}}
parameters = next(get_pipeline_param_rows(parameters["pipeline-id"], parameters["pipeline-parameters"]))
consts = next(get_pipeline_param_rows('constants', 'kkl-parameters.csv'))


def get_resource(resource):
    session = requests.session()

    if consts.get("gcs_bucket"):
        # initialize google
        logging.info("syncing to google storage bucket {}".format(consts["gcs_bucket"]))
        gcs = storage.Client.from_service_account_json(consts["gcs_secret_file"])
        gcs_bucket = gcs.get_bucket(consts["gcs_bucket"])
    else:
        # initialize filesystem
        gcs, gcs_bucket = None, None

    for i, row in enumerate(resource):
        if not parameters.get("download_limit") or i < int(parameters["download_limit"]):
            file_path = parameters["out_path"]+row["image_path"]
            if not gcs_bucket and not os.path.exists(os.path.dirname(file_path)):
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
            url = parameters["base_url"] + row["image_path"]
            if gcs_bucket:
                logging.info("Downloading to GCS {} -> gs://{}/{}".format(url, consts["gcs_bucket"], file_path))
                blob = gcs_bucket.blob(file_path)
                blob.upload_from_string(session.get(url).content)
            else:
                logging.info("Downloading to local file {} -> {}".format(url, file_path))
                with open(file_path, "wb") as f:
                    f.write(session.get(url).content)
            logging.info("Sleeping {} seconds".format(parameters["sleep_time"]))
            time.sleep(int(parameters["sleep_time"]))
            yield {"downloaded_image_path": row["image_path"]}


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
        descriptor.update(name="kkl_downloaded_images",
                          path="kkl_downloaded_images.csv",
                          schema={"fields": [{"name": "downloaded_image_path", "type": "string"}]})

spew(datapackage, get_resources(target_resource_num), aggregations["stats"])
