from datapackage_pipelines.wrapper import ingest, spew
import logging, collections
from pipeline_params import get_pipeline_param_rows
from google.cloud import storage
from contextlib import contextmanager
from tempfile import mkdtemp
import os
from PIL import Image


@contextmanager
def temp_dir(*args, **kwargs):
    dir = mkdtemp(*args, **kwargs)
    try:
        yield dir
    except Exception:
        if os.path.exists(dir):
            os.rmdir(dir)
        raise


@contextmanager
def temp_file(*args, **kwargs):
    with temp_dir(*args, **kwargs) as dir:
        file = os.path.join(dir, "temp")
        try:
            yield file
        except Exception:
            if os.path.exists(file):
                os.unlink(file)
            raise


parameters, datapackage, resources = ingest()
aggregations = {"stats": {}}
parameters = next(get_pipeline_param_rows(parameters["pipeline-id"], parameters["pipeline-parameters"]))
consts = next(get_pipeline_param_rows('constants', 'kkl-parameters.csv'))


def filter_resource(resource):
    logging.info("syncing to google storage bucket {}".format(consts["gcs_bucket"]))
    gcs = storage.Client.from_service_account_json(consts["gcs_secret_file"])
    gcs_bucket = gcs.get_bucket(consts["gcs_bucket"])
    for row_num, row in enumerate(resource, start=1):
        input_file_path = parameters["image_in_path"] + row["image_path"]
        if gcs_bucket:
            try:
                blob = gcs_bucket.blob(input_file_path)
                with temp_file() as filename:
                    blob.download_to_filename(filename)
                    file_size = os.path.getsize(filename)
                    width, height = Image.open(filename).size
            except Exception:
                logging.exception("Failed to get image data")
                file_size, width, height = 0, 0, 0
            yield dict(row, **{"file_size_bytes": file_size,
                               "width_px": width,
                               "height_px": height})
        else:
            raise NotImplemented()


for descriptor, resource in zip(datapackage["resources"], resources):
    if descriptor["name"] == "kkl":
        kkl_descriptor, kkl_resource = descriptor, resource
    else:
        raise Exception()

kkl_descriptor["schema"]["fields"] += [{"name": "file_size_bytes", "type": "integer"},
                                       {"name": "width_px", "type": "integer"},
                                       {"name": "height_px", "type": "integer"}]

spew(dict(datapackage, resources=[kkl_descriptor]),
     [filter_resource(kkl_resource)],
     aggregations["stats"])
