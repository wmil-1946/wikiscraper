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
stats = {}
aggregations = {"stats": stats}
# parameters = next(get_pipeline_param_rows(parameters["pipeline-id"], parameters["pipeline-parameters"]))
consts = next(get_pipeline_param_rows('constants', 'arc-parameters.csv'))


def filter_resource(resource):
    logging.info("syncing to google storage bucket {}".format(consts["gcs_bucket"]))
    gcs = storage.Client.from_service_account_json(consts["gcs_secret_file"])
    gcs_bucket = gcs.get_bucket(consts["gcs_bucket"])
    for row_num, row in enumerate(resource, start=1):
        if gcs_bucket:
            input_file_path = row["gcs_url"].replace("gs://{}/".format(consts["gcs_bucket"]), "")
            _, ext = os.path.splitext(input_file_path)
            ext_stat = "total {} files".format(ext)
            stats.setdefault(ext_stat, 0)
            stats[ext_stat] += 1
            logging.info("{}: {}".format(ext_stat, stats[ext_stat]))
            if ext == '.pdf':
                logging.info('skipping pdf file')
                continue
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


arc_descriptor, arc_resource = None, None
for descriptor, resource in zip(datapackage["resources"], resources):
    if descriptor["name"] == "joined":
        arc_descriptor, arc_resource = descriptor, resource
    else:
        for row in resource:
            pass

arc_descriptor.update(name="arc", path="arc.csv")
arc_descriptor["schema"]["fields"] += [{"name": "file_size_bytes", "type": "integer"},
                                       {"name": "width_px", "type": "integer"},
                                       {"name": "height_px", "type": "integer"}]

spew(dict(datapackage, resources=[arc_descriptor]),
     [filter_resource(arc_resource)],
     aggregations["stats"])
