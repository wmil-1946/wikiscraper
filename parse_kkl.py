from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pyquery import PyQuery as pq
from pipeline_params import get_pipeline_param_rows
import logging, datetime, os
from google.cloud import storage


parameters, datapackage, __ = ingest()
aggregations = {"stats": {}}
parameters = next(get_pipeline_param_rows(parameters["pipeline-id"], parameters["pipeline-parameters"]))
consts = next(get_pipeline_param_rows('constants', 'kkl-parameters.csv'))


def get_resource():
    logging.info("parsing pages {} to {}".format(parameters["first_page_num"], parameters["last_page_num"]))
    if consts.get("gcs_bucket"):
        # initialize google
        logging.info("syncing to google storage bucket {}".format(consts["gcs_bucket"]))
        gcs = storage.Client.from_service_account_json(consts["gcs_secret_file"])
        gcs_bucket = gcs.get_bucket(consts["gcs_bucket"])
    else:
        # initialize filesystem
        gcs, gcs_bucket = None, None
    for i in range(int(parameters["first_page_num"]), int(parameters["last_page_num"])+1):
        if i == 1:
            in_filepath = os.path.join(parameters["in_path"], "index.html")
        else:
            in_filepath = os.path.join(parameters["in_path"], "page{}.html".format(i))
        if gcs_bucket:
            blob = gcs_bucket.blob(in_filepath)
            in_file_content = blob.download_as_string()
        else:
            with open(in_filepath) as f:
                in_file_content = f.read()
        page = pq(in_file_content)
        for tr in page("#AddLineTazlumCtrl1_GridView1").find("tr"):
            tds = pq(tr).find("td")
            texts = []
            for i, td in enumerate(tds):
                txt = pq(td).text()
                if txt and i > 0:
                    texts.append(txt)
            description, source, imgdate = None, None, None
            if len(texts) > 0:
                description = texts[0]
            if len(texts) > 1:
                source = texts[1]
            try:
                if len(texts) > 2:
                    datestr = texts[2]
                    imgdate = datetime.datetime.strptime(datestr, "%d/%m/%Y")
            except:
                pass
            imgs = pq(tr).find("img")
            image_path = None
            if len(imgs) > 0:
                image_path = pq(imgs[0]).attr("src")
            if imgdate and description and source and image_path:
                yield {"description": description, "source": source, "image_path": image_path, "date": imgdate}



datapackage["resources"].append({PROP_STREAMING: True,
                                 "name": "kkl",
                                 "path": "kkl.csv",
                                 "schema": {"fields": [{"name": "description", "type": "string"},
                                                       {"name": "source", "type": "string"},
                                                       {"name": "date", "type": "date"},
                                                       {"name": "image_path", "type": "string"}]}})

spew(datapackage, [get_resource()], aggregations["stats"])