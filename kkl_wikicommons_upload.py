#!/usr/bin/env python
from datapackage_pipelines.wrapper import ingest, spew
import logging, collections
from pipeline_params import get_pipeline_param_rows
from google.cloud import storage
from contextlib import contextmanager
from tempfile import mkdtemp
import os
import pywikibot
import time
from pywikibot.pagegenerators import GeneratorFactory
import datetime
from pywikibot.specialbots import UploadRobot
from pywikibot.data.api import APIError
import sys
from datapackage import Package


LICENSE_TEMPLATE = "PD-Israel"
SUPPORTED_TEMPLATE = "Supported by Wikimedia Israel|year=2018"
FILES_CATEGORY = "Files from JNF uploaded by Wikimedia Israel"
FILES_CATEGORY_ID = "Files_from_JNF_uploaded_by_Wikimedia_Israel"



DESCRIPTION_TEMPLATE=lambda description, datestring, source, author, jnfnum: """=={{int:filedesc}}==
{{Information
|description={{he|1=__DESCRIPTION__}}
|date=__DATESTRING__
|source={{he|1=__SOURCE__}}
|author={{he|1=__AUTHOR__}}
|permission=
|other versions=
|other fields={{Information field|Name=JNF Number|Value=__JNFNUM__}}
}}
=={{int:license-header}}==
{{__LICENSE__}}
{{__SUPPORTED__}}
[[Category:__FILESCAT__]]""".replace("__DATESTRING__", datestring) \
                            .replace("__SOURCE__", source) \
                            .replace("__AUTHOR__", author) \
                            .replace("__JNFNUM__", jnfnum) \
                            .replace("__DESCRIPTION__", description) \
                            .replace("__LICENSE__", LICENSE_TEMPLATE) \
                            .replace("__SUPPORTED__", SUPPORTED_TEMPLATE) \
                            .replace("__FILESCAT__", FILES_CATEGORY) \


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


@contextmanager
def throttle(delay_seconds=None):
    delay_seconds = int(os.environ.get("THROTTLE_SECONDS", "1")) if not delay_seconds else delay_seconds
    if hasattr(throttle, 'last_call'):
        seconds_since_last_call = (datetime.datetime.now() - throttle.last_call).seconds
        if seconds_since_last_call < delay_seconds:
            logging.info("throttling {} seconds...".format(delay_seconds - seconds_since_last_call))
            time.sleep(delay_seconds - seconds_since_last_call)
    yield
    throttle.last_call = datetime.datetime.now()


def delete_page(page):
    with throttle():
        page.delete(reason="Deleting duplicated images created by bot", prompt=True, mark=True)


def get_gcs_bucket(consts):
    logging.info("uploading from google storage bucket {}".format(consts["gcs_bucket"]))
    gcs = storage.Client.from_service_account_json(consts["gcs_secret_file"])
    return gcs.get_bucket(consts["gcs_bucket"])


def init_stats():
    stats = {}
    stats["num eligible for download"] = 0
    stats["invalid resolution"] = 0
    stats["invalid description"] = 0
    stats["invalid source"] = 0
    stats["invalid year"] = 0
    stats["in skip list"] = 0
    stats["skipped start at"] = 0
    stats['invalid image_path'] = 0
    return stats


def get_donum_from_row(row):
    return row["image_path"].replace("/ArchiveTazlumim/TopSmlPathArc/Do", "").replace(".jpeg", "")


def is_valid_row(row, stats):
    if row["width_px"] * row["height_px"] < 200 * 200:
        stats["invalid resolution"] += 1
        logging.info('invalid resolution: {} X {}'.format(row["width_px"], row["height_px"]))
        return False
    elif len(row["description"]) < 3:
        stats["invalid description"] += 1
        logging.info('invalid description: {}'.format(row["description"]))
        return False
    elif len(row["source"]) < 2:
        stats["invalid source"] += 1
        logging.info('invalid source: {}'.format(row["source"]))
        return False
    elif row["date"].year > 1947:
        stats["invalid year"] += 1
        logging.info('invalid year: {}'.format(row["date"]))
        return False
    elif 'mi:' in row['image_path']:
        stats['invalid image_path'] += 1
        logging.info('invalid image path: {}'.format(row['image_path']))
        return False
    else:
        stats["num eligible for download"] += 1
        return True


# def load_datapackage_resources(resources, stats):
#     logging.info("Loading datapackage resources...")
#     donums = {}
#     start_at_donum = False
#     reached_start_at = False
#     for resource in resources:
#         for row_num, row in enumerate(resource, start=1):
#             donum = get_donum_from_row(row)
#             if start_at_donum and not reached_start_at and donum != start_at_donum:
#                 stats["skipped start at"] += 1
#             elif is_valid_row(row, stats):
#                 if start_at_donum and donum == start_at_donum:
#                     reached_start_at = True
#                 donums[donum] = row
#                 stats["num eligible for download"] += 1
#     return donums


def upload(consts, parameters, row, donum, stats, retry_num=0):
    if is_valid_row(row, stats):
        if os.environ.get('WIKISCRAPER_DRY_RUN'):
            site = None
        else:
            site = pywikibot.Site()
            site.login()
        gcs_bucket = get_gcs_bucket(consts)
        blob = gcs_bucket.blob("data/kkl/images" + row["image_path"])
        with temp_file() as filename:
            blob.download_to_filename(filename)
            logging.info(os.path.getsize(filename))
            page_title = row["description"][:100] + "-JNF{}.jpeg".format(donum)
            logging.info("page_title={}".format(page_title))
            if os.environ.get('WIKISCRAPER_DRY_RUN'):
                page = None
            else:
                page = pywikibot.FilePage(site, page_title)
                assert page.site.family == 'commons', 'invalid page site: {}'.format(page.site)
            page_text = DESCRIPTION_TEMPLATE(row["description"], str(row["date"]), 'ארכיון הצילומים של קק"ל',
                                             row["source"], donum)
            if os.environ.get('WIKISCRAPER_DRY_RUN'):
                logging.info(" -- {} -- \n{}".format(filename, page_text))
            else:
                with throttle():
                    if not page.exists():
                        page.text = page_text
                        if page.upload(filename, comment="uploaded by wmilbot", ignore_warnings=True):
                            logging.info("uploaded successfully")
                        else:
                            raise Exception("Upload failed")
                    else:
                        page.get()
                        page.text = page_text
                        page.save(summary='update by wmilbot')
        return True, ''
    else:
        return True, 'invalid row'


def ingest_spew():
    raise NotImplementedError
    # parameters, datapackage, resources = ingest()
    # parameters = next(get_pipeline_param_rows(parameters["pipeline-id"], parameters["pipeline-parameters"]))
    # consts = next(get_pipeline_param_rows('constants', 'kkl-parameters.csv'))
    # gcs_bucket = get_gcs_bucket(consts)
    # stats = init_stats()
    # for donum, row in load_datapackage_resources(resources, stats).items():
    #     success, error = upload(gcs_bucket, parameters, row, donum)
    #     assert success
    # spew(dict(datapackage, resources=[]), [])


def cli_upload(consts, parameters, start_donum=None, upload_limit=1):
    reached_start_at = False if start_donum else True
    num_uploaded = 0
    package = Package('final-data/kkl/extended-metadata/datapackage.json')
    stats = init_stats()
    for row in package.get_resource('kkl').iter(keyed=True):
        donum = get_donum_from_row(row)
        if reached_start_at or donum == start_donum:
            reached_start_at = True
            success, error = upload(consts, parameters, row, donum, stats)
            if success:
                num_uploaded += 1
            if upload_limit > 0 and num_uploaded >= upload_limit:
                break
    stats['num processed'] = num_uploaded
    stats['last donum'] = donum
    print(stats)


def cli():
    parameters = next(get_pipeline_param_rows('kkl-wikicommons-upload', 'kkl-parameters.csv'))
    consts = next(get_pipeline_param_rows('constants', 'kkl-parameters.csv'))
    if len(sys.argv) > 2:
        if sys.argv[2] == 'upload':
            if len(sys.argv) > 3:
                if sys.argv[3] == 'all':
                    cli_upload(consts, parameters, None, 0)
                else:
                    cli_upload(consts, parameters, sys.argv[3])
            else:
                cli_upload(consts, parameters)
        if sys.argv[2] == 'upload-after':
            cli_upload(consts, parameters, sys.argv[3],
                       upload_limit=int(sys.argv[4]) if len(sys.argv) > 4 else 1)


if len(sys.argv) > 1 and sys.argv[1] == '--cli':
    cli()
else:
    ingest_spew()
