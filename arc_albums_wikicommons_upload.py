#!/usr/bin/env python
from datapackage_pipelines.wrapper import ingest, spew
import logging
from pipeline_params import get_pipeline_param_rows
from google.cloud import storage
from contextlib import contextmanager
from tempfile import mkdtemp
import os
import pywikibot
import time
import datetime
import sys
from tabulator import Stream
import requests


LICENSE_TEMPLATE = "PD-Israel"
SUPPORTED_TEMPLATE = "Supported by Wikimedia Israel|year=2018"
FILES_CATEGORY = "Files from ISA uploaded by Wikimedia Israel"
FILES_CATEGORY_ID = "Files_from_ISA_uploaded_by_Wikimedia_Israel"



DESCRIPTION_TEMPLATE=lambda pic_id, description, year, author: """=={{int:filedesc}}==
{{Information
|description={{he|1=__DESCRIPTION__}}
|date=__YEAR__
|source={{he|1=ארכיון המדינה}}
|author={{he|1=__AUTHOR__}}
|permission=
|other versions=
|other fields={{Information field|Name=ISA Collection Identifier|Value=__PIC_ID__}}
}}
=={{int:license-header}}==
{{PD-Israel}}
{{Supported by Wikimedia Israel|year=2018}}
[[Category:Files from ISA uploaded by Wikimedia Israel]]""".replace("__YEAR__", year) \
    .replace("__PIC_ID__", pic_id) \
    .replace("__DESCRIPTION__", description) \
    .replace("__AUTHOR__", author)


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
    delay_seconds = int(os.environ.get("THROTTLE_SECONDS", "15")) if not delay_seconds else delay_seconds
    if hasattr(throttle, 'last_call'):
        seconds_since_last_call = (datetime.datetime.now() - throttle.last_call).seconds
        if seconds_since_last_call < delay_seconds:
            logging.info("throttling {} seconds...".format(delay_seconds - seconds_since_last_call))
            time.sleep(delay_seconds - seconds_since_last_call)
    yield
    throttle.last_call = datetime.datetime.now()


def get_file_name(row, pic_id):
    name = row["name"]
    for s in ['[', ']']:
        name = name.replace(s, '')
    return name[:80] + "-{}.jpg".format(pic_id)


def get_description(row):
    return row['albumName'] + ': ' + row['name']


def get_year(row):
    return row['objectDatingStart'].split('-')[0]


def get_author(row):
    return row['archiveName']


def upload(row, pic_id, stats, retry_num=0):
    logging.info(pic_id)
    stats["last pic_id"] = pic_id
    if os.environ.get('WIKISCRAPER_DRY_RUN'):
        site = None
    else:
        site = pywikibot.Site()
        site.login()
    with temp_file() as filename:
        with open(filename, 'wb') as f:
            f.write(requests.get(row['attachmentUrl'].replace('storage.archive.gov', 'storage.archives.gov')).content)
        logging.info(os.path.getsize(filename))
        if os.environ.get('WIKISCRAPER_DRY_RUN'):
            page = None
        else:
            page = pywikibot.FilePage(site, get_file_name(row, pic_id))
            assert page.site.family == 'commons', 'invalid page site: {}'.format(page.site)
        page_text = DESCRIPTION_TEMPLATE(pic_id, get_description(row), get_year(row), get_author(row))
        try:
            if os.environ.get('WIKISCRAPER_DRY_RUN'):
                logging.info(" -- {} -- \n{}".format(filename, page_text))
            else:
                with throttle():
                    if not page.exists():
                        page.text = page_text
                        if page.upload(filename, comment="uploaded by wmilbot", ignore_warnings=True):
                            logging.info("----- {} uploaded successfully".format(pic_id))
                            stats["num uploaded"] += 1
                        else:
                            raise Exception("Upload failed")
                    else:
                        page.get()
                        page.text = page_text
                        page.save(summary='update by wmilbot')
                        logging.info('----- {} updated successfully'.format(pic_id))
                        stats["num updated"] += 1
        except Exception:
            if False:
                logging.exception("Upload failed, retrying...")
                time.sleep(.1 + (retry_num * 5))
                return upload(row, pic_id, stats, retry_num + 1)
            else:
                raise
    return True, ''


def ingest_spew():
    raise NotImplementedError


def cli_upload(start_donum, upload_limit=1):
    reached_start_at = False if start_donum else True
    num_uploaded = 0
    stats = {"num uploaded": 0,
             "num updated": 0}
    with Stream('final-data/arc/joined_albums-modified-1jul-2018.csv', headers=1) as stream:
        for row in stream.iter(keyed=True):
            pic_id = row['objectReference'].replace('ISA-Collections-', '') + '-' + row['objectId']
            if reached_start_at or pic_id == start_donum:
                reached_start_at = True
                success, error = upload(row, pic_id, stats)
                if success:
                    num_uploaded += 1
                if upload_limit > 0 and num_uploaded >= upload_limit:
                    break
    print(stats)


def cli():
    if len(sys.argv) > 2:
        if sys.argv[2] == 'upload':
            cli_upload(sys.argv[3] if len(sys.argv) > 3 and sys.argv[3] != 'all' else None,
                       0 if len(sys.argv) > 3 and sys.argv[3] == 'all' else 1)
        if sys.argv[2] == 'upload-after':
            cli_upload(sys.argv[3], upload_limit=int(sys.argv[4]) if len(sys.argv) > 4 else 1)


if len(sys.argv) > 1 and sys.argv[1] == '--cli':
    cli()
else:
    ingest_spew()
