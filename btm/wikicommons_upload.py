from dataflows import Flow, checkpoint, printer, add_field
import pywikibot
from contextlib import contextmanager
import datetime, time, logging, os
import tempfile
import requests


def get_years(rows):
    for row in rows:
        row['year'] = None
        if row['title']:
            for year in range(10, 47):
                if '19' + str(year) in row['title']:
                    if row['year'] is None:
                        row['year'] = '19' + str(year)
                    else:
                        row['year'] = False
            if row['year']:
                yield row


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


def upload(rows):
    dry_run = False
    skip_rownums = [0,1,2,3,4, 500, 501, 2500, 2501, 14497, 14498, 14499, 14502]
    skip_images = [
        'http://www.bitmuna.com//wp-content/gallery/d79bd7a0d7a1d799d799d7aa-d794d79ed7a9d799d797-1/CCh-2_236.jpg',
        'http://www.bitmuna.com//wp-content/gallery/d79bd7a0d7a1d799d799d7aa-d794d79ed7a9d799d797-1/CCh-2_237.jpg',
        'http://www.bitmuna.com//wp-content/gallery/d79bd7a0d7a1d799d799d7aa-d794d79ed7a9d799d797-1/CCh-2_283.jpg',
        'http://www.bitmuna.com//wp-content/gallery/d79bd7a0d7a1d799d799d7aa-d794d79ed7a9d799d797-1/CCh-2_286.jpg',
        'http://www.bitmuna.com//wp-content/gallery/d79bd7a0d7a1d799d799d7aa-d794d79ed7a9d799d797-1/CCh-2_290.jpg',
        'http://www.bitmuna.com//wp-content/gallery/d79bd7a0d7a1d799d799d7aa-d794d79ed7a9d799d797-1/CCh-2_291.jpg',
        'http://www.bitmuna.com//wp-content/gallery/d79bd7a0d7a1d799d799d7aa-d794d79ed7a9d799d797-1/CCh-2_294.jpg',
        'http://www.bitmuna.com//wp-content/gallery/d79bd7a0d7a1d799d799d7aa-d794d79ed7a9d799d797-1/CCh-2_300.jpg',
    ]
    for rownum, row in enumerate(rows):
        if rownum in skip_rownums: continue
        if row['image'] in skip_images: continue
        if rownum < 14497: continue
        yield row
        print(rownum)
        file_page_title = row['title']
        for char in ['"', "'", '[', ']', ',', ';', '>', '?', '<', '/', '\\', '.']:
            file_page_title = file_page_title.replace(char, '')
        file_page_title = file_page_title[:70] + '_btm' + str(rownum)
        page_text = """=={{int:filedesc}}==
{{Information
|description={{he|1=__DESCRIPTION__}}
|date=__YEAR__
|source={{he|1=ביתמונה}}
|author={{he|1=ביתמונה}}
|permission=
|other versions=
}}
=={{int:license-header}}==
{{PD-Israel}}
{{Supported by Wikimedia Israel|year=2019}}
[[Category:Files from Bitmuna Archive uploaded by Wikimedia Israel]]""".\
            replace('__DESCRIPTION__', row['title']).\
            replace('__YEAR__', row['year'])
        if dry_run:
            print('dry run - {} -- {}'.format(file_page_title, page_text))
        else:
            print('uploading {}'.format(file_page_title))
            site = pywikibot.Site()
            site.login()
            page = pywikibot.FilePage(site, file_page_title)
            assert page.site.family == 'commons', 'invalid page site: {}'.format(page.site)
            with throttle():
                if not page.exists():
                    page.text = page_text
                    with tempfile.NamedTemporaryFile() as f:
                        f.write(requests.get(row['image']).content)
                        if page.upload(f.name, comment="uploaded by wmilbot", ignore_warnings=True):
                            print("----- {} uploaded successfully".format(row['image']))
                        else:
                            raise Exception("Upload failed")
                else:
                    page.get()
                    page.text = page_text
                    page.save(summary='update by wmilbot')
                    print('----- {} updated successfully'.format(row['image']))


Flow(
    checkpoint('scraped-site-filtered-years-album-images', checkpoint_path='btm/.checkpoints'),
    add_field('year', 'year'),
    get_years,
    upload
).process()
