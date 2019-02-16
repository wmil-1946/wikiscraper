import os
import time
import random
from requests_html import HTMLSession
from retrying import retry
from dataflows import Flow, load, printer, dump_to_path, add_field


# wait 4s, 8s, 16s, 32s and continue with 32s up to 10 minutes
@retry(wait_exponential_multiplier=4000, wait_exponential_max=32000, stop_max_delay=60*10*1000)
def download_item_page(session, url):
    sleeptime = random.randint(200, 2000)/1000
    print('downloading {} after sleep of {} seconds'.format(url, sleeptime))
    time.sleep(sleeptime)
    r = session.get(url)
    if 'window.rbzid=' in r.text:
        print('rbz block detected, attempting render')
        r.html.render(wait=5, sleep=5)
        raise Exception()
    return r.status_code, r.text


def download_item_pages(rows):
    session = HTMLSession()
    os.makedirs('data/musportal-item-pages-en/files', exist_ok=True)
    for rownum, row in enumerate(rows):
        filename = 'data/musportal-item-pages-en/files/rownum_{}.html'.format(rownum)
        if os.path.exists(filename):
            print('file exists: {}'.format(filename))
            row['downloaded_status_code'] = None
            row['downloaded_html_length'] = None
            row['downloaded_file_name'] = filename
        else:
            status_code, html_content = download_item_page(session, row['item_url'])
            with open(filename, 'w') as f:
                f.write(html_content)
            print('saved file: {}'.format(filename))
            row['downloaded_status_code'] = status_code
            row['downloaded_html_length'] = len(html_content)
            row['downloaded_file_name'] = filename
        yield row


print(Flow(
    load('musportal/.checkpoints/all_page_items_en/datapackage.json'),
    add_field('downloaded_status_code', 'integer'),
    add_field('downloaded_html_length', 'integer'),
    add_field('downloaded_file_name', 'string'),
    download_item_pages,
    printer(),
    dump_to_path('data/musportal-item-pages-en'),
).process()[1])
