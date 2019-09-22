import time
import random
from retrying import retry
from dataflows import Flow, load
import subprocess


# wait 4s, 8s, 16s, 32s and continue with 32s up to 10 minutes
@retry(wait_exponential_multiplier=50, wait_exponential_max=2000, stop_max_delay=60*5*1000)
def do_download_item_pages(urls, output_filename):
    # sleeptime = random.randint(5, 10)/1000
    print('downloading to file {}'.format(output_filename))
    # time.sleep(sleeptime)
    subprocess.check_call('node musportal/download_puppeteer.js -- --urls {} > "{}"'.format(' '.join(['"{}"'.format(u) for u in urls]), output_filename), shell=True)


en_items_urls = []


def download_item_urls(rows):
    for rownum, row in enumerate(rows):
        if rownum < 124:
            continue
        assert row['item_url'] not in en_items_urls and row['item_url']
        do_download_item_pages([row['item_url']], 'data/musportal-item-pages-en-puppeteer/rownum{}.txt'.format(rownum))
        en_items_urls.append(row['item_url'])


Flow(
    load('musportal/.checkpoints/all_page_items_en/datapackage.json'),
    download_item_urls
).process()
