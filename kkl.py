from selenium import webdriver
from pipeline_params import get_pipeline_param_rows
from pipeline_logs import log_successful_pipeline_execution
import os, logging
from google.cloud import storage

logging.basicConfig(level=logging.INFO)


params = next(get_pipeline_param_rows('kkl.py', 'kkl-parameters.csv'))
consts = next(get_pipeline_param_rows('constants', 'kkl-parameters.csv'))

stats = {"num_wrote_pages": 0}

# initialize webdriver
chrome_options = webdriver.ChromeOptions()
prefs = {'download.default_directory': params['out_path']}
chrome_options.add_experimental_option("prefs", prefs)
chrome_options.add_argument('--headless')
browser = webdriver.Chrome(chrome_options=chrome_options)

# get the first page
browser.get(params['first_page_url'])
first_page_source = browser.page_source

if consts.get("gcs_bucket"):
    # initialize google
    logging.info("syncing to google storage bucket {}".format(consts["gcs_bucket"]))
    gcs = storage.Client.from_service_account_json(consts["gcs_secret_file"])
    gcs_bucket = gcs.get_bucket(consts["gcs_bucket"])
else:
    # initialize filesystem
    gcs, gcs_bucket = None, None
    os.makedirs(params['out_path'], exist_ok=True)

index_filepath = os.path.join(params['out_path'], "index.html")
if gcs_bucket:
    blob = gcs_bucket.blob(index_filepath)
    blob.upload_from_string(first_page_source)
else:
    with open(index_filepath, "w") as f:
        f.write(first_page_source)
stats["num_wrote_pages"] += 1
logging.info("wrote {}".format(index_filepath))

# you can find out the last page number by opening browser console and trying out a very large page number
# __doPostBack('AddLineTazlumCtrl1$GridView1','Page$9999')
# then scroll to the bottom of the page and you will see the last page number
logging.info("downloading pages {} - {}".format(params['second_page_num'], params['last_page_num']))
for i in range(int(params['second_page_num']), int(params['last_page_num'])+1):
    logging.info("executing script")
    browser.execute_script("__doPostBack('AddLineTazlumCtrl1$GridView1','Page${}')".format(i))
    page_filepath = os.path.join(params['out_path'], "page{}.html".format(i))
    if gcs_bucket:
        blob = gcs_bucket.blob(page_filepath)
        blob.upload_from_string(browser.page_source)
    else:
        with open(page_filepath, "w") as f:
            f.write(browser.page_source)
    stats["num_wrote_pages"] += 1
    logging.info("wrote {}".format(page_filepath))

browser.quit()

log_successful_pipeline_execution('kkl.py', 'kkl-log.csv', stats, params)
