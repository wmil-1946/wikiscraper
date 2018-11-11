from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pyquery import PyQuery as pq
import datetime, logging, requests, json, time, os
from pipeline_params import get_pipeline_param_rows
from bs4 import BeautifulSoup


source_parameters, datapackage, resources = ingest()
stats = {}
aggregations = {"stats": stats}
parameters = next(get_pipeline_param_rows(source_parameters["pipeline-id"], source_parameters["pipeline-parameters"]))
constants = next(get_pipeline_param_rows('constants', source_parameters['pipeline-parameters']))


session = requests.session()
failed_items = []


def download(url, retry_num=0):
    time.sleep(float(constants["sleep-time"]))
    try:
        return session.get(url).content.decode()
    except Exception:
        if retry_num >= int(constants["max-retries"]):
            raise
        else:
            return download(url, retry_num + 1)


def get_search_term(search_term):
    logging.info("getting search term {}".format(search_term))
    base_url = parameters["base-url"].format(**{"search-term": search_term,
                                                "year-from": parameters["year-from"],
                                                "year-to": parameters["year-to"]})
    start, stop = [int(v) if v else 0 for v in (os.environ.get(PARAM, parameters.get(param)) for param, PARAM
                                                in [("start", "WIKISCRAPER_ARC_START"), ("stop", "WIKISCRAPER_ARC_STOP")])]
    logging.info("stop={}".format(stop))
    num_consecutive_no_result_pages = 0
    num_consecutive_exceptions = 0
    while stop == 0 or start < stop:
        logging.info("start={}".format(start))
        url = base_url + '&start={}'.format(start)
        logging.info(url)
        page = pq(download(url))
        if "לא נמצאו תוצאות עבור החיפוש שחיפשת" in str(page):
            logging.info("-- no results -- ")
            num_consecutive_no_result_pages += 1
            logging.info("{} consecutive no results pages".format(num_consecutive_no_result_pages))
            if num_consecutive_no_result_pages >= 3:
                logging.info("breaking")
                break
        else:
            num_consecutive_no_result_pages = 0
        imgs = {}
        for img in map(pq, page.find('img.large-search-image-image')):
            if img.attr("alt") and img.attr("src"):
                imgs[img.attr("src")] = img.attr("alt")
                stats["parsed_valid_imgs"] += 1
            stats["parsed_imgs"] += 1
        for footer in page.find("footer"):
            hrefs = pq(footer).find(".search-image-read-more")
            href = pq(hrefs[0]).attr("href") if hrefs and len(hrefs) > 0 else None
            # http://www.archives.gov.il/archives/#/Archive/0b071706802fd9dd/File/0b07170685132058
            try:
                archive_id, _, object_id = href.split("/")[-3:]
                json_url = constants["json-object-base-url"].format(archive_id=archive_id, object_id=object_id)
                logging.info("Downloading {}".format(json_url))
                data = json.loads(download(json_url))
                assert len(data["objectHierarchy"]["attachment"]) == 1
                # Archives/0b071706802fd9dd/Files/0b0717068512b017/00071706.81.D2.7D.67.jpg
                attachmentUrl = data["objectHierarchy"]["attachment"][0]["attachmentUrl"]
                attachmentId = attachmentUrl.split("/")[3]
                img_alts = [img_alt for img_src, img_alt in imgs.items() if "/Files/{}".format(attachmentId) in img_src]
                if "objectDating" in data:
                    assert len(data["objectDating"]) == 1
                    yield {"archiveId": data["objectHierarchy"]["archiveId"],
                           "archiveName": data["objectHierarchy"]["archiveName"],
                           "archive_objectReference": data["objectHierarchy"]["objectReference"],
                           "objectId": data["objectHierarchy"]["hierarchyPath"][0]["objectId"],
                           "name": img_alts[0],
                           "descriptionHtml": data.get("objectDescription", {}).get("objectDesc", {}).get("displayValue"),
                           "descriptionText": ''.join(BeautifulSoup(
                               data.get("objectDescription", {}).get("objectDesc", {}).get("displayValue", ""),
                               "lxml").findAll(text=True)),
                           "objectReference": data["objectHierarchy"]["hierarchyPath"][0]["objectReference"],
                           "objectUrl": "http://storage.archives.gov.il/" + data["objectHierarchy"]["hierarchyPath"][0][
                               "objectUrl"],
                           "attachmentUrl": "http://storage.archives.gov.il/" + attachmentUrl,
                           "object_shemMekoriHe": data["additionalAttributes"]["shemMekoriHe"],
                           "objectDatingStart": data["objectDating"][0]["datingPeriodStart"],
                           "objectDatingEnd": data["objectDating"][0]["datingPeriodEnd"],
                           "objectDescription": data.get("objectDescription", {})}
                    stats["valid_items"] += 1
                num_consecutive_exceptions = 0
            except Exception:
                logging.exception("failed to get row for {}".format(href))
                failed_items.append((href, url))
                stats["failed_items"] += 1
                num_consecutive_exceptions += 1
                if num_consecutive_exceptions > 10:
                    logging.info("{} consecutive exceptions, raising the exception".format(num_consecutive_exceptions))
                    raise
            start += 1
            stats["downloaded_items"] += 1
            if stop > 0 and start >= stop:
                break
        stats["downloaded_pages"] += 1


def get_resource():
    stats.update(downloaded_items=0, downloaded_pages=0, valid_items=0, parsed_imgs=0, parsed_valid_imgs=0,
                 failed_items=0)
    for search_term in [s.strip() for s in parameters["search-terms"].split(",")]:
        yield from get_search_term(search_term)


def get_failed_hrefs_resource():
    for href, url in failed_items:
        yield {"href": href, "url": url}


datapackage["resources"].append({PROP_STREAMING: True,
                                 "name": "arc",
                                 "path": "arc.csv",
                                 "schema": {"fields": [{"name": n, "type": t}
                                                       for n, t in [("archiveId", "string"),
                                                                    ("archiveName", "string"),
                                                                    ("archive_objectReference", "string"),
                                                                    ("objectId", "string"),
                                                                    ("name", "string"),
                                                                    ("descriptionHtml", "string"),
                                                                    ("descriptionText", "string"),
                                                                    ("objectReference", "string"),
                                                                    ("objectUrl", "string"),
                                                                    ("attachmentUrl", "string"),
                                                                    ("object_shemMekoriHe", "string"),
                                                                    ("objectDatingStart", "string"),
                                                                    ("objectDatingEnd", "string"),
                                                                    ("objectDescription", "object"),
                                                                    ("objectDescriptionName", "string")]]}})


datapackage["resources"].append({PROP_STREAMING: True,
                                 "name": "failed_hrefs",
                                 "path": "failed_hrefs.csv",
                                 "schema": {"fields": [{"name": "href", "type": "string"},
                                                       {"name": "url", "type": "string"}]}})


spew(datapackage, [get_resource(), get_failed_hrefs_resource()], aggregations["stats"])
