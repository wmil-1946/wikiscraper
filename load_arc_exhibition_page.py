from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pyquery import PyQuery as pq
import datetime, requests, logging


parameters, datapackage, resources = ingest()
aggregations = {"stats": {}}


raise Exception("TODO: processor is not fully operational yet")


# http://www.archives.gov.il/search/?q=%D7%96%D7%95%D7%9C%D7%98%D7%9F+%D7%A7%D7%9C%D7%95%D7%92%D7%A8&search_type=images&start_period=1897&end_period=1948
# http://www.archives.gov.il/%D7%9B%D7%9C-%D7%94%D7%AA%D7%A2%D7%A8%D7%95%D7%9B%D7%95%D7%AA/


def get_resource():
    main_page = pq(requests.get(parameters["url"]).content)
    for a in main_page.find("a"):
        logging.info(pq(a).attr("href"))

    page = pq(requests.get(parameters["url"]).content)
    for image in page.find(".img-box .image-parent .image-child"):
        row = {"title": None, "datestring": None, "image": None}
        imgs = pq(image).find('img')
        if len(imgs) > 0:
            row["image"] = pq(imgs[0]).attr("src")
        item_dates = pq(image).find('.exb-item-date')
        if len(item_dates) > 0:
            row["datestring"] = pq(item_dates[0]).text()
        img_titles = pq(image).find('.exb-img-title')
        if len(img_titles) > 0:
            row["title"] = pq(img_titles[0]).text()
        logging.info(row)
        yield row



datapackage["resources"].append({PROP_STREAMING: True,
                                 "name": "arc",
                                 "path": "arc.csv",
                                 "schema": {"fields": [{"name": "datestring", "type": "string"},
                                                       {"name": "title", "type": "string"},
                                                       {"name": "image_url", "type": "string"}]}})


spew(datapackage, [get_resource()], aggregations["stats"])