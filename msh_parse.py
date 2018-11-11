from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pyquery import PyQuery as pq
import datetime, logging, requests, json, time, os
from pipeline_params import get_pipeline_param_rows
from bs4 import BeautifulSoup


parameters, datapackage, resources = ingest()


def get_resource():
    all_pic_ids = []
    for i in [1,2,3,4,5]:
        with open('data/msh/source-html/page{}.html'.format(i), 'r') as f:
            page = pq(f.read())
            for rownum, tr in enumerate(map(pq, page.find('#results_table tr'))):
                data = dict(enumerate(map(pq, tr.find('td'))))
                if len(data) > 3:
                    row =  {"pic_id": data[1].text(),
                            "envelope_17": data[2].text(),
                            "envelope": data[3].text(),
                            "description": data[4].text(),
                            "participants": data[5].text(),
                            "place": data[6].text(),
                            "comment": data[7].text(),
                            "year": data[8].text(),
                            "image_url": pq(data[9].find('a')).attr('href'),}
                    if row['pic_id'] and row['description'] and row['year'] and row['image_url']:
                        if int(row['year']) < 1948:
                            pic_id = int(row['pic_id'])
                            assert pic_id not in all_pic_ids
                            all_pic_ids.append(pic_id)
                            yield row
    logging.info('{} pics'.format(len(all_pic_ids)))


spew(dict(datapackage, resources=[{PROP_STREAMING: True,
                                   "name": "msh",
                                   "path": "msh.csv",
                                   "schema": {"fields": [{"name": "pic_id", "type": "string"},
                                                         {"name": "envelope_17", "type": "string"},
                                                         {"name": "envelope", "type": "string"},
                                                         {"name": "description", "type": "string"},
                                                         {"name": "participants", "type": "string"},
                                                         {"name": "place", "type": "string"},
                                                         {"name": "comment", "type": "string"},
                                                         {"name": "year", "type": "string"},
                                                         {"name": "image_url", "type": "string"},]}}]), [get_resource()])
