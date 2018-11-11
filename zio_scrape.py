from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pyquery import PyQuery as pq
import logging, requests


def scrape_page(year, page_number, parameters):
    logging.info('scraping page {}'.format(page_number))
    url = parameters['url']
    url = url.replace('__YEAR__', str(year))
    url = url.replace('__PAGE_NUMBER__', str(page_number))
    res = requests.get(url)
    res.raise_for_status()
    page = pq(res.content)
    items = []
    for rownum, tr in enumerate(map(pq, page.find('table.CzaPhotoListView tr'))):
        try:
            data = dict(enumerate(map(pq, tr.find('td'))))
            propvalues = pq(data[1].find('.propValue'))
            item = {'description': pq(data[1].find('.TextLimitMedium')).text(),
                    'year': int(propvalues[1].text.strip()),
                    'id': propvalues[2].text.strip(),
                    'thumb_url': pq(data[0].find('img')).attr('src'),
                    'details_url': pq(tr.find('.CzaDetailsBtn')).attr('href'),
                    'scrape_year': year,
                    'page_number': page_number,
                    'rownum': rownum,
                    'error': ''}
        except Exception as e:
            logging.exception('failed to scrape year {} page {} rownum {}'.format(year,
                                                                                  page_number,
                                                                                  rownum))
            item = {'description': None,
                    'year': None,
                    'id': None,
                    'thumb_url': None,
                    'details_url': None,
                    'scrape_year': year,
                    'page_number': page_number,
                    'rownum': rownum,
                    'error': str(e)}
        items.append(item)
    logging.info('got {} items'.format(len(items)))
    return items, pq(page.find('a.Next'))


def scrape_year(year, parameters):
    logging.info('scraping year {}'.format(year))
    for page_number in range(parameters.get('page-from', 1),
                             parameters.get('page-to', 9999)):
        items, has_next_page = scrape_page(year, page_number, parameters)
        yield from items
        if not has_next_page:
            break


def get_resource(parameters):
    for year in range(parameters.get('year-from', 1700),
                      parameters.get('year-to', 1948)):
        yield from scrape_year(year, parameters)


def main():
    parameters, datapackage, resources, stats = ingest() + ({},)
    datapackage['resources'] = [{PROP_STREAMING: True,
                                 "name": "zio",
                                 "path": "zio.csv",
                                 "schema": {"fields": [{"name": "description", "type": "string"},
                                                       {"name": "year", "type": "year"},
                                                       {"name": "id", "type": "string"},
                                                       {"name": "thumb_url", "type": "string"},
                                                       {"name": "details_url", "type": "string"},
                                                       {"name": "scrape_year", "type": "year"},
                                                       {"name": "page_number", "type": "integer"},
                                                       {"name": "rownum", "type": "integer"},
                                                       {'name': 'error', 'type': 'string'}]}}]
    spew(datapackage, [get_resource(parameters)], stats)


if __name__ == '__main__':
    main()
