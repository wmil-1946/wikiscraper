from dataflows import Flow, dump_to_path, printer, load, PackageWrapper
import sys
from pyquery import PyQuery as pq
import requests
import itertools
import re
import tempfile
import pywikibot
import logging
import time
from hebrew_numbers import int_to_gematria
from contextlib import contextmanager
import datetime
import os
from collections import defaultdict


ALBUMS_URL = 'http://info.palmach.org.il/show_item.asp?levelId=38530'
IMAGE_LIST_URL = 'http://info.palmach.org.il/show_item.asp?levelId=38530&itemId=6342&itemType=0'
IMAGE_DETAILS_BASE_URL = 'http://info.palmach.org.il/'
IMAGE_ID_REGEX = 'show_item.asp\?levelId=(\d*)&itemId=(\d*)&itemType=(\d*)&obj=(\d*)&picI=(\d*)'
IMAGE_HIGH_RES_URL = 'http://palmach.org.il/Uploads/ArchivePictures/{0}/P_{0}_0.jpg'
HEB_YEARS = {year: int_to_gematria(year-1239).replace('״', '"') for year in range(1900,1948)}
DESCRIPTION_TEMPLATE = """=={{int:filedesc}}==
{{Information
|description={{he|1=__DESCRIPTION__}}
|date=__YEAR__
|source={{he|1=__SOURCE__}}
|author={{he|1=__AUTHOR__}}
|permission=
|other versions=
|other fields={{Information field|Name=Palmah Collection Identifier|Value=__PIC_ID__}}
}}
=={{int:license-header}}==
{{PD-Israel}}
{{Supported by Wikimedia Israel|year=2018}}
[[Category:Files from Palmah Archive uploaded by Wikimedia Israel]]"""


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


def get_albums():
    page = pq(ALBUMS_URL, encoding='cp1255')
    num_albums = 0
    for option in page('#album option'):
        id = int(option.attrib['value'])
        if id == 0:
            continue
        yield {'id': id, 'title': option.text}
        num_albums += 1
    print('got {} albums'.format(num_albums))


def get_image_list_schema(package: PackageWrapper):
    schema = package.pkg.resources[0].descriptor['schema']
    schema['fields'] = [{'name': 'album_id', 'type': 'integer'},
                        {'name': 'details_url', 'type': 'string'},
                        {'name': 'thumbnail_url', 'type': 'string'},
                        {'name': 'thumbnail_alt', 'type': 'string'}]
    yield package.pkg
    yield from package


def get_image_list_rows(rows):
    with requests.Session() as s:
        for album in rows:
            print('getting images for album {}'.format(album))
            for page_num in range(1, 999):
                if page_num == 1:
                    res = s.post(IMAGE_LIST_URL,
                                        {'name_h': '', 'internal_id': '', 'album': str(album['id']), 'unit': '0'})
                else:
                    res = s.get(IMAGE_LIST_URL + '&n=' + str(page_num))
                res.encoding = 'cp1255'
                page = pq(res.text)
                num_yielded_images = 0
                for aelt in page('a'):
                    if aelt.attrib.get('href', '').startswith('show_item.asp'):
                        img = aelt.find('img')
                        if img is not None:
                            num_yielded_images += 1
                            yield {'album_id': album['id'],
                                   'details_url': aelt.attrib['href'],
                                   'thumbnail_url': img.attrib.get('src'),
                                   'thumbnail_alt': img.attrib.get('alt')}
                if num_yielded_images == 0:
                    print('got 0 images, stopping for this album')
                    break
                else:
                    print('got {} images for page number {}'.format(num_yielded_images, page_num))


def get_image_details_schema(package: PackageWrapper):
    schema = package.pkg.resources[0].descriptor['schema']
    schema['fields'] += [{'name': 'data', 'type': 'object'},
                         {'name': 'image_url', 'type': 'string'},]
    yield package.pkg
    # yield itertools.islice(next(package.it), 0, 10)
    yield from package


def get_image_details_row(row):
    print('getting image details for {}'.format(row['details_url']))
    time.sleep(.1)
    try:
        page = pq('http://info.palmach.org.il/' + row['details_url'], encoding='cp1255')
        error = None
    except Exception as e:
        page = None
        error = str(e)
    if page:
        metadata_elts = page('#ScrollCream2')
        image_elts = page('#realImg')
        if len(metadata_elts) > 0 and len(image_elts) > 0:
            metadata = pq(metadata_elts[0]).text()
            image = image_elts[0].attrib.get('src')
            attrib, value = None, []
            data = {}
            for line in metadata.split('\n'):
                if line.endswith(':'):
                    if attrib:
                        data[attrib] = '\n'.join(value)
                    attrib, value = line[:-1], []
                else:
                    line = line.strip()
                    if line:
                        value.append(line.strip())
            row.update(data=data, image_url=image)
    else:
        print('failed to download, skipping: {}'.format(row))
        row.update(data={'error': error}, image_url='')


def get_year_from_text(text):
    text = text.strip()
    matches = re.findall('(^|\s)(19\d\d)-(\d{1,2})($|\s)', text)
    if matches:
        if len(matches) == 1:
            year_from = matches[0][1]
            year_to = matches[0][2]
            if len(year_to) == 1:
                if year_from[2] in ['3', '4']:
                    return int(year_from), int('19' + year_from[2] + year_to)
            elif year_to[0] in ['3', '4']:
                return int(year_from), int('19' + year_to)
        # print('invalid match: {}'.format(text))
        return None, None
    matches = re.findall('(^|\s)(\d\d)-(\d{1,2})($|\s)', text)
    if matches:
        if len(matches) == 1:
            year_from = matches[0][1]
            year_to = matches[0][2]
            if year_from[0] in ['3', '4']:
                if len(year_to) == 1:
                    return int('19' + year_from), int('19' + year_from[0] + year_to)
                elif year_to[0] in ['3', '4']:
                    return int('19' + year_from), int('19' + year_to)
        # print('invalid match: {}'.format(text))
        return None, None
    matches = re.findall('(^|\s|\.|/|\'|-)(19\d\d)($|\s)', text)
    if matches:
        if len(matches) == 1:
            return int(matches[0][1]), None
        # print('invalid match: {}'.format(text))
        return None, None
    matches = re.findall('(^|\s|\.|/|\')(\d{1,2})[./](\d{2}|19\d{2})($|\s)', text)
    if matches:
        if len(matches) == 1:
            match = matches[0][2]
            if len(match) == 4:
                return int(match), None
            elif match[0] in ['4', '3', '2']:
                return int('19' + match), None
        # print('invalid match: {}'.format(text))
        return None, None
    matches = re.findall('(^|\s|\.|/|\')(\d{1,2})($|\s)', text)
    if matches:
        if len(matches) == 1:
            match = matches[0][1]
            if match[0] in ['2', '3', '4']:
                return int('19' + match), None
        return None, None
    for year, heb_year in HEB_YEARS.items():
        matches = re.findall('(^|\s)({})($|\s)'.format(heb_year), text)
        if matches:
            if len(matches) == 1:
                return int(year), None
    # print('no match: {}'.format(text))
    return None, None


def parse_metadata(stats: defaultdict, limit_image_ids=None):

    def get_images(resource, albums, images, metadata_fields):
        all_image_ids = set()
        for row in resource:
            stats['total_images'] += 1
            if not row['data']:
                stats['missing_data'] += 1
                # print('missing data: {}'.format(row))
                continue
            album_title = albums[row['album_id']]
            m = re.match(IMAGE_ID_REGEX, row['details_url'])
            if m is None:
                stats['failed_to_get_image_id'] += 1
                print('failed to get image_id: {}'.format(row))
                continue
            image_id = m.groups()[3]
            if limit_image_ids and str(image_id) not in limit_image_ids:
                continue
            if image_id in all_image_ids:
                stats['duplicate_image-id'] += 1
                print('duplicate image_id: {}'.format(row))
                continue
            all_image_ids.add(image_id)
            year_from, year_to = None, None
            if 'תאריך צילום' in row['data']:
                year_from, year_to = get_year_from_text(row['data']['תאריך צילום'])
            if year_from:
                stats['valid_year'] += 1
            # if not year:
            #     year = get_year_from_text(album_title)
            # if not year:
            #     for k, v in row['data'].items():
            #         if k != 'תאריך צילום':
            #             year = get_year_from_text(v)
            #             if year:
            #                 break
            participants = row['data'].get('מופיעים בתמונה')
            description = row['data'].get('תאור התמונה')
            thumbnail_alt = row.get('thumbnail_alt')
            full_description = album_title
            if thumbnail_alt and len(thumbnail_alt) > 3:
                full_description += ' - ' + thumbnail_alt
            if description and len(description) > 3:
                full_description += ' - ' + description
            if participants and len(participants) > 3:
                full_description += '. ' + participants
            row.update(album_title=album_title,
                       image_id=image_id,
                       image_high_res_url=IMAGE_HIGH_RES_URL.format(image_id),
                       year_from=year_from,
                       year_to=year_to,
                       description=description,
                       participants=participants,
                       full_description=full_description)
            metadata_field_names = [f['name'] for f in metadata_fields]
            for k, v in row['data'].items():
                if k not in metadata_field_names:
                    metadata_fields.append({'name': k, 'type': 'string'})
                row[k] = v
            stats['yielded_images'] += 1
            images.append(row)

    def step(package: PackageWrapper):
        metadata_fields = [{'name': 'album_title', 'type': 'string'},
                           {'name': 'image_id', 'type': 'string'},
                           {'name': 'year_from', 'type': 'integer'},
                           {'name': 'year_to', 'type': 'integer'},
                           {'name': 'description', 'type': 'string'},
                           {'name': 'participants', 'type': 'string'},
                           {'name': 'image_high_res_url', 'type': 'string'},
                           {'name': 'full_description', 'type': 'string'}]

        package.pkg.remove_resource('albums')
        schema = package.pkg.get_resource('image_details').descriptor['schema']
        schema['fields'] = [f for f in schema['fields'] if f['name'] not in ['data']]
        schema['fields'] += metadata_fields
        yield package.pkg

        albums = {}
        images = []
        dynamic_metadata_fields = []

        for i, resource in enumerate(package):
            if i == 0:
                for row in resource:
                    albums[row['id']] = row['title']
                    stats['num_albums'] += 1
            elif i == 1:
                get_images(resource, albums, images, dynamic_metadata_fields)

        package.pkg.get_resource('image_details').descriptor['schema']['fields'] += dynamic_metadata_fields
        yield (img for img in images)

    return step


def prepare_upload(stats: defaultdict):

    def process_row(row):
        file_page_title = row['full_description'][:80] + '-{}.jpg'.format(row['image_id'])
        description = row['full_description']
        if not description or len(description) < 3:
            stats['missing description'] += 1
            return None
        if not row['year_from']:
            stats['missing year_from'] += 1
            return None
        if int(row['year_from']) > 1947 or int(row['year_from']) < 1940:
            stats['year_from not in valid range'] += 1
            return None
        if row['year_to'] and (int(row['year_to']) > 1947 or int(row['year_to']) < 1940):
            stats['year_to not in valid range'] += 1
            return None
        stats['valid row'] += 1
        return {'image_id': row['image_id'],
                'title': file_page_title,
                'description': description,
                'year': row['year_from'] if not row['year_to'] else '{} - {}'.format(row['year_from'],
                                                                                     row['year_to']),
                'source': 'ארכיון הפלמ"ח',
                'author': 'ארכיון הפלמ"ח',
                'image_high_res_url': row['image_high_res_url']}

    def process_rows(resource):
        for row in resource:
            row = process_row(row)
            if row:
                yield row

    def step(package: PackageWrapper):
        schema = package.pkg.get_resource('image_metadata').descriptor['schema']
        schema['fields'] = [{'name': 'image_id', 'type': 'integer'},
                            {'name': 'title', 'type': 'string'},
                            {'name': 'description', 'type': 'string'},
                            {'name': 'year', 'type': 'string'},
                            {'name': 'source', 'type': 'string'},
                            {'name': 'author', 'type': 'string'},
                            {'name': 'image_high_res_url', 'type': 'string'}]
        yield package.pkg
        for resource in package:
            yield process_rows(resource)

    return step


def upload(dry_run, limit_rows, from_image_id, stats: defaultdict):

    def step(row):
        if from_image_id and not stats['reached_from_image_id']:
            stats['reached_from_image_id'] = str(row['image_id']) == str(from_image_id)
            if not stats['reached_from_image_id']:
                return
        if limit_rows and stats['total'] >= limit_rows:
            return
        stats['last_image_id'] = row['image_id']
        stats['total'] += 1
        file_page_title = row['title']
        for char in ['"', "'", '[', ']', ',']:
            file_page_title = file_page_title.replace(char, '')
        description = row['description']
        image_url = row['image_high_res_url']
        page_text = DESCRIPTION_TEMPLATE
        for tag, value in (('__DESCRIPTION__', str(description)),
                           ('__YEAR__', str(row['year'])),
                           ('__PIC_ID__', str(row['image_id'])),
                           ('__AUTHOR__', str(row['author'])),
                           ('__SOURCE__', str(row['source']))):
            page_text = page_text.replace(tag, value)
        if dry_run:
            print('dry run - {} -- {} -- {}'.format(image_url, file_page_title, page_text))
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
                        f.write(requests.get(image_url).content)
                        if page.upload(f.name, comment="uploaded by wmilbot", ignore_warnings=True):
                            print("----- {} uploaded successfully".format(row['image_id']))
                            stats['uploaded'] += 1
                        else:
                            raise Exception("Upload failed")
                else:
                    page.get()
                    page.text = page_text
                    page.save(summary='update by wmilbot')
                    print('----- {} updated successfully'.format(row['image_id']))
                    stats['updated'] += 1
    return step


if sys.argv[1] == '--albums':
    Flow(get_albums(),
         # printer(),
         dump_to_path('data/plmh/albums')
         ).process()
elif sys.argv[1] == '--image-list':
    Flow(load('data/plmh/albums/res_1.csv'),
         get_image_list_schema,
         get_image_list_rows,
         # printer(),
         dump_to_path('data/plmh/image_list')
         ).process()
elif sys.argv[1] == '--image-details':
    Flow(load('data/plmh/image_list/res_1.csv'),
         get_image_details_row,
         get_image_details_schema,
         # printer(),
         dump_to_path('data/plmh/image_details')
         ).process()
elif sys.argv[1] == '--parse-metadata':
    stats = defaultdict(int)
    Flow(load('data/plmh/albums/res_1.csv', 'albums'),
         load('data/plmh/image_details/res_1.csv', 'image_details'),
         parse_metadata(stats,
                        sys.argv[2].split(',') if len(sys.argv) > 2 else None),
         dump_to_path('data/plmh/image_metadata')
         ).process()
    print(stats)
elif sys.argv[1] == '--prepare-upload':
    stats = defaultdict(int)
    Flow(load('data/plmh/image_metadata/image_details.csv', 'image_metadata'),
         prepare_upload(stats),
         dump_to_path('data/plmh/prepare_upload')
         ).process()
    print(stats)
elif sys.argv[1] == '--upload':
    stats = defaultdict(int)
    dry_run, limit_rows, from_image_id = None, None, None
    if len(sys.argv) > 2 and sys.argv[2] == '--dry-run':
        dry_run = True
    elif len(sys.argv) > 3:
        if sys.argv[2] == '--limit':
            limit_args = sys.argv[3].split('-')
            limit_rows = int(limit_args[0])
            if len(limit_args) == 2:
                from_image_id = limit_args[1]
        elif sys.argv[2] == '--from':
            from_image_id = sys.argv[3]
    Flow(load('final-data/plmh/upload_images.csv'),
         upload(dry_run, limit_rows, from_image_id, stats)
         ).process()
    print(stats)
else:
    print('unrecognized option')
    exit(1)
