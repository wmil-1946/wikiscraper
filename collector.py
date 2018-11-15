import argparse
import time
import datetime
import logging
import csv

from selenium import webdriver
from pathlib import Path

DOWNLOAD_DIR = 'C:/Users/wmilbot/GitHub/wikiscraper/media/'

LOAD_MORE_PICTURES_GROUP = 15

SOURCE_URL = 'http://gpophotoeng.gov.il/'
SOURCE_NAME = 'אתר לשכת העיתונות הממשלתית'

DEFAULT_MAX_IMAGES = 1000000
DEFAULT_SLEEP_TIME = 20

SORTING_OPTIONS = ('AlfaNumericAsc', 'AlfaNumericDsc')  # incomplete

SEARCH_URL_BASIC = 'http://gpophotoeng.gov.il/fotoweb/grid.fwx?sorting=ModifiedTimeDsc'
SEARCH_URL_DATE_RANGE = 'http://gpophotoeng.gov.il/fotoweb/Grid.fwx?sorting=AlfaNumericAsc&SF_GROUP3_BOOLEAN=and&SF_GROUP12_BOOLEAN=and&SF_GROUP13_BOOLEAN=and&SF_GROUP14_BOOLEAN=and&SF_GROUP15_BOOLEAN=and&SF_GROUP16_BOOLEAN=and&SF_GROUP17_BOOLEAN=and&SF_GROUP18_BOOLEAN=and&SF_GROUP19_BOOLEAN=and&SF_GROUP20_BOOLEAN=and&SF_GROUP21_BOOLEAN=and&SF_GROUP22_BOOLEAN=and&SF_GROUP23_BOOLEAN=and&SF_GROUP24_BOOLEAN=and&SF_GROUP25_BOOLEAN=and&SF_GROUP26_BOOLEAN=and&SF_GROUP27_BOOLEAN=and&SF_GROUP28_BOOLEAN=and&SF_GROUP29_BOOLEAN=and&SF_GROUP30_BOOLEAN=and&SF_GROUP31_BOOLEAN=and&SF_GROUP32_BOOLEAN=and&SF_GROUP33_BOOLEAN=and&SF_GROUP34_BOOLEAN=and&SF_GROUP35_BOOLEAN=and&SF_GROUP36_BOOLEAN=and&SF_GROUP37_BOOLEAN=and&archiveId=&SF_GROUP4_BOOLEAN=and&SF_GROUP4_FIELD=FQYFT&SF_FIELD4_GROUP=4&SF_FIELD4=BMP+or+FPIX+or+JPEG+or+PNTG+or+8BIM+or+PNG+or+QDGX+or+PICT+or+QTIF+or+SGI+or+TPIC+or+TIFF+or+NEF+or+PCDI&SF_GROUP1_BOOLEAN=and&SF_FIELD1_GROUP=1&SF_FIELD1_MATCHTYPE=exact&SF_FIELD1=&SF_GROUP1_FIELD=&SF_FIELD2_GROUP=2&SF_GROUP2_BOOLEAN=and&SF_FIELD2_MATCHTYPE=exact&SF_FIELD2=&SF_GROUP2_FIELD=&SF_GROUP5_BOOLEAN=and&SF_FIELD5_GROUP=5&SF_GROUP5_FIELD=FQYID&SF_FIELD5={}&SF_FIELD5_TO={}&SF_GROUP6_BOOLEAN=and&SF_FIELD6_GROUP=6&SF_GROUP6_FIELD=FQYFS&SF_FIELD6=&SF_FIELD6_TO=&SF_GROUP7_BOOLEAN=and&SF_FIELD7_GROUP=7&SF_GROUP7_FIELD=FQYIR&SF_FIELD7=&SF_FIELD7_TO=&SF_GROUP8_BOOLEAN=and&SF_GROUP8_FIELD=FQYIC&SF_FIELD8_GROUP=8&SF_FIELD8=&SF_GROUP9_BOOLEAN=and&SF_GROUP9_FIELD=FQYFM&SF_FIELD9_GROUP=9&SF_FIELD9=&SF_GROUP10_BOOLEAN=and&SF_GROUP10_FIELD=&SF_FIELD10_GROUP=10&SF_FIELD10=&SF_GROUP11_BOOLEAN=and&SF_GROUP11_FIELD=FQYOL&SF_FIELD11_GROUP=11&SF_FIELD11='

SEARCH_URL_19TH_CENTURY = SEARCH_URL_DATE_RANGE.format('18000101', '19000101')
FROM_DATE = '19000102'
TO_DATE = '19191231'

DEFAULT_DELIMITER = ';'
DEFAULT_DELIMITER_REPL = ','


class ImageCollector(object):
    def __init__(self, sleep_time=DEFAULT_SLEEP_TIME,
                 force_create_meta_data_file=False):
        self.browser = None
        self.DELIMITER = DEFAULT_DELIMITER
        self.DELIMITER_REPL = DEFAULT_DELIMITER_REPL
        self.LOAD_MORE_PICTURES_GROUP = LOAD_MORE_PICTURES_GROUP
        self.logger = logging.getLogger('default')
        self.SLEEP_TIME = sleep_time
        self.EXTRA_SLEEP_TIME = 5
        self.meta_data_file = 'meta_data.csv'
        self.metadata_field_names = ['timestamp',
                                     'source_name',
                                     'source_url',
                                     'image_name',
                                     'date_created',
                                     'credit',
                                     'caption',
                                     'keywords',
                                     'image_url',
                                     ]
        self.force_create_meta_data_file = force_create_meta_data_file

    def _initialize_browser(self):
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            'download.default_directory': DOWNLOAD_DIR}
        chrome_options.add_experimental_option("prefs", prefs)
        return webdriver.Chrome(chrome_options=chrome_options)

    def write_meta_data_row_to_file(self, row):
        with open(self.meta_data_file, mode='a', encoding='utf8',
                  newline='') as f:
            csv.DictWriter(f, fieldnames=self.metadata_field_names,
                           delimiter=self.DELIMITER).writerow(
                row)

    def create_meta_data_file_if_not_exists(self):
        if self.force_create_meta_data_file or not Path(
                self.meta_data_file).is_file():
            self.create_metadata_file()

    def create_metadata_file(self):
        with open(self.meta_data_file, mode='w', encoding='utf8',
                  newline='') as f:
            writer = csv.DictWriter(f, fieldnames=self.metadata_field_names,
                                    delimiter=self.DELIMITER)
            writer.writeheader()

    def setup_collection(self, start_date, end_date, start_image=0):
        self.create_meta_data_file_if_not_exists()

        self.browser = self._initialize_browser()
        self.browser.implicitly_wait(self.SLEEP_TIME)

        self.browser.get(SEARCH_URL_DATE_RANGE.format(start_date, end_date))
        time.sleep(self.SLEEP_TIME)

        images_on_display = self.currently_displayed_images()
        if len(images_on_display) < start_image:
            next_url = '{}#Preview{}'.format(self.browser.current_url,
                                      start_image + 1)
            self.browser.get(next_url)
            time.sleep(self.SLEEP_TIME + self.EXTRA_SLEEP_TIME)
        else:
            try:
                start_image = images_on_display[start_image]
            except IndexError as e:
                print('start image is higher than latest image found')
                raise e
            start_image.click()
            time.sleep(self.SLEEP_TIME)

    def currently_displayed_images(self):
        return self.browser.find_elements_by_class_name(
            'Thumbnail')

    def teardown_collection(self):
        self.browser.quit()

    def collect_images(self, start_date, end_date,
                       max_images=DEFAULT_MAX_IMAGES, start_image=0,
                       leave_open=False):
        print('Start Colllecting images.')
        self.setup_collection(start_date, end_date, start_image=start_image)
        images_collected = 0 + start_image
        next_page_exists = True
        while images_collected < max_images + start_image and next_page_exists:
            images_collected += 1
            print('Colllecting image {}'.format(images_collected))
            if not self.image_already_exists():
                self.download_image_in_page()
                self.save_meta_data_in_page()
            if not images_collected % self.LOAD_MORE_PICTURES_GROUP:
                self.load_more_pictures()
            next_page_exists = self.go_to_next_page()
        if not leave_open:
            self.teardown_collection()
        print('Done Collecting images.')

    def image_already_exists(self):
        image_name = self.get_image_name()
        return Path(DOWNLOAD_DIR).joinpath(image_name).is_file()

    def save_meta_data_in_page(self):
        meta_data = self.build_meta_data_dict()
        meta_data_row = {'timestamp': datetime.datetime.now(),
                         'source_name': SOURCE_NAME,
                         'source_url': SOURCE_URL,
                         'image_name': self.get_image_name(),
                         'image_url': self.get_current_url(),
                         'date_created': meta_data.get('IPTC Created Date',
                                                       None),
                         'credit': meta_data.get('Credit'),
                         'caption': '\t\t\t'.join(
                             meta_data.get('caption', [])).replace(
                             self.DELIMITER, self.DELIMITER_REPL),
                         'keywords': ','.join(
                             meta_data.get('keywords', [])).replace(
                             self.DELIMITER, self.DELIMITER_REPL),

                         }
        self.write_meta_data_row_to_file(meta_data_row)

    def load_more_pictures(self):
        current_url = self.get_current_url()
        self.exit_preview_mode()
        self.scroll_down_and_wait()
        if not self.get_current_url() == current_url:
            self.browser.get(current_url)
            time.sleep(self.SLEEP_TIME)

    def download_image_in_page(self):
        self.switch_browser_to_first_iframe()
        links = self.browser.find_elements_by_tag_name('a')
        download_link = [link for link in links if 'Download' in link.text][0]
        download_link.click()
        self.switch_browser_to_default_content()
        time.sleep(self.SLEEP_TIME)

    def scroll_down_and_wait(self):
        print('trying to scroll down')
        self.browser.execute_script(
            'window.scrollTo(0, document.body.scrollHeight)')
        time.sleep(self.SLEEP_TIME + self.EXTRA_SLEEP_TIME)

    def exit_preview_mode(self):
        self.browser.execute_script('window.parent.Grid.hidePreview()')

    def go_to_next_page(self):
        self.switch_browser_to_first_iframe()
        next_image = self.browser.find_element_by_id('nextLink')
        if 'disabled' in next_image.get_attribute('class'):
            return False
        next_image.click()
        self.switch_browser_to_default_content()
        time.sleep(self.SLEEP_TIME)
        return True

    def switch_browser_to_first_iframe(self):
        iframe = self.browser.find_elements_by_tag_name('iframe')[0]
        self.browser.switch_to.frame(iframe)

    def switch_browser_to_default_content(self):
        self.browser.switch_to.default_content()

    def get_image_name(self):
        self.switch_browser_to_first_iframe()
        image_name = self.browser.find_element_by_tag_name('h2').text
        self.switch_browser_to_default_content()
        return image_name

    def get_current_url(self):
        return self.browser.current_url

    def build_meta_data_dict(self):
        data_dict = dict()
        self.switch_browser_to_first_iframe()
        data_dict['caption'] = self._get_caption_in_frame()
        data_dict['keywords'] = self._get_keywords_in_frame()
        table = self._get_meta_data_inner_table_in_frame()
        rows = table.find_elements_by_tag_name('tr')
        for row in rows:
            cells = row.find_elements_by_tag_name('td')
            data_dict[cells[0].text.strip()] = cells[1].text.strip()
        self.switch_browser_to_default_content()
        return data_dict

    def _get_keywords_in_frame(self):
        table = self._get_meta_data_outer_table_in_frame()
        return [link.text for link in table.find_elements_by_tag_name('a')]

    def _get_caption_in_frame(self):
        table = self._get_meta_data_outer_table_in_frame()
        # get all text between "Caption" and "Keywords"
        return [x for x in table.text.split('Keywords')[0].split('Caption')[
            -1].strip().splitlines() if x]

    def _get_meta_data_outer_table_in_frame(self):
        tables = self.browser.find_element_by_class_name(
            'PreviewInfo').find_elements_by_tag_name('table')
        return tables[0]

    def _get_meta_data_inner_table_in_frame(self):
        tables = self.browser.find_elements_by_tag_name('table')
        table = tables[-1]
        return table


def main():
    arg_parser = argparse.ArgumentParser(
        description='WikiScraper - a public domain image collector')
    arg_parser.add_argument('--sleep', action='store', nargs='?', type=int,
                            default=5,
                            help='seconds to sleep between each command')
    arg_parser.add_argument('--start-date', action='store', type=str,
                            help='date range to search, start date. '
                                 'format yyyymmdd')
    arg_parser.add_argument('--end-date', action='store', type=str,
                            help='date range to search, end date. '
                                 'format yyyymmdd')
    arg_parser.add_argument('--max_images', action='store', nargs='?',
                            type=int,
                            default=DEFAULT_MAX_IMAGES,
                            help='max number of images to collect')
    arg_parser.add_argument('--start-image', action='store', nargs='?',
                            type=int,
                            default=0,
                            help='start collection with image of given number.'
                            )
    arg_parser.add_argument('--recreate', action='store_true',
                            default=False,
                            help='rewrite metadata if set. otherwise, append')
    arg_parser.add_argument('--leave-open', action='store_true',
                            default=False,
                            help='leave the page open after done.')

    parsed_args = arg_parser.parse_args()

    collector = ImageCollector(
        sleep_time=parsed_args.sleep,
        force_create_meta_data_file=parsed_args.recreate)
    try:
        collector.collect_images(parsed_args.start_date, parsed_args.end_date,
                                 max_images=parsed_args.max_images,
                                 start_image=parsed_args.start_image,
                                 leave_open=parsed_args.leave_open)
    except:
        if not parsed_args.leave_open:
            collector.teardown_collection()
        raise
    finally:
        if parsed_args.leave_open:
            input("Press Enter Key to continue..")


if __name__ == '__main__':
    main()