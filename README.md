# WikiScraper

Tools for scraping images (and their corresponding Meta-Data) from Websites that hold photographs that are public property.

All the scraped and published works are in the public domain in Israel.
According to Israel's copyright statute from 2007 (translation), a work is released to the public domain on 1 January of the 71st year after the author's death (paragraph 38 of the 2007 statute) with the following exceptions:

* A photograph taken on 24 May 2008 or earlier — the old British Mandate act applies, i.e. on 1 January of the 51st year after the creation of the photograph (paragraph 78(i) of the 2007 statute, and paragraph 21 of the old British Mandate act).
* If the copyrights are owned by the State, not acquired from a private person, and there is no special agreement between the State and the author — on 1 January of the 51st year after the creation of the work (paragraphs 36 and 42 in the 2007 statute).

For more details, see [here](https://commons.wikimedia.org/wiki/Category:PD_Israel_%26_British_Mandate)

Collected images are uploaded to WikiCommons by [wmilbot](https://commons.wikimedia.org/wiki/User:Wmilbot)

Photos from the following collections were published so far:

* [JNF Photo Archive](https://commons.wikimedia.org/wiki/Category:Files_from_JNF_uploaded_by_Wikimedia_Israel)
* [Israel State Archive](https://commons.wikimedia.org/wiki/Category:Files_from_ISA_uploaded_by_Wikimedia_Israel)
* [Palmah Archive](https://commons.wikimedia.org/wiki/Category:Files_from_Palmah_Archive_uploaded_by_Wikimedia_Israel)
* [Moshe Sharett Archive](https://commons.wikimedia.org/wiki/Category:Files_from_Moshe_Sharett_Archive_uploaded_by_Wikimedia_Israel)

## Development Dependencies

- Chrome (>62)
- Chrome Webdriver https://sites.google.com/a/chromium.org/chromedriver/getting-started
- Python 3.6
- Pipenv

## Install

```bash
pipenv install
```

## Usage

### Collector

```bash
pipenv shell
python ./collector.py --help

usage: collector.py [-h] [--sleep [SLEEP]] [--start-date START_DATE]
                    [--end-date END_DATE] [--max_images [MAX_IMAGES]]
                    [--start-image [START_IMAGE]] [--recreate] [--leave-open]

WikiScraper - a public domain image collector

optional arguments:
  -h, --help            show this help message and exit
  --sleep [SLEEP]       seconds to sleep between each command
  --start-date START_DATE
                        date range to search, start date. format yyyymmdd
  --end-date END_DATE   date range to search, end date. format yyyymmdd
  --max_images [MAX_IMAGES]
                        max number of images to collect
  --start-image [START_IMAGE]
                        start collection with image of given number.
  --recreate            rewrite metadata if set. otherwise, append
  --leave-open          leave the page open after done.
```

### Pipelines

Each scraper parses a parameters csv file (`kkl-parameters.csv` / `arc-parameters.csv`) and creates a corresponding log csv file

You should sync these csv files along with all files under `data` directory back to the google drive sheets / data folder

#### KKL

```
# download the source html pages - uses selenium chrome webdriver
pipenv run python kkl.py

# parse metadata
pipenv run dpp run ./kkl-parse-pages

# download images
pipenv run dpp run ./kkl-download-images
```

#### ARC

```
# perform a search and parse image results
pipenv run dpp run ./arc-search-ajax-download
```

#### MSH (Moshe Sharett)

```
# Download source html pages (using curl)
./msh-download.sh
# Parse
pipenv run dpp run ./msh-parse
```

#### plmh

Get the metadata

```
python3 plmh-flow.py --albums &&\
python3 plmh-flow.py --image-list &&\
python3 plmh-flow.py --image-details
```

### Sync to cloud storage

Some collectors support syncing to Google Cloud Storage

You need to create a bucket and service account with appropriate permissions

once done you should provide the gcs_ constants for the relevant scraper with the gcs bucket / project / key file location

Some pipelines don't sync directly to google sync but write files to data/ directory, you should sync the data directory to google cloud storage

Each sync is made to a new directory with current date to prevent accidential deletion and to have full data changes

### running wikicommons upload

Install the environment

```
pipenv install
```

Run upload of all kkl images, in case of existing page, will update the page text

```
pipenv run ./kkl_wikicommons_upload.py --cli upload all
pipenv run ./kkl_wikicommons_upload.py --cli upload-after 020466 999999
```

Run upload of arc images, after last uploaded image

```
pipenv run ./arc_wikicommons_upload.py --cli upload-after ZKlugerPhotos-00132pp 999999
```

Run upload of msh images, after last uploaded image

```
pipenv run ./msh_wikicommons_upload.py --cli upload-after 3001 10
```

Run upload of arc albums

```
pipenv run ./arc_albums_wikicommons_upload.py --cli upload
```
