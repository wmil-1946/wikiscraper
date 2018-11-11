from datapackage_pipelines.wrapper import ingest, spew
from tempfile import mkdtemp
from contextlib import contextmanager
import os, requests
import logging, filecmp


@contextmanager
def temp_dir(*args, **kwargs):
    dir = mkdtemp(*args, **kwargs)
    try:
        yield dir
    except Exception:
        if os.path.exists(dir):
            os.rmdir(dir)
        raise


@contextmanager
def temp_file(*args, **kwargs):
    with temp_dir(*args, **kwargs) as dir:
        file = os.path.join(dir, "temp")
        try:
            yield file
        except Exception:
            if os.path.exists(file):
                os.unlink(file)
            raise


def is_missing_photo(row, parameters, skip_rows):
    if (row['objectReference'], row['objectId']) in skip_rows:
        # these are the previous missing photos
        # assuming they were deleted, they can be considered as not missing anymore
        return False
    with temp_file() as filename:
        url = row['attachmentUrl'].replace('storage.archive.gov.il',
                                           'storage.archives.gov.il')
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, 'wb') as handle:
            for block in response.iter_content(1024):
                handle.write(block)
        missing_image = parameters['missing-photo']
        if filecmp.cmp(filename, missing_image, shallow=False):
            logging.info(url)
            return True
        else:
            return False


def get_resource(resources, parameters, skip_rows, stats):
    stats['missing photos'] = 0
    for resource in resources:
        for row in resource:
            if is_missing_photo(row, parameters, skip_rows):
                stats['missing photos'] += 1
                yield row


def get_skip_rows(resource):
    skip_rows = []
    for row in resource:
        skip_rows.append((row['objectReference'], row['objectId']))
    return skip_rows


def get_datapackage(datapackage):
    datapackage['resources'] = [datapackage['resources'][1]]
    return datapackage


def main():
    parameters, datapackage, resources, stats = ingest() + ({},)
    skip_rows = get_skip_rows(next(resources))
    spew(get_datapackage(datapackage),
         [get_resource(resources, parameters, skip_rows, stats)],
         stats)


if __name__ == '__main__':
    main()
