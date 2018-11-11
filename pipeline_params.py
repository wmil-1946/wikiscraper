from tabulator import Stream
import logging


def stream_source(source, stream_kwargs):
    yield from Stream(source, **stream_kwargs).open()


def get_all_pipeline_params(source, stream_kwargs=None):
    logging.info("reading pipeline params from {}".format(source))
    current_pipeline_rows, current_pipeline_id = None, None
    for row in stream_source(source, stream_kwargs if stream_kwargs else {}):
        if current_pipeline_id:
            if not row or len(row) < 1 or len([f for f in row if f and f.strip() != '']) < 1:
                if len(current_pipeline_rows) > 0:
                    yield {'pipeline_id': current_pipeline_id, 'rows': Stream(current_pipeline_rows, headers=1, ignore_blank_headers=True).open().iter(keyed=True)}
                current_pipeline_rows, current_pipeline_id = None, None
            else:
                current_pipeline_rows.append(row)
        elif len(row) > 0 and row[0]:
            str = row[0].strip()
            if str[0] == '(' and str[-1] == ')':
                current_pipeline_id = str[1:-1]
                current_pipeline_rows = []
    if len(current_pipeline_rows) > 0:
        yield {'pipeline_id': current_pipeline_id, 'rows': Stream(current_pipeline_rows, headers=1, ignore_blank_headers=True).open().iter(keyed=True)}


def get_pipeline_param_rows(pipeline_id, source, stream_kwargs=None):
    for p in get_all_pipeline_params(source, stream_kwargs):
        if p["pipeline_id"] == pipeline_id:
            yield from (row for row in p["rows"])
