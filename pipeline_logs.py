from datetime import datetime
import socket, json, os
from tabulator import Stream
from itertools import chain
import logging


def log_successful_pipeline_execution(pipeline_id, log_target, stats=None, params=None):
    headers = ["hostname", "time", "pipeline_id", "params", "stats"]
    row = [socket.gethostname(),
           datetime.now().strftime('%Y-%m-%d %H:%m:%S'),
           pipeline_id,
           json.dumps(params, ensure_ascii=False, indent=2),
           json.dumps(dict(stats, success=True), ensure_ascii=False, indent=2)]
    if not os.path.exists(log_target):
        with Stream([headers, row]) as stream:
            stream.save(log_target)
    else:
        def get_rows():
            yield from chain(Stream(log_target).open(), [row])
        with Stream(get_rows) as stream:
            stream.save(log_target)
    logging.info(json.dumps({k: v for k, v in zip(headers, row)}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    from datapackage_pipelines.wrapper import ingest, spew
    from pipeline_params import get_pipeline_param_rows
    parameters, datapackage, resources = ingest()
    stats = {"num_rows": 0, "num_resources": 0}

    def get_resource(resource):
        for row in resource:
            yield row
            stats["num_rows"] += 1

    def get_resources():
        for resource in resources:
            yield get_resource(resource)
            stats["num_resources"] += 1
        log_successful_pipeline_execution(parameters["pipeline-id"],
                                          parameters["pipeline-log"],
                                          stats,
                                          next(get_pipeline_param_rows(parameters["pipeline-id"], parameters["pipeline-parameters"])))

    spew(datapackage, get_resources())