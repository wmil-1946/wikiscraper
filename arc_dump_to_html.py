from datapackage_pipelines.wrapper import ingest, spew
import logging
import template_functions


parameters, datapackage, resources, stats = ingest() + ({},)


def get_context(**context):
    return template_functions.get_context(context)


def get_images():
    for resource in resources:
        for row in resource:
            yield row


env = template_functions.get_jinja_env()
context = {"images": get_images()}
template_functions.build_template(env, "arc_index.html", template_functions.get_context(context),
                                  "final-data/arc/index.html")

spew(dict(datapackage, resources=[]), [], stats)
