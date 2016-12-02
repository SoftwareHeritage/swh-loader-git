#!/usr/bin/env python3

import click
import logging
import json

from swh.scheduler.celery_backend.config import app
from swh.loader.git import tasks  # noqa


@click.command()
@click.option('--json-file', help="Json file from github api")
@click.option('--queue', default="swh.loader.git.tasks.ReaderGitRepository",
              help="Destination queue")
@click.option('--limit', default=None, help='limit on urls to send')
def main(json_file, queue, limit):
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(process)d %(message)s'
    )
    if limit:
        limit = int(limit)

    repos = json.loads(open(json_file).read())

    task_destination = app.tasks[queue]

    count = 0
    for repo in repos['items']:
        url = repo['html_url']
        count += 1
        task_destination.delay(url)
        logging.info(url)
        if limit and count >= limit:
            return


if __name__ == '__main__':
    main()
