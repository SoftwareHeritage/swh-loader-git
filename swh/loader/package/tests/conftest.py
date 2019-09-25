# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import re
import pytest

from functools import partial
from os import path
from urllib.parse import urlparse

from .common import DATADIR


logger = logging.getLogger(__name__)


@pytest.fixture
def swh_config(monkeypatch):
    conffile = os.path.join(DATADIR, 'loader.yml')
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile


def get_response_cb(request, context, ignore_urls=[]):
    """Mount point callback to fetch on disk the content of a request

    This is meant to be used as 'body' argument of the requests_mock.get()
    method.

    It will look for files on the local filesystem based on the requested URL,
    using the following rules:

    - files are searched in the DATADIR/<hostname> directory

    - the local file name is the path part of the URL with path hierarchy
      markers (aka '/') replaced by '_'

    Eg. if you use the requests_mock fixture in your test file as:

        requests_mock.get('https://nowhere.com', body=get_response_cb)
        # or even
        requests_mock.get(re.compile('https://'), body=get_response_cb)

    then a call requests.get like:

        requests.get('https://nowhere.com/path/to/resource')

    will look the content of the response in:

        DATADIR/resources/nowhere.com/path_to_resource

    Args:
        request (requests.Request): Object requests
        context (requests.Context): Object holding response metadata
                                    information (status_code, headers, etc...)
        ignore_urls (List): urls whose status response should be 404 even if
                            the local file exists

    Returns:
        Optional[FileDescriptor] on the on disk file to read from the test
        context

    """
    logger.debug('get_response_cb(%s, %s)', request, context)
    url = urlparse(request.url)
    if url in ignore_urls:
        context.status_code = 404
        return None
    dirname = url.hostname  # pypi.org | files.pythonhosted.org
    # url.path: pypi/<project>/json -> local file: pypi_<project>_json
    filename = url.path[1:]
    if filename.endswith('/'):
        filename = filename[:-1]
    filename = filename.replace('/', '_')
    filepath = path.join(DATADIR, dirname, filename)
    if not path.isfile(filepath):
        context.status_code = 404
        return None
    fd = open(filepath, 'rb')
    context.headers['content-length'] = str(path.getsize(filepath))
    return fd


def local_get_factory(ignore_urls=[]):
    @pytest.fixture
    def local_get(requests_mock):
        cb = partial(get_response_cb, ignore_urls=ignore_urls)
        requests_mock.get(re.compile('https://'), body=cb)

        return requests_mock

    return local_get


local_get = local_get_factory([])
