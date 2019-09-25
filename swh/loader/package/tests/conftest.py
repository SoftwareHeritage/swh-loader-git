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


# Check get_local_factory function
# Maximum number of iteration checks to generate requests responses
MAX_VISIT_FILES = 10


@pytest.fixture
def swh_config(monkeypatch):
    conffile = os.path.join(DATADIR, 'loader.yml')
    monkeypatch.setenv('SWH_CONFIG_FILENAME', conffile)
    return conffile


def get_response_cb(request, context, ignore_urls=[], visit=None):
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
        visit (Optional[int]): Visit number for the given url (can be None)

    Returns:
        Optional[FileDescriptor] on the on disk file to read from the test
        context

    """
    logger.debug('get_response_cb(%s, %s)', request, context)
    logger.debug('url: %s', request.url)
    logger.debug('ignore_urls: %s', ignore_urls)
    if request.url in ignore_urls:
        context.status_code = 404
        return None
    url = urlparse(request.url)
    dirname = url.hostname  # pypi.org | files.pythonhosted.org
    # url.path: pypi/<project>/json -> local file: pypi_<project>_json
    filename = url.path[1:]
    if filename.endswith('/'):
        filename = filename[:-1]
    filename = filename.replace('/', '_')
    filepath = path.join(DATADIR, dirname, filename)
    if visit:
        filepath = filepath + '_visit%s' % visit
    if not path.isfile(filepath):
        context.status_code = 404
        return None
    fd = open(filepath, 'rb')
    context.headers['content-length'] = str(path.getsize(filepath))
    return fd


def local_get_factory(ignore_urls=[],
                      has_multi_visit=False):
    @pytest.fixture
    def local_get(requests_mock):
        if not has_multi_visit:
            cb = partial(get_response_cb,
                         ignore_urls=ignore_urls)
            requests_mock.get(re.compile('https://'), body=cb)
        else:
            requests_mock.get(re.compile('https'), [
                {
                    'body': partial(
                        get_response_cb,
                        ignore_urls=ignore_urls,
                        visit=i)
                } for i in range(MAX_VISIT_FILES)]
            )

        return requests_mock

    return local_get


local_get = local_get_factory([])
local_get_visits = local_get_factory(has_multi_visit=True)
