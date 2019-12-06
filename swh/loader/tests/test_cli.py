# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest


from swh.loader.cli import run, get_loader, SUPPORTED_LOADERS
from swh.loader.package.loader import PackageLoader

from click.testing import CliRunner


def test_get_loader_wrong_input(swh_config):
    """Unsupported loader should raise

    """
    loader_type = 'unknown'
    assert loader_type not in SUPPORTED_LOADERS
    with pytest.raises(ValueError, match='Invalid loader'):
        get_loader(loader_type, url='db-url')


def test_get_loader(swh_config):
    """Instantiating a supported loader should be ok

    """
    loader_input = {
        'archive': {
            'url': 'some-url',
            'artifacts': [],
        },
        'debian': {
            'url': 'some-url',
            'date': 'something',
            'packages': [],
        },
        'deposit': {
            'url': 'some-url',
            'deposit_id': 1,
        },
        'npm': {
            'url': 'https://www.npmjs.com/package/onepackage',
        },
        'pypi': {
            'url': 'some-url',
        },
    }
    for loader_type, kwargs in loader_input.items():
        loader = get_loader(loader_type, **kwargs)
        assert isinstance(loader, PackageLoader)


help_msg = """Usage: run [OPTIONS] [OPTIONS]...

  Loader cli tools

  Load an origin from its url with loader <name>

Options:
  -t, --type [archive|debian|deposit|npm|pypi]
                                  Loader to run
  -u, --url TEXT                  Origin url to load
  -h, --help                      Show this message and exit.
"""


def test_run_help(swh_config):
    """Help message should be ok

    """
    runner = CliRunner()
    result = runner.invoke(run, ['-h'])
    assert result.exit_code == 0
    assert result.output.startswith(help_msg)


def test_run_pypi(mocker, swh_config):
    """Triggering a load should be ok

    """
    mock_loader = mocker.patch('swh.loader.package.pypi.loader.PyPILoader')
    runner = CliRunner()
    result = runner.invoke(run, [
        '--type', 'pypi',
        '--url', 'https://some-url'
    ])
    assert result.exit_code == 0
    mock_loader.assert_called_once_with(url='https://some-url')  # constructor
