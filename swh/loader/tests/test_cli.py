# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest


from swh.loader.cli import run, list, get_loader, SUPPORTED_LOADERS
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


def test_run_help(swh_config):
    """Help message should be ok

    """
    runner = CliRunner()
    result = runner.invoke(run, ['-h'])

    assert result.exit_code == 0
    expected_help_msg = """Usage: run [OPTIONS] [archive|cran|debian|deposit|functional|npm|pypi] URL
           [OPTIONS]...

  Ingest with loader <type> the origin located at <url>

Options:
  -h, --help  Show this message and exit.
"""  # noqa

    assert result.output.startswith(expected_help_msg)


def test_run_pypi(mocker, swh_config):
    """Triggering a load should be ok

    """
    mock_loader = mocker.patch('swh.loader.package.pypi.loader.PyPILoader')
    runner = CliRunner()
    result = runner.invoke(run, ['pypi', 'https://some-url'])
    assert result.exit_code == 0
    mock_loader.assert_called_once_with(url='https://some-url')  # constructor


def test_list_help(mocker, swh_config):
    """Triggering a load should be ok

    """
    runner = CliRunner()
    result = runner.invoke(list, ['--help'])
    assert result.exit_code == 0
    expected_help_msg = """Usage: list [OPTIONS] [[all|archive|cran|debian|deposit|functional|npm|pypi]]

  List supported loaders and optionally their arguments

Options:
  -h, --help  Show this message and exit.
"""  # noqa
    assert result.output.startswith(expected_help_msg)


def test_list_help_npm(mocker, swh_config):
    """Triggering a load should be ok

    """
    runner = CliRunner()
    result = runner.invoke(list, ['npm'])
    assert result.exit_code == 0
    expected_help_msg = '''Loader: Load npm origin's artifact releases into swh archive.
signature: (url: str)
'''  # noqa
    assert result.output.startswith(expected_help_msg)
