# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

import click
import pkg_resources

from typing import Any

from swh.core.cli import CONTEXT_SETTINGS
from swh.scheduler.cli.utils import parse_options


logger = logging.getLogger(__name__)


LOADERS = {entry_point.name.split('.', 1)[1]: entry_point
           for entry_point in pkg_resources.iter_entry_points('swh.workers')
           if entry_point.name.split('.', 1)[0] == 'loader'}


SUPPORTED_LOADERS = list(LOADERS)


def get_loader(name: str, **kwargs) -> Any:
    """Given a loader name, instantiate it.

    Args:
        name: Loader's name
        kwargs: Configuration dict (url...)

    Returns:
        An instantiated loader

    """
    if name not in LOADERS:
        raise ValueError(
            'Invalid loader %s: only supported loaders are %s' %
            (name, SUPPORTED_LOADERS))

    registry_entry = LOADERS[name].load()()
    logger.debug(f'registry: {registry_entry}')
    loader_cls = registry_entry['loader']
    logger.debug(f'loader class: {loader_cls}')
    return loader_cls(**kwargs)


@click.command(name='run', context_settings=CONTEXT_SETTINGS)
@click.option('--type', '-t', help='Loader to run',
              type=click.Choice(SUPPORTED_LOADERS))
@click.option('--url', '-u', default=None,
              help="Origin url to load")
@click.argument('options', nargs=-1)
@click.pass_context
def run(ctx, type, url, options):
    """Loader cli tools

    Load an origin from its url with loader <name>

    """
    (_, kw) = parse_options(options)
    logger.debug(f'kw: {kw}')
    loader = get_loader(type, url=url, **kw)
    result = loader.load()
    click.echo(result)
