# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import inspect
import logging

import click
import iso8601
import pkg_resources

from typing import Any

from swh.core.cli import CONTEXT_SETTINGS
from swh.scheduler.cli.utils import parse_options


logger = logging.getLogger(__name__)


LOADERS = {
    entry_point.name.split(".", 1)[1]: entry_point
    for entry_point in pkg_resources.iter_entry_points("swh.workers")
    if entry_point.name.split(".", 1)[0] == "loader"
}


SUPPORTED_LOADERS = sorted(list(LOADERS))


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
            "Invalid loader %s: only supported loaders are %s"
            % (name, SUPPORTED_LOADERS)
        )

    registry_entry = LOADERS[name].load()()
    logger.debug(f"registry: {registry_entry}")
    loader_cls = registry_entry["loader"]
    logger.debug(f"loader class: {loader_cls}")
    return loader_cls(**kwargs)


@click.group(name="loader", context_settings=CONTEXT_SETTINGS)
@click.pass_context
def loader(ctx):
    """Loader cli tools

    """
    pass


@loader.command(name="run", context_settings=CONTEXT_SETTINGS)
@click.argument("type", type=click.Choice(SUPPORTED_LOADERS))
@click.argument("url")
@click.argument("options", nargs=-1)
@click.pass_context
def run(ctx, type, url, options):
    """Ingest with loader <type> the origin located at <url>"""
    (_, kw) = parse_options(options)
    logger.debug(f"kw: {kw}")
    visit_date = kw.get("visit_date")
    if visit_date and isinstance(visit_date, str):
        visit_date = iso8601.parse_date(visit_date)
        kw["visit_date"] = visit_date

    loader = get_loader(type, url=url, **kw)
    result = loader.load()
    click.echo(result)


@loader.command(name="list", context_settings=CONTEXT_SETTINGS)
@click.argument("type", default="all", type=click.Choice(["all"] + SUPPORTED_LOADERS))
@click.pass_context
def list(ctx, type):
    """List supported loaders and optionally their arguments"""
    if type == "all":
        loaders = ", ".join(SUPPORTED_LOADERS)
        click.echo(f"Supported loaders: {loaders}")
    else:
        registry_entry = LOADERS[type].load()()
        loader_cls = registry_entry["loader"]
        doc = inspect.getdoc(loader_cls).strip()

        # Hack to get the signature of the class even though it subclasses
        # Generic, which reimplements __new__.
        # See <https://bugs.python.org/issue40897>
        signature = inspect.signature(loader_cls.__init__)
        signature_str = str(signature).replace("self, ", "")

        click.echo(f"Loader: {doc}\nsignature: {signature_str}")
