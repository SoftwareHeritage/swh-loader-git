# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import sys

import click

from swh.storage import get_storage

from .tar_out import GitTarOutLoader


@click.command()
@click.argument("url")
@click.argument("output_path")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
@click.option(
    "--index/--no-index", default=True, help="Whether to generate the Git index (.idx)"
)
@click.option(
    "--prefix",
    default=None,
    help="Prefix to add to all paths in the archive (defaults to project name)",
)
def main(url, output_path, verbose, index, prefix):
    """Fetch a Git repository from URL and save it as a bare repository in
    OUTPUT_PATH (uncompressed tar archive).

    If index generation is disabled (via --no-index), the archive will be
    missing the .idx file and thus won't be immediately functional.
    You can generate it manually later after extracting the tarball using:
    `git index-pack objects/pack/pack-<HASH>.pack`
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s:%(name)s:%(message)s")

    storage = get_storage(cls="memory")
    loader = GitTarOutLoader(
        storage=storage,
        url=url,
        tar_output_path=output_path,
        incremental=False,
        generate_index=index,
        prefix=prefix,
    )
    result = loader.load()
    if result["status"] == "eventful":
        click.echo(f"Successfully created {output_path}")
    elif result["status"] == "uneventful":
        click.echo("Loader finished: nothing to fetch.")
    else:
        click.echo(f"Loader finished with status: {result['status']}")
        if "error" in result:
            click.echo(f"Error: {result['error']}")
        sys.exit(1)


if __name__ == "__main__":
    main()
