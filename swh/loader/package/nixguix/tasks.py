# Copyright (C) 2020-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import shared_task

from swh.loader.package.nixguix.loader import NixGuixLoader


@shared_task(name=__name__ + ".LoadNixguix")
def load_nixguix(*, url=None):
    """Load functional (e.g. guix/nix) package"""
    return NixGuixLoader.from_configfile(url=url).load()
