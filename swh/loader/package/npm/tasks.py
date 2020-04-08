# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import shared_task

from swh.loader.package.npm.loader import NpmLoader


@shared_task(name=__name__ + ".LoadNpm")
def load_npm(*, url: str):
    """Load Npm package"""
    return NpmLoader(url).load()
