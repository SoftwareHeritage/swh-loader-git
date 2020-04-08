# Copyright (C) 2019-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import shared_task

from swh.loader.package.cran.loader import CRANLoader


@shared_task(name=__name__ + ".LoadCRAN")
def load_cran(url=None, artifacts=[]):
    """Load CRAN's artifacts"""
    return CRANLoader(url, artifacts).load()
