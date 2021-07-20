# Copyright (C) 2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import shared_task

from swh.loader.package.opam.loader import OpamLoader


@shared_task(name=__name__ + ".LoadOpam")
def load_opam(url, opam_root, opam_instance, opam_url, opam_package):
    """Load Opam's artifacts"""
    return OpamLoader.from_configfile(
        url=url,
        opam_root=opam_root,
        opam_instance=opam_instance,
        opam_url=opam_url,
        opam_package=opam_package,
    ).load()
