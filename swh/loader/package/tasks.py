# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from celery import current_app as app
from swh.loader.package.loader import GNULoader


@app.task(name=__name__ + '.LoadGNU')
def load_gnu(name, origin_url=None, tarballs=None):
    return GNULoader().load(name, origin_url,
                            tarballs=tarballs)
