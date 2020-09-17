# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from typing import Any, Dict

import pkg_resources

try:
    __version__ = pkg_resources.get_distribution("swh.loader.core").version
except pkg_resources.DistributionNotFound:
    __version__ = "devel"


DEFAULT_PARAMS: Dict[str, Any] = {
    "headers": {"User-Agent": "Software Heritage Loader (%s)" % (__version__)}
}
