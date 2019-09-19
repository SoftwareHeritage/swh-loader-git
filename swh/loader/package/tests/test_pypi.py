# Copyright (C) 2019 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import pytest

from swh.loader.package.pypi import PyPILoader

# scenario

# configuration error #

def test_badly_configured_loader_raise():
    """Badly configured loader should raise"""
    assert 'SWH_CONFIG_FILENAME' in os.environ  # cf. tox.ini
    del os.environ['SWH_CONFIG_FILENAME']
    with pytest.raises(ValueError) as e:
        PyPILoader(url='some-url')

    assert 'Misconfiguration' in e.value.args[0]

# "edge" cases (for the same origin) #

# no release artifact:
# {visit full, status: uneventful, no contents, etc...}

# problem during loading:
# {visit: partial, status: uneventful, no snapshot}

# problem during loading: failure early enough in between swh contents...
# some contents (contents, directories, etc...) have been written in storage
# {visit: partial, status: eventful, no snapshot}

# problem during loading: failure late enough we can have snapshots (some
# revisions are written in storage already)
# {visit: partial, status: eventful, snapshot}

# "normal" cases (for the same origin) #

# release artifact, no prior visit
# {visit full, status eventful, snapshot}

# release artifact, no new artifact
# {visit full, status uneventful, same snapshot as before}

# release artifact, new artifact
# {visit full, status full, new snapshot with shared history as prior snapshot}

# release artifact, old artifact with different checksums
# {visit full, status full, new snapshot with shared history and some new
# different history}
