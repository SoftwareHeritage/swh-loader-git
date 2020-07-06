# Copyright (C) 2018-2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import subprocess

from typing import Optional


def prepare_repository_from_archive(
    archive_path: str,
    filename: Optional[str] = None,
    tmp_path: str = "/tmp",
    uncompress_archive: bool = True,
) -> str:
    if uncompress_archive:
        # uncompress folder/repositories/dump for the loader to ingest
        subprocess.check_output(["tar", "xf", archive_path, "-C", tmp_path])
    # build the origin url (or some derivative form)
    _fname = filename if filename else os.path.basename(archive_path)
    return f"file://{tmp_path}/{_fname}"
