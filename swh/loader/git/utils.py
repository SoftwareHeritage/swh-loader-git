# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

"""Utilities helper functions"""

import datetime
import os
import shutil
import tempfile

from swh.core import tarball


def init_git_repo_from_archive(project_name, archive_path,
                               root_temp_dir='/tmp'):
    """Given a path to an archive containing a git repository.

    Uncompress that archive to a temporary location and returns the path.

    If any problem whatsoever is raised, clean up the temporary location.

    Args:
        project_name (str): Project's name
        archive_path (str): Full path to the archive
        root_temp_dir (str): Optional temporary directory mount point
                             (default to /tmp)

    Returns
        A tuple:
        - temporary folder: containing the mounted repository
        - repo_path, path to the mounted repository inside the temporary folder

    Raises
        ValueError in case of failure to run the command to uncompress

    """
    temp_dir = tempfile.mkdtemp(
        suffix='.swh.loader.git', prefix='tmp.', dir=root_temp_dir)

    try:
        # create the repository that will be loaded with the dump
        tarball.uncompress(archive_path, temp_dir)
        repo_path = os.path.join(temp_dir, project_name)
        # tarball content may not be as expected (e.g. no top level directory
        # or a top level directory with a name different from project_name),
        # so try to make it loadable anyway
        if not os.path.exists(repo_path):
            os.mkdir(repo_path)
            for root, dirs, files in os.walk(temp_dir):
                if '.git' in dirs:
                    shutil.copytree(os.path.join(root, '.git'),
                                    os.path.join(repo_path, '.git'))
                    break
        return temp_dir, repo_path
    except Exception as e:
        shutil.rmtree(temp_dir)
        raise e


def check_date_time(timestamp):
    """Check date time for overflow errors.

    Args:
        timestamp (timestamp): Timestamp in seconds

    Raise:
        Any error raised by datetime fromtimestamp conversion error.

    """
    if not timestamp:
        return None
    datetime.datetime.fromtimestamp(timestamp,
                                    datetime.timezone.utc)
