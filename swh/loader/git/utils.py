# Copyright (C) 2017  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import shutil
import tempfile

from subprocess import call


def init_git_repo_from_archive(archive_path, root_temp_dir='/tmp'):
    """Given a path to an archive containing a git repository.
    Uncompress that archive to a temporary location and returns the path.

    If any problem whatsoever is raised, clean up the temporary location.

    Returns:
        A tuple:
        - temporary folder: containing the mounted repository
        - repo_path, path to the mounted repository inside the temporary folder

    Raises:
        ValueError in case of failure to run the command to uncompress

    """
    project_name = os.path.basename(os.path.dirname(archive_path))
    temp_dir = tempfile.mkdtemp(suffix='.swh.loader.git',
                                prefix='tmp.',
                                dir=root_temp_dir)

    try:
        repo_path = os.path.join(temp_dir, project_name)

        # create the repository that will be loaded with the dump
        cmd = ['unzip', '-q', '-o', archive_path, '-d', temp_dir]
        r = call(cmd)

        if r != 0:
            raise ValueError(
                'Failed to uncompress git repository for %s' %
                project_name)

        return temp_dir, repo_path
    except Exception as e:
        shutil.rmtree(temp_dir)
        raise e
