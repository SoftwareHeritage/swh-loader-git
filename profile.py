#!/usr/bin/env python3

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import sys
import os

from sgloader.sgloader import run


# Clean the db up
action = "initdb"
db_url = "dbname=swhgitloader user=tony"

# cleanup first
run("cleandb", db_url)


# Then run
repo_path = os.path.expanduser("~/repo/perso/dot-files")
dataset_dir = "./dataset"
#run(action, db_url, repo_path, dataset_dir)
