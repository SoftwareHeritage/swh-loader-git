#!/usr/bin/env python3
# coding: utf-8

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pstats
import os

most_significative = 20

filepath = './scratch/swhgitloader.profile'
profile_file = filepath if os.path.isfile(filepath) else None

if profile_file is None:
    raise BaseException ("Profile file %s must exist. Please run `make profile-run first.` " % filepath)

p = pstats.Stats(profile_file)

# Remove the extraneous paths from all the module names
# Sort the entries according to the standard module/line/name
# And then print all the stats
# p.strip_dirs().sort_stats(-1).print_stats()

# Sort by name and print the stats
# p.sort_stats('name')
# p.print_stats()

# To determine what's taking most of the time:
# Sort according to the 'cumulative time in a function' column
# Display only the first 10 columns
p.sort_stats('cumulative').print_stats(most_significative)

# What functions are looping a lot, and taking a lot of time, you would do:
# p.sort_stats('time').print_stats(10)

# Sorts statistics with a primary key of time, and a secondary key of cumulative
# time, and then prints out some of the statistics.
# p.sort_stats('time', 'cumulative').print_stats(most_significative)
