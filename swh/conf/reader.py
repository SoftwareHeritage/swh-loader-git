#!/usr/bin/env python3

# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import configparser
import os


def read(conf_file, default_conf):
    """Read the user's configuration file.
    Fill in the gap using `default_conf`.
`default_conf` is similar to this:
DEFAULT_CONF = {
    'a': ('string', '/tmp/swh-git-loader/log'),
    'b' : ('string', 'dbname=swhgitloader')
    'c' : ('bool', true)
    'e' : ('bool', None)
    'd' : ('int', 10)
}

    """
    config = configparser.ConfigParser(defaults=default_conf)
    config.read(os.path.expanduser(conf_file))
    conf = config._sections['main']

    # remaining missing default configuration key are set
    for key in default_conf:
        type, default_value = default_conf[key]
        val = conf.get(key, None)
        if not val:  # fallback to default value
            conf[key] = default_value
        else:  # force only the type for int
            if type == 'int':
                conf[key] = int(val)

    return conf


def prepare_folders(conf, *keys):
    """Prepare the folder mentioned in config under keys.
    """
    def makedir(folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    for key in keys:
        makedir(conf[key])
