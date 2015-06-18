#!/usr/bin/env python3
# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import argparse
import configparser
import logging
import os

from flask import Flask, make_response, json

from swh.gitloader import models
from swh import hash, db


# Default configuration file
DEFAULT_CONF_FILE = '~/.config/swh/back.ini'

# default configuration (can be overriden by the DEFAULT_CONF_FILE)
DEFAULT_CONF = {
    'file_content_storage_dir': '/tmp/swh-git-loader/file-content-storage',
    'object_content_storage_dir': '/tmp/swh-git-loader/git-object-storage',
    'log_dir': '/tmp/swh-git-loader/log',
    'db_url': 'dbname=swhgitloader',
    'blob_compression': None,
    'folder_depth': 4,
}


def parse_args():
    """Parse the configuration for the cli.

    """

    cli = argparse.ArgumentParser(
        description='Parse git repository objects to load them into DB.')
    cli.add_argument('--verbose', '-v', action='store_true',
                     help='Verbosity level in log file.')
    cli.add_argument('--config', '-c', help='configuration file path')

    subcli = cli.add_subparsers(dest='action')
    subcli.add_parser('initdb', help='initialize DB')
    subcli.add_parser('cleandb', help='clean DB')

    args = cli.parse_args()

    return args

def read_conf(args):
    """Read the user's configuration file.

    args contains the repo to parse.
    Transmit to the result.
    """
    config = configparser.ConfigParser(defaults=DEFAULT_CONF)
    conf_file = args.config or DEFAULT_CONF_FILE
    config.read(os.path.expanduser(conf_file))
    conf = config._sections['main']

    # propagate CLI arguments to conf dictionary
    conf['action'] = args.action

    conf['folder_depth'] = DEFAULT_CONF['folder_depth'] \
                              if 'folder_depth' not in conf \
                              else int(conf['folder_depth'])

    if 'debug' not in conf:
        conf['debug'] = None

    if 'blob_compression' not in conf:
        conf['blob_compression'] = DEFAULT_CONF['blob_compression']

    return conf


app = Flask(__name__)


@app.route('/')
def hello():
    return 'SWH API - In dev for the moment!'


def lookup(hexsha1, type):
    """Lookup function"""
    app.logger.debug('Looking up commit: %s ' % hexsha1)
    try:
        sha1_bin = hash.sha1_bin(hexsha1)
        with db.connect(app.config['conf']['db_url']) as db_conn:
            if models.find_object(db_conn, sha1_bin, type):
                return json.jsonify(sha1=hexsha1)  # 200
            return make_response('Not found!', 404)
    except:
        return make_response('Bad request!', 400)

@app.route('/commits/<hexsha1>')
def is_commit_in(hexsha1):
    """Return the given commit or not."""
    return lookup(hexsha1, models.Type.commit)

@app.route('/trees/<hexsha1>')
def is_tree_in(hexsha1):
    """Return the given commit or not."""
    return lookup(hexsha1, models.Type.tree)


if __name__ == '__main__':
    args = parse_args()
    conf = read_conf(args)

    log_filename = os.path.join(conf['log_dir'], 'back.log')
    logging.basicConfig(filename=log_filename,
                        level=logging.DEBUG if args.verbose else logging.INFO)

    # setup app
    app.config['conf'] = conf
    app.debug = True if conf['debug'] == 'true' else False

    app.run()
