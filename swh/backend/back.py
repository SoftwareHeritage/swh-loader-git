#!/usr/bin/env python3
# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from flask import Flask, make_response, json, request

from swh.gitloader import models
from swh import hash, db


app = Flask(__name__)


@app.route('/')
def hello():
    return 'Dev SWH API'


def lookup(hexsha1, predicate_fn, type=None):
    """Looking up type object with sha1.
    - predicate_fn is a lookup function taking in this order a db_conn, binary
    sha1 and optionally a type to look for in the backend.
    - type is of models.Type (commit, tree, blob)
    This function returns an http response
    """
    app.logger.debug('Looking up %s: %s '
                     % (type, hexsha1))
    try:
        sha1_bin = hash.sha1_bin(hexsha1)
    except:
        logging.error("The sha1 provided must be in hexadecimal.")
        return make_response('Bad request!', 400)

    with db.connect(app.config['conf']['db_url']) as db_conn:
        if predicate_fn(db_conn, sha1_bin, type):
            return json.jsonify(sha1=hexsha1)  # 200
        return make_response('Not found!', 404)


@app.route('/commits/<hexsha1>')
def commit_exists_p(hexsha1):
    """Return the given commit or not."""
    return lookup(hexsha1, models.find_object, models.Type.commit)


@app.route('/trees/<hexsha1>')
def tree_exists_p(hexsha1):
    """Return the given commit or not."""
    return lookup(hexsha1, models.find_object, models.Type.tree)


@app.route('/blobs/<hexsha1>')
def blob_exists_p(hexsha1):
    """Return the given commit or not."""
    return lookup(hexsha1, models.find_blob)


def persist_object(hexsha1, predicate_fn, insert_fn, type):
    """Add object in storage.
    """
    try:
        sha1_bin = hash.sha1_bin(hexsha1)
    except:
        logging.error("The sha1 provided must be in hexadecimal.")
        return make_response('Bad request!', 400)

    # body = request.form  # do not care for the body for the moment
    with db.connect(app.config['conf']['db_url']) as db_conn:
        if predicate_fn(db_conn, sha1_bin, type):
            return make_response('Successful update!', 200)  # immutable
        else:
            # creation
            insert_fn(db_conn, sha1_bin, type)
            return make_response('Successful creation!', 204)


# put objects (tree/commits)
@app.route('/commits/<hexsha1>', methods=['PUT'])
def put_commit(hexsha1):
    """Put a commit in storage.
    """
    return persist_object(hexsha1, models.find_object, models.add_object, models.Type.commit)


@app.route('/trees/<hexsha1>', methods=['PUT'])
def put_tree(hexsha1):
    """Put a tree in storage.
    """
    return persist_object(hexsha1, models.find_object, models.add_object, models.Type.tree)


@app.route('/blobs/<hexsha1>', methods=['PUT'])
def put_blob(hexsha1):
    """Put a blob in storage.
    """
    try:
        sha1_bin = hash.sha1_bin(hexsha1)
    except:
        logging.error("The sha1 provided must be in hexadecimal.")
        return make_response('Bad request!', 400)

    body = request.form  # do not care for the body for the moment
    size, obj_git_sha = body['size'], body['git-sha1']

    with db.connect(app.config['conf']['db_url']) as db_conn:
        if models.find_blob(db_conn, sha1_bin):
            return make_response('Successful update!', 200)  # immutable
        else:
            # creation
            models.add_blob(db_conn, sha1_bin, size, obj_git_sha)
            return make_response('Successful creation!', 204)



def run(conf):
    # setup app
    app.config['conf'] = conf
    app.debug = True if conf['debug'] == 'true' else False

    app.run()
