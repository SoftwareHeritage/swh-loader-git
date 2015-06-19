#!/usr/bin/env python3
# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from flask import Flask, make_response, json, request

from swh import models, hash, db
from swh.gitloader import storage

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
    app.logger.debug('lookup %s %s ' % (type, hexsha1))
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
    return lookup(hexsha1, models.find_blob, models.Type.blob)


def hex_to_bin(hexsha1):
    """Given an hexadecimal sha1, return its binary equivalent.
    Return None if hexsha1 is not the right sha1."""
    try:
        return hash.sha1_bin(hexsha1)
    except:
        return None


def persist_object(hexsha1, predicate_fn, insert_fn, type):
    """Add object in storage.
    """
    logging.debug(app.config['conf'])
    sha1_bin = hex_to_bin(hexsha1)
    if sha1_bin is None:
        logging.error("The sha1 provided must be in hexadecimal.")
        return make_response('Bad request!', 400)

    payload = request.form
    logging.debug("payload: %s" % payload)
    blob_content = payload['content']

    # payload = request.form  # do not care for the payload for the moment
    with db.connect(app.config['conf']['db_url']) as db_conn:
        if predicate_fn(db_conn, sha1_bin, type):
            return make_response('Successful update!', 200)  # immutable
        else:
            try:
                logging.debug('store %s %s' % (hexsha1, type))
                storage.add_object(app.config['conf']['object_content_storage_dir'],
                                   hexsha1,
                                   blob_content,
                                   app.config['conf']['folder_depth'])
                insert_fn(db_conn, sha1_bin, type)
                return make_response('Successful creation!', 204)
            except IOError:
                db_conn.rollback()
                logging.error('store %s %s' % (hexsha1, type))
                return make_response('Internal server error!', 500)

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
    logging.debug('store blob %s' % hexsha1)
    sha1_bin = hex_to_bin(hexsha1)
    if sha1_bin is None:
        logging.error("The sha1 provided must be in hexadecimal.")
        return make_response('Bad request!', 400)

    payload = request.form
    logging.debug("payload: %s" % payload)
    size, obj_git_sha_hex, blob_content = (payload['size'],
                                           payload['git-sha1'],
                                           payload['content'])

    # FIXME: to improve
    obj_git_sha_bin = hex_to_bin(obj_git_sha_hex)
    if obj_git_sha_bin is None:
        logging.error("The sha1 provided must be in hexadecimal.")
        return make_response('Bad request!', 400)

    with db.connect(app.config['conf']['db_url']) as db_conn:
        if models.find_blob(db_conn, sha1_bin):
            return make_response('Successful update!', 200)  # immutable
        else:
            try:
                logging.debug('store blob %s' % hexsha1)
                storage.add_blob(app.config['conf']['file_content_storage_dir'],
                                 hexsha1,
                                 blob_content,
                                 app.config['conf']['folder_depth'],
                                 app.config['conf']['blob_compression'])

                # creation
                models.add_blob(db_conn, sha1_bin, size, obj_git_sha_bin)
                return make_response('Successful creation!', 204)
            except IOError:
                db_conn.rollback()
                logging.error('store blob %s' % hexsha1)
                return make_response('Internal server error', 500)


def run(conf):
    """Run the api's server.
    conf is a dictionary of keywords:
    - 'db_url' the db url's access (through psycopg2 format)
    - 'file_content_storage_dir'   where to store blobs on disk
    - 'object_content_storage_dir' where to store commits/trees on disk
    - 'port'   to override the default of 5000 (from the underlying layer: flask)
    - 'debug'  activate the verbose logs
    """
    app.config['conf'] = conf  # app.config is the app's state (accessible)

    app.run(port=conf['port'] if 'port' in conf else None,
            debug=True if conf['debug'] == 'true' else False)
