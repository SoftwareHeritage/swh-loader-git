#!/usr/bin/env python3
# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from flask import Flask, make_response, json, request

from swh.storage import store, models

app = Flask(__name__)


@app.route('/')
def hello():
    return 'Dev SWH API'


def lookup(config, git_object):
    """Looking up type object with sha1.
    - predicate_fn is a lookup function taking in this order a db_conn, binary
    sha1 and optionally a type to look for in the backend.
    - type is of models.Type (commit, tree, blob)
    This function returns an http response
    """
    sha1_hex = git_object['sha1']
    logging.debug('read %s %s' % (git_object['type'], sha1_hex))
    res = store.find(config, git_object)
    if res:
        return json.jsonify(sha1=sha1_hex)  # 200
    return make_response('Not found!', 404)


@app.route('/git/commits/<sha1_hex>')
def commit_exists_p(sha1_hex):
    """Return the given commit or not."""
    git_object = {'sha1': sha1_hex,
                  'type': models.Type.commit}
    return lookup(app.config['conf'], git_object)


@app.route('/git/trees/<sha1_hex>')
def tree_exists_p(sha1_hex):
    """Return the given commit or not."""
    git_object = {'sha1': sha1_hex,
                  'type': models.Type.tree}
    return lookup(app.config['conf'], git_object)


@app.route('/git/blobs/<sha1_hex>')
def blob_exists_p(sha1_hex):
    """Return the given commit or not."""
    git_object = {'sha1': sha1_hex,
                  'type': models.Type.blob}
    return lookup(app.config['conf'], git_object)

def add_object(config, git_object):
    """Add object in storage.
    """
    type = git_object['type']
    sha1_hex = git_object['sha1']
    logging.debug('store %s %s' % (type, sha1_hex))

    if store.find(config, git_object):
        logging.debug('update %s %s' % (sha1_hex, type))
        return make_response('Successful update!', 200)  # immutable
    else:
        logging.debug('store %s %s' % (sha1_hex, type))
        res = store.add(config,
                        git_object)
        if res is None:
            return make_response('Bad request!', 400)
        elif res is False:
            return make_response('Internal server error!', 500)
        else:
            return make_response('Successful creation!', 204)


@app.route('/git/commits/<sha1_hex>', methods=['PUT'])
def put_commit(sha1_hex):
    """Put a commit in storage.
    """
    git_object = {'sha1': sha1_hex,
                  'type': models.Type.commit,
                  'content': request.form['content']}
    return add_object(app.config['conf'], git_object)


@app.route('/git/trees/<sha1_hex>', methods=['PUT'])
def put_tree(sha1_hex):
    """Put a tree in storage.
    """
    logging.debug('store tree %s' % sha1_hex)
    git_object = {'sha1': sha1_hex,
                  'type': models.Type.tree,
                  'content': request.form['content']}
    return add_object(app.config['conf'], git_object)


@app.route('/git/blobs/<sha1_hex>', methods=['PUT'])
def put_blob(sha1_hex):
    """Put a blob in storage.
    """
    logging.debug('store blob %s' % sha1_hex)
    git_object = {'sha1': sha1_hex,
                  'type': models.Type.blob,
                  'content': request.form['content'],
                  'size': request.form['size'],
                  'git-sha1': request.form['git-sha1']}
    return add_object(app.config['conf'], git_object)


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
