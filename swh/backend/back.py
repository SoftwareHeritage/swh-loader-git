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


def _build_object(sha1_hex, type, content=None, size=None, git_sha1=None):
    """Build the object.
    """
    return {'sha1': sha1_hex,
            'type': type,
            'content': content,
            'size': size,
            'git-sha1': git_sha1}


_uri_types = {'commits': models.Type.commit,
              'blobs': models.Type.blob,
              'trees': models.Type.tree}

def _do_action(action_fn, uri_type, sha1_hex):
    uri_type_ok = _uri_types.get(uri_type, None)
    if uri_type_ok is None:
        return make_response('Bad request!', 400)

    git_object = _build_object(sha1_hex,
                               uri_type_ok,
                               request.form.get('content', None),
                               request.form.get('size', None),
                               request.form.get('git-sha1', None))
    return action_fn(app.config['conf'], git_object)


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
        res = store.add(config, git_object)

        if res is None:
             return make_response('Bad request!', 400)
        elif res is False:
            logging.error('store %s %s' % (sha1_hex, type))
            return make_response('Internal server error!', 500)
        else:
            return make_response('Successful creation!', 204)


@app.route('/git/<uri_type>/<sha1_hex>')
def object_exists_p(uri_type, sha1_hex):
    """Return the given commit or not."""
    return _do_action(lookup, uri_type, sha1_hex)


@app.route('/git/<uri_type>/<sha1_hex>', methods=['PUT'])
def put_object(uri_type, sha1_hex):
    """Put an object in storage.
    """
    return _do_action(add_object, uri_type, sha1_hex)


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
