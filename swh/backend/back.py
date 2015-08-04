#!/usr/bin/env python3
# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from datetime import datetime

from flask import Flask, make_response, json, request

from swh.storage import store

app = Flask(__name__)


@app.route('/')
def hello():
    """A simple api to define what the server is all about.
    FIXME: A redirect towards a static page defining the routes would be nice.
    """
    return 'Dev SWH API'


def lookup(config, vcs_object):
    """Looking up type object with sha1.
    - predicate_fn is a lookup function taking in this order a db_conn, binary
    sha1 and optionally a type to look for in the backend.
    - type is of models.Type (revision, directory, content)
    This function returns an http response
    """
    sha1hex = vcs_object['sha1']
    logging.debug('read %s %s' % (vcs_object['type'], sha1hex))
    res = store.find(config, vcs_object)
    if res:
        return json.jsonify(sha1=sha1hex)  # 200
    return make_response('Not found!', 404)


def build_content(sha1hex, payload):
    """Build a content object from the payload.
    """
    payload = payload if payload else {}
    return {'sha1': sha1hex,
            'type': store.Type.content,
            'content-sha1': payload.get('content-sha1'),
            'content-sha256': payload.get('content-sha256'),
            'content': payload.get('content'),
            'size': payload.get('size')}


def build_directory(sha1hex, payload):
    """Build a directory object from the payload.
    """
    payload = payload if payload else {}  # FIXME get hack -> split get-post/put
    directory = {'sha1': sha1hex,
                 'type': store.Type.directory,
                 'content': payload.get('content')}

    directory_entries = []
    for entry in payload.get('entries', []):
        directory_entry = build_directory_entry(sha1hex, entry)
        directory_entries.append(directory_entry)

    directory.update({'entries': directory_entries})
    return directory


def date_from_string(str_date):
    """Convert a string date with format '%a, %d %b %Y %H:%M:%S +0000'.
    """
    return datetime.strptime(str_date, '%a, %d %b %Y %H:%M:%S +0000')


def build_directory_entry(parent_sha1hex, entry):
    """Build a directory object from the entry.
    """
    return {'name': entry['name'],
            'target-sha1': entry['target-sha1'],
            'nature': entry['nature'],
            'perms': entry['perms'],
            'atime': date_from_string(entry['atime']),
            'mtime': date_from_string(entry['mtime']),
            'ctime': date_from_string(entry['ctime']),
            'parent': entry['parent']}


def build_revision(sha1hex, payload):
    """Build a revision object from the payload.
    """
    obj = {'sha1': sha1hex,
           'type': store.Type.revision}
    if payload:
        obj.update({'content': payload['content'],
                    'date': date_from_string(payload['date']),
                    'directory': payload['directory'],
                    'message': payload['message'],
                    'author': payload['author'],
                    'committer': payload['committer'],
                    'parent-sha1s': payload['parent-sha1s']})
    return obj


def build_release(sha1hex, payload):
    """Build a release object from the payload.
    """
    obj = {'sha1': sha1hex,
           'type': store.Type.release}
    if payload:
        obj.update({'sha1': sha1hex,
                    'content': payload['content'],
                    'revision': payload['revision'],
                    'date': payload['date'],
                    'name': payload['name'],
                    'comment': payload['comment'],
                    'author': payload['author']})
    return obj


def build_occurrence(sha1hex, payload):
    """Build a content object from the payload.
    """
    obj = {'sha1': sha1hex,
           'type': store.Type.occurrence}
    if payload:
        obj.update({'content': payload['content'],
                    'reference': payload['reference'],
                    'type': store.Type.occurrence,
                    'revision': sha1hex,
                    'url-origin': payload['url-origin']})
    return obj


def build_origin(sha1hex, payload):
    """Build an origin.
    """
    obj = {'id': payload['url'],
           'origin-type': payload['type']}
    return obj

# dispatch on build object function for the right type
_build_object_fn = {store.Type.revision: build_revision,
                    store.Type.directory: build_directory,
                    store.Type.content: build_content,
                    store.Type.release: build_release,
                    store.Type.occurrence: build_occurrence}

# from uri to type
_uri_types = {'revisions': store.Type.revision,
              'directories': store.Type.directory,
              'contents': store.Type.content,
              'releases': store.Type.release,
              'occurrences': store.Type.occurrence}


def _do_action(action_fn, uri_type, sha1hex):
    uri_type_ok = _uri_types.get(uri_type, None)
    if uri_type_ok is None:
        return make_response('Bad request!', 400)

    payload = request.get_json()
    vcs_object = _build_object_fn[uri_type_ok](sha1hex, payload)
    return action_fn(app.config['conf'], vcs_object)


def add_object(config, vcs_object):
    """Add object in storage.
    """
    type = vcs_object['type']
    sha1hex = vcs_object['sha1']
    logging.debug('store %s %s' % (type, sha1hex))

    if store.find(config, vcs_object):
        logging.debug('update %s %s' % (sha1hex, type))
        return make_response('Successful update!', 200)  # immutable
    else:
        logging.debug('store %s %s' % (sha1hex, type))
        res = store.add(config, vcs_object)
        if res is None:
            return make_response('Bad request!', 400)
        elif res is False:
            logging.error('store %s %s' % (sha1hex, type))
            return make_response('Internal server error!', 500)
        else:
            return make_response('Successful creation!', 204)


@app.route('/objects/', methods=['POST'])
def filter_unknowns_objects():
    """Filters unknown sha1 to the backend and returns them.
    """
    if request.headers.get('Content-Type') != 'application/json':
        return make_response('Bad request. Expected json data!', 400)

    payload = request.get_json()
    sha1s = payload.get('sha1s')
    if sha1s is None:
        return make_response(
            "Bad request! Expects 'sha1s' key with list of hexadecimal sha1s.",
            400)

    unknowns_sha1s = store.find_unknowns(app.config['conf'], None, sha1s)

    if unknowns_sha1s is None:
        return make_response('Bad request!', 400)
    else:
        return json.jsonify(sha1s=unknowns_sha1s)


@app.route('/vcs/<uri_type>/', methods=['POST'])
def filter_unknowns_type(uri_type):
    """Filters unknown sha1 to the backend and returns them.
    """
    if request.headers.get('Content-Type') != 'application/json':
        return make_response('Bad request. Expected json data!', 400)

    payload = request.get_json()
    sha1s = payload.get('sha1s')
    if sha1s is None:
        return make_response(
            "Bad request! Expects 'sha1s' key with list of hexadecimal sha1s.",
            400)

    unknowns_sha1s = store.find_unknowns(app.config['conf'], _uri_types[uri_type], sha1s)

    if unknowns_sha1s is None:
        return make_response('Bad request!', 400)
    else:
        return json.jsonify(sha1s=unknowns_sha1s)


@app.route('/origins/', methods=['POST'])
def post_origin():
    """Post an origin.
    """
    if request.headers.get('Content-Type') != 'application/json':
        return make_response('Bad request. Expected json data!', 400)

    origin = request.json

    try:
        origin_found = store.find_origin(app.config['conf'], origin)
        if origin_found:
            return json.jsonify(id=origin_found[0])
        else:
            return make_response('Origin not found!', 404)
    except:
        return make_response('Bad request!', 400)


@app.route('/origins/', methods=['PUT'])
def put_origin():
    """Create an origin or returns it if already existing.
    """
    if request.headers.get('Content-Type') != 'application/json':
        return make_response('Bad request. Expected json data!', 400)

    origin = request.json

    try:
        origin_found = store.find_origin(app.config['conf'], origin)
        if origin_found:
            return json.jsonify(id=origin_found[0])  # FIXME 204
        else:
            origin_id = store.add_origin(app.config['conf'], origin)
            return json.jsonify(id=origin_id)  # FIXME 201

    except:
        return make_response('Bad request!', 400)


@app.route('/vcs/<uri_type>/', methods=['PUT'])
def put_all(uri_type):
    """Store or update given objects (uri_type in {contents, directories, revisions, releases).
    """
    if request.headers.get('Content-Type') != 'application/json':
        return make_response('Bad request. Expected json data!', 400)

    payload = request.json
    obj_type = _uri_types[uri_type]

    for obj in payload:  # iterate over objects of type uri_type
        obj_to_store = _build_object_fn[obj_type](obj['sha1'], obj)

        obj_found = store.find(app.config['conf'], obj_to_store)
        if not obj_found:
            store.add(app.config['conf'], obj_to_store)

    return make_response('Successful creation!', 204)


@app.route('/vcs/<uri_type>/<sha1hex>')
def object_exists_p(uri_type, sha1hex):
    """Assert if the given object type exists.
    """
    return _do_action(lookup, uri_type, sha1hex)


@app.route('/vcs/<uri_type>/<sha1hex>', methods=['PUT'])
def put_object(uri_type, sha1hex):
    """Put an object in storage.
    """
    return _do_action(add_object, uri_type, sha1hex)


def run(conf):
    """Run the api's server.
    conf is a dictionary of keywords:
    - 'db_url' the db url's access (through psycopg2 format)
    - 'content_storage_dir' revisions/directories/contents storage on disk
    - 'port'   to override the default of 5000 (from the underlying layer:
    flask)
    - 'debug'  activate the verbose logs
    """
    app.config['conf'] = conf  # app.config is the app's state (accessible)

    app.run(port=conf.get('port', None), debug=conf['debug'] == 'true')
