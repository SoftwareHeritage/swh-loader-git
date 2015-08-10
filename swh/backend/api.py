#!/usr/bin/env python3
# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from flask import Flask, Response, make_response, request

from . import mapping
from swh.storage import store, db
from swh.protocols import serial

# api's definition
app = Flask(__name__)


def read_request_payload(request):
    """Read the request's payload."""  # FIXME: Check the signed pickled data?
    payload = serial.load(request.stream)
    print("payload: ", payload)
    return payload


def write_response(data):
    """Write response from data."""
    return Response(serial.dumps(data), mimetype=serial.MIMETYPE)


@app.route('/')
def hello():
    """A simple api to define what the server is all about.
    FIXME: A redirect towards a static page defining the routes would be nice.
    """
    return 'Dev SWH API'


@app.route('/pickle', methods=['POST'])
def pickle_tryout():
    """Test pickle.
    """
    unpickled = read_request_payload(request)
    print("unpickled data: %s" % unpickled)
    return make_response('Received!', 200)


# dispatch on build object function for the right type
_build_object_fn = {store.Type.revision: mapping.build_revision,
                    store.Type.directory: mapping.build_directory,
                    store.Type.content: mapping.build_content,
                    store.Type.release: mapping.build_release,
                    store.Type.occurrence: mapping.build_occurrence}

# from uri to type
_uri_types = {'revisions': store.Type.revision,
              'directories': store.Type.directory,
              'contents': store.Type.content,
              'releases': store.Type.release,
              'occurrences': store.Type.occurrence}


def _do_action(action_fn, uri_type, sha1hex, map_result_fn):
    uri_type_ok = _uri_types.get(uri_type, None)
    if not uri_type_ok:
        return make_response('Bad request!', 400)

    vcs_object = _build_object_fn[uri_type_ok](sha1hex, None)
    return action_fn(app.config['conf'], vcs_object, map_result_fn)


def _do_action_with_payload(action_fn, uri_type, sha1hex, map_result_fn):
    uri_type_ok = _uri_types.get(uri_type, None)
    if uri_type_ok is None:
        return make_response('Bad request!', 400)

    payload = read_request_payload(request)
    vcs_object = _build_object_fn[uri_type_ok](sha1hex, payload)
    return action_fn(app.config['conf'], vcs_object, map_result_fn)


# FIXME: improve payload to have multiple type checksums list
# and return symmetrically the result with filtered checksums per type
@app.route('/objects/', methods=['POST'])
def filter_unknowns_objects():
    """Filters unknown sha1 to the backend and returns them.
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    sha1s = read_request_payload(request)
    unknowns_sha1s = store.find_unknowns(app.config['conf'], None, sha1s)
    if unknowns_sha1s is None:
        return make_response('Bad request!', 400)
    else:
        return write_response(unknowns_sha1s)


# occurrence type is not dealt the same way
_post_all_uri_types = {'revisions': store.Type.revision,
                       'directories': store.Type.directory,
                       'contents': store.Type.content,
                       'releases': store.Type.release}


@app.route('/vcs/<uri_type>/', methods=['POST'])
def filter_unknowns_type(uri_type):
    """Filters unknown sha1 to the backend and returns them.
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    obj_type = _post_all_uri_types.get(uri_type)
    if obj_type is None:
        return make_response('Bad request. Type not supported!', 400)

    sha1s = read_request_payload(request)
    unknowns_sha1s = store.find_unknowns(app.config['conf'], obj_type, sha1s)
    if unknowns_sha1s is None:
        return make_response('Bad request!', 400)
    else:
        return write_response(unknowns_sha1s)


@app.route('/origins/', methods=['POST'])
def post_origin():
    """Post an origin.
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    origin = read_request_payload(request)

    try:
        origin_found = store.find_origin(app.config['conf'], origin)
        if origin_found:
            return write_response({'id': origin_found[0]})
        else:
            return make_response('Origin not found!', 404)
    except:
        return make_response('Bad request!', 400)


@app.route('/origins/', methods=['PUT'])
def put_origin():
    """Create an origin or returns it if already existing.
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    origin = read_request_payload(request)
    config = app.config['conf']

    try:
        origin_found = store.find_origin(config, origin)
        if origin_found:
            return write_response({'id': origin_found[0]})  # FIXME 204
        else:
            origin_id = store.add_origin(config, origin)
            return write_response({'id': origin_id})  # FIXME 201

    except:
        return make_response('Bad request!', 400)


@app.route('/vcs/revisions/', methods=['PUT'])
def put_all_revisions():
    """Store or update given revisions.
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    payload = read_request_payload(request)
    obj_type = store.Type.revision

    config = app.config['conf']

    with db.connect(config['db_url']) as db_conn:
        try:
            couple_parents = []
            for obj in payload:  # iterate over objects of type uri_type
                obj_to_store = _build_object_fn[obj_type](obj['sha1'], obj)

                obj_found = store.find(db_conn, obj_to_store)
                if not obj_found:
                    store.add(db_conn, config, obj_to_store)

                    # deal with revision history
                    parent_shas = obj_to_store.get('parent-sha1s', None)
                    if parent_shas:
                        couple_parents.extend([(obj_to_store['sha1'], p) for p in parent_shas])

            store.add_revision_history(db_conn, couple_parents)
        except:  # all kinds of error break the transaction
            db_conn.rollback()
            return make_response('Failure', 500)

    return make_response('Successful creation!', 204)


@app.route('/vcs/<uri_type>/', methods=['PUT'])
def put_all(uri_type):
    """Store or update given objects (uri_type in {contents, directories, releases).
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    payload = read_request_payload(request)
    obj_type = _uri_types[uri_type]

    config = app.config['conf']

    with db.connect(config['db_url']) as db_conn:
        try:
            for obj in payload:  # iterate over objects of type uri_type
                obj_to_store = _build_object_fn[obj_type](obj['sha1'], obj)

                obj_found = store.find(db_conn, obj_to_store)
                if not obj_found:
                    store.add(db_conn, config, obj_to_store)
        except:  # all kinds of error break the transaction
            db_conn.rollback()
            return make_response('Failure', 500)

    return make_response('Successful creation!', 204)

def lookup(config, vcs_object, map_result_fn):
    """Looking up type object with sha1.
    - config is the configuration needed for the backend to execute query
    - vcs_object is the object to look for in the backend
    - map_result_fn is a mapping function which takes the backend's result
    and transform its output accordingly.

    This function returns an http response of the result.
    """
    sha1hex = vcs_object['sha1']
    logging.debug('read %s %s' % (vcs_object['type'], sha1hex))

    with db.connect(config['db_url']) as db_conn:
        res = store.find(db_conn, vcs_object)
        if res:
            return write_response(map_result_fn(sha1hex, res))  # 200
        return make_response('Not found!', 404)


def add_object(config, vcs_object, map_result_fn):
    """Add object in storage.
    - config is the configuration needed for the backend to execute query
    - vcs_object is the object to look for in the backend
    - map_result_fn is a mapping function which takes the backend's result
    and transform its output accordingly.

    This function returns an http response of the result.
    """
    type = vcs_object['type']
    sha1hex = vcs_object['sha1']  # FIXME: remove useless key and send direct list
    logging.debug('store %s %s' % (type, sha1hex))

    with db.connect(config['db_url']) as db_conn:
        if store.find(db_conn, vcs_object):
            logging.debug('update %s %s' % (sha1hex, type))
            return make_response('Successful update!', 200)  # immutable
        else:
            logging.debug('store %s %s' % (sha1hex, type))
            res = store.add(db_conn, config, vcs_object)
            if res is None:
                return make_response('Bad request!', 400)
            elif res is False:
                logging.error('store %s %s' % (sha1hex, type))
                return make_response('Internal server error!', 500)
            else:
                return make_response(map_result_fn(sha1hex, res), 204)


@app.route('/vcs/occurrences/<sha1hex>')
def list_occurrences_for(sha1hex):
    """Return the occurrences pointing to the revision sha1hex.
    """
    return _do_action(lookup,
                      'occurrences',
                      sha1hex,
                      lambda _, result: list(map(lambda col: col[1], result)))


def result_map_id_with_sha1(sha1hex, result):
    """A default mapping function to map the backend's result.
    """
    # FIXME: could do something more complicated given the result
    return {'id': sha1hex}


@app.route('/vcs/<uri_type>/<sha1hex>')
def object_exists_p(uri_type, sha1hex):
    """Assert if the object with sha1 sha1hex, of type uri_type, exists.
    """
    return _do_action(lookup, uri_type, sha1hex, result_map_id_with_sha1)


@app.route('/vcs/<uri_type>/<sha1hex>', methods=['PUT'])
def put_object(uri_type, sha1hex):
    """Put an object in storage.
    """
    return _do_action_with_payload(add_object,
                                   uri_type,
                                   sha1hex,
                                   lambda _1, _2: 'Successful Creation!')  # FIXME use sha1hex or result instead


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
