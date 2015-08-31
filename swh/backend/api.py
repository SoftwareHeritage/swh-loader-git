#!/usr/bin/env python3
# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from flask import Flask, Response, make_response, request

from swh.storage import store, db, service
from swh.protocols import serial

# api's definition
app = Flask(__name__)


def read_request_payload(request):
    """Read the request's payload.
    """  # TODO: Check the signed pickled data?
    return serial.load(request.stream)


def write_response(data):
    """Write response from data.
    """
    return Response(serial.dumps(data), mimetype=serial.MIMETYPE)


@app.route('/')
def hello():
    """A simple api to define what the server is all about.
    FIXME: A redirect towards a static page defining the routes would be nice.
    """
    return 'Dev SWH API'


# dispatch on build object function for the right type
_build_object_fn = service.build_object_fn

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


# occurrence type is not dealt the same way
_post_all_uri_types = {'revisions': store.Type.revision,
                       'directories': store.Type.directory,
                       'contents': store.Type.content}


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
    config = app.config['conf']

    with db.connect(config['db_url']) as db_conn:
        unknowns_sha1s = service.filter_unknowns_type(db_conn, obj_type, sha1s)
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
    config = app.config['conf']

    with db.connect(config['db_url']) as db_conn:
        try:
            origin_found = service.find_origin(db_conn, origin)
            if origin_found:
                return write_response(origin_found)
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

    with db.connect(config['db_url']) as db_conn:
        try:
            origin_found = service.add_origin(db_conn, origin)
            return write_response(origin_found)  # FIXME 204
        except:
            return make_response('Bad request!', 400)


@app.route('/vcs/revisions/', methods=['PUT'])
def put_all_revisions():
    """Store or update given revisions.
    FIXME: Refactor same behavior with `put_all`.
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    payload = read_request_payload(request)
    obj_type = store.Type.revision

    config = app.config['conf']

    with db.connect(config['db_url']) as db_conn:
        service.add_revisions(db_conn, config, obj_type, payload)
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
        service.add_objects(db_conn, config, obj_type, payload)

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


@app.route('/vcs/<uri_type>/<sha1hex>')
def object_exists_p(uri_type, sha1hex):
    """Assert if the object with sha1 sha1hex, of type uri_type, exists.
    """
    return _do_action(lookup,
                      uri_type,
                      sha1hex,
                      lambda sha1hex, _: {'id': sha1hex})


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
