#!/usr/bin/env python3
# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging

from flask import Flask, Response, make_response, request

from swh.store import store, db, service
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


# from uri to type
_uri_types = {'revisions': store.Type.revision,
              'directories': store.Type.directory,
              'contents': store.Type.content,
              'releases': store.Type.release,
              'occurrences': store.Type.occurrence}


def _do_action_with_payload(conf, action_fn, uri_type, id, map_result_fn):
    uri_type_ok = _uri_types.get(uri_type, None)
    if uri_type_ok is None:
        return make_response('Bad request!', 400)

    vcs_object = read_request_payload(request)
    vcs_object.update({'id': id,
                       'type': uri_type_ok})
    return action_fn(conf, vcs_object, map_result_fn)


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

@app.route('/vcs/persons/', methods=['POST'])
def post_person():
    """Find a person.
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    origin = read_request_payload(request)
    config = app.config['conf']

    with db.connect(config['db_url']) as db_conn:
        try:
            person_found = service.find_person(db_conn, origin)
            if person_found:
                return write_response(person_found)
            else:
                return make_response('Person not found!', 404)
        except:
            return make_response('Bad request!', 400)


@app.route('/origins/', methods=['POST'])
def post_origin():
    """Find an origin.
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
            return write_response(origin_found)  # FIXME: 204
        except:
            return make_response('Bad request!', 400)


@app.route('/vcs/persons/', methods=['PUT'])
def put_all_persons():
    """Store or update given revisions.
    FIXME: Refactor same behavior with `put_all`.
    """
    if request.headers.get('Content-Type') != serial.MIMETYPE:
        return make_response('Bad request. Expected %s data!' % serial.MIMETYPE, 400)

    payload = read_request_payload(request)
    obj_type = store.Type.person

    config = app.config['conf']

    with db.connect(config['db_url']) as db_conn:
        service.add_persons(db_conn, config, obj_type, payload)
    return make_response('Successful creation!', 204)


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


def add_object(config, vcs_object, map_result_fn):
    """Add object in storage.
    - config is the configuration needed for the backend to execute query
    - vcs_object is the object to look for in the backend
    - map_result_fn is a mapping function which takes the backend's result
    and transform its output accordingly.

    This function returns an http response of the result.
    """
    type = vcs_object['type']
    id = vcs_object['id']
    logging.debug('store %s %s' % (type, id))

    with db.connect(config['db_url']) as db_conn:
        res = service.add_objects(db_conn, config, type, [vcs_object])
        return make_response(map_result_fn(id, res), 204)


def _do_lookup(conf, uri_type, id, map_result_fn):
    """Looking up type object with sha1.
    - config is the configuration needed for the backend to execute query
    - vcs_object is the object to look for in the backend
    - map_result_fn is a mapping function which takes the backend's result
    and transform its output accordingly.

    This function returns an http response of the result.
    """
    uri_type_ok = _uri_types.get(uri_type, None)
    if not uri_type_ok:
        return make_response('Bad request!', 400)

    with db.connect(conf['db_url']) as db_conn:
        res = store.find(db_conn, id, uri_type_ok)
        if res:
            return write_response(map_result_fn(id, res))  # 200
        return make_response('Not found!', 404)


@app.route('/vcs/occurrences/<id>')
def list_occurrences_for(id):
    """Return the occurrences pointing to the revision id.
    """
    return _do_lookup(app.config['conf'],
                      'occurrences',
                      id,
                      lambda _, result: list(map(lambda col: col[1], result)))


@app.route('/vcs/<uri_type>/<id>')
def object_exists_p(uri_type, id):
    """Assert if the object with sha1 id, of type uri_type, exists.
    """
    return _do_lookup(app.config['conf'],
                      uri_type,
                      id,
                      lambda sha1, _: {'id': sha1})


@app.route('/vcs/<uri_type>/<id>', methods=['PUT'])
def put_object(uri_type, id):
    """Put an object in storage.
    """
    return _do_action_with_payload(app.config['conf'],
                                   add_object,
                                   uri_type,
                                   id,
                                   lambda sha1, _2: sha1)  # FIXME: use id or result instead


def run(conf):
    """Run the api's server.
    conf is a dictionary of keywords:
    - 'db_url' the db url's access (through psycopg2 format)
    - 'content_storage_dir' revisions/directories/contents storage on disk
    - 'host'   to override the default 127.0.0.1 to open or not the server to
    the world
    - 'port'   to override the default of 5000 (from the underlying layer:
    flask)
    - 'debug'  activate the verbose logs
    """
    print("""SWH Api run
host: %s
port: %s
debug: %s""" % (conf['host'], conf.get('port', None), conf['debug']))

    # app.config is the app's state (accessible)
    app.config.update({'conf': conf})

    app.run(host=conf['host'],
            port=conf.get('port', None),
            debug=conf['debug'] == 'true')
