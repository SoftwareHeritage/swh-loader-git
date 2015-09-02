# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage import store, db, service
from swh.conf import reader


# FIXME: duplicated from bin/swh-backend...
# Default configuration file
DEFAULT_CONF_FILE = '~/.config/swh/back.ini'


# default configuration
DEFAULT_CONF = {
    'content_storage_dir': ('string', '/tmp/swh-git-loader/content-storage'),
    'log_dir': ('string', '/tmp/swh-git-loader/log'),
    'db_url': ('string', 'dbname=softwareheritage-dev'),
    'storage_compression': ('bool', None),
    'folder_depth': ('int', 4),
    'debug': ('bool', None),
    'port': ('int', 5000)
}


def store_only_new(db_conn, conf, obj_type, obj):
    """Store object if not already present.
    """
    obj.update({'type': obj_type})
    if not store.find(db_conn, obj):
        store.add(db_conn, conf, obj)


_obj_to_persist_fn = {store.Type.revision: service.add_revisions}


def store_unknown_objects(db_conn, conf, obj_type, swhmap):
    """Load objects to the backend.
    """
    sha1s = swhmap.keys()

    # have: filter unknown obj
    unknown_obj_sha1s = service.filter_unknowns_type(db_conn, obj_type, sha1s)
    if not unknown_obj_sha1s:
        return True

    # seen: now store in backend
    persist_fn = _obj_to_persist_fn.get(obj_type, service.add_objects)
    obj_fulls = map(swhmap.get, unknown_obj_sha1s)
    return persist_fn(db_conn, conf, obj_type, obj_fulls)


def load_to_back(backend_setup_file, swhrepo):
    """Load to the backend the repository swhrepo.
    """
    # Read the configuration file (no check yet)
    conf = reader.read(backend_setup_file or DEFAULT_CONF_FILE, DEFAULT_CONF)

    with db.connect(conf['db_url']) as db_conn:
        # First, store/retrieve the origin identifier
        # FIXME: should be done by the cloner worker (which is not yet plugged
        # on the right swh db ftm)
        service.add_origin(db_conn, swhrepo.get_origin())

        # First reference all unknown persons
        service.add_persons(db_conn, conf, store.Type.person,
                            swhrepo.get_persons())

        res = store_unknown_objects(db_conn, conf, store.Type.content,
                                    swhrepo.get_contents())
        if res:
            res = store_unknown_objects(db_conn, conf, store.Type.directory,
                                        swhrepo.get_directories())
            if res:
                res = store_unknown_objects(db_conn, conf, store.Type.revision,
                                            swhrepo.get_revisions())
                if res:
                    # brutally send all remaining occurrences
                    service.add_objects(db_conn, conf, store.Type.occurrence,
                                        swhrepo.get_occurrences())

                    # and releases (the idea here is that compared to existing
                    # objects, the quantity is less)
                    service.add_objects(db_conn, conf, store.Type.release,
                                        swhrepo.get_releases())
