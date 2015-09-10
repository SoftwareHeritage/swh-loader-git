# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.loader.git.storage import storage, db, service
from swh.loader.git.conf import reader
from swh.storage.objstorage import ObjStorage


# FIXME: duplicated from bin/swh-backend...
# Default configuration file
DEFAULT_CONF_FILE = '~/.config/swh/back.ini'


# default configuration
DEFAULT_CONF = {
    'content_storage_dir': ('string', '/tmp/swh-loader-git/content-storage'),
    'log_dir': ('string', '/tmp/swh-loader-git/log'),
    'db_url': ('string', 'dbname=softwareheritage-dev'),
    'folder_depth': ('int', 4),
    'debug': ('bool', None),
    'host': ('string', '127.0.0.1'),
    'port': ('int', 5000)
}


def store_only_new(db_conn, conf, obj_type, obj):
    """Store object if not already present.

    """
    if not storage.find(db_conn, obj['id'], obj_type):
        storage.add(db_conn, conf, obj)


_obj_to_persist_fn = {storage.Type.revision: service.add_revisions}


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


def load_to_back(conf, swh_repo):
    """Load to the backend the repository swh_repo.

    """
    with db.connect(conf['db_url']) as db_conn:
        # First, store/retrieve the origin identifier
        # FIXME: should be done by the cloner worker (which is not yet plugged
        # on the right swh db ftm)
        service.add_origin(db_conn, swh_repo.get_origin())

        # First reference all unknown persons
        service.add_persons(db_conn, conf, storage.Type.person,
                            swh_repo.get_persons())

        res = store_unknown_objects(db_conn, conf,
                                    storage.Type.content,
                                    swh_repo.get_contents())
        if res:
            res = store_unknown_objects(db_conn, conf,
                                        storage.Type.directory,
                                        swh_repo.get_directories())
            if res:
                res = store_unknown_objects(db_conn, conf,
                                            storage.Type.revision,
                                            swh_repo.get_revisions())
                if res:
                    # brutally send all remaining occurrences
                    service.add_objects(db_conn, conf,
                                        storage.Type.occurrence,
                                        swh_repo.get_occurrences())

                    # and releases (the idea here is that compared to existing
                    # objects, the quantity is less)
                    service.add_objects(db_conn, conf,
                                        storage.Type.release,
                                        swh_repo.get_releases())


def prepare_and_load_to_back(backend_setup_file, swh_repo):
    """Prepare and load to back the swh_repo.
    backend-setup-file is the backend's setup to load to access the db and file
    storage.

    """
    # Read the configuration file (no check yet)
    conf = reader.read(backend_setup_file or DEFAULT_CONF_FILE, DEFAULT_CONF)
    reader.prepare_folders(conf, 'content_storage_dir')
    conf.update({
        'objstorage': ObjStorage(conf['content_storage_dir'],
                                 conf['folder_depth'])
        })

    load_to_back(conf, swh_repo)
