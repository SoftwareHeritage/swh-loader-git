# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from swh.storage import store, db
from swh.conf import reader


# FIXME duplicated from bin/swh-backend...
# Default configuration file
DEFAULT_CONF_FILE = '~/.config/swh/back.ini'


# default configuration
DEFAULT_CONF = {
    'content_storage_dir' : ('string', '/tmp/swh-git-loader/content-storage'),
    'log_dir'             : ('string', '/tmp/swh-git-loader/log'),
    'db_url'              : ('string', 'dbname=softwareheritage-dev'),
    'storage_compression' : ('bool'  , None),
    'folder_depth'        : ('int'   , 4),
    'debug'               : ('bool'  , None),
    'port'                : ('int'   , 5000)
}

def store_new(db_conn, conf, obj_type, obj):
    """Store object if not already present.
    """
    print("obj %s: %s" % (obj_type, obj))
    obj.update({'type': obj_type})
    if not store.find(db_conn, obj):
        store.add(db_conn, conf, obj)


def store_objects(db_conn, conf, obj_type, swhmap):
    """Load objects to the backend.
    """
    sha1s = swhmap.keys()

    # have: filter unknown obj
    unknown_obj_sha1s = store.find_unknowns(db_conn, obj_type, sha1s)
    if not unknown_obj_sha1s:
        return True

    # seen: now create the data for the backend to store
    obj_map = swhmap.objects()

    for sha1 in unknown_obj_sha1s:
        store_new(db_conn, conf, obj_type, obj_map[sha1])


def load_to_back(backend_setup_file, swhrepo):
    """Load to the backend the repository swhrepo.
    """
    # Read the configuration file (no check yet)
    conf = reader.read(backend_setup_file or DEFAULT_CONF_FILE, DEFAULT_CONF)

    with db.connect(conf['db_url']) as db_conn:
        # First, store/retrieve the origin identifier
        # FIXME: should be done by the cloner worker (which is not yet plugged on
        # the right swh db ftm)
        origin = swhrepo.get_origin()
        if not store.find_origin(db_conn, origin):
            store.add_origin(db_conn, origin)

        res = store_objects(db_conn, conf, store.Type.content, swhrepo.get_contents())
        if res:
            res = store_objects(db_conn, conf, store.Type.directory,
                                swhrepo.get_directories())
            if res:
                res = store_objects(db_conn, conf, store.Type.revision,
                                    swhrepo.get_revisions())
                if res:
                    # brutally send all remaining occurrences
                    for occurrence in swhrepo.get_occurrences():
                        occurrence.update({'revision': occurrence['sha1']})
                        store_new(db_conn, conf, store.Type.occurrence, occurrence)

                    # and releases (the idea here is that compared to existing other
                    # objects, the quantity is less)
                    for release in swhrepo.get_releases():
                        store_new(db_conn, conf, store.Type.release, release)
