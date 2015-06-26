# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum

from swh.storage import db


class Type(Enum):
    """Types of git objects.
    """
    commit = 'commit'
    tree = 'tree'
    blob = 'blob'
    tag = 'tag'


def cleandb(db_conn):
    """Clean the database.
    """
    db.queries_execute(db_conn, ['DROP TABLE IF EXISTS files;',
                                 'DROP TABLE IF EXISTS git_objects;',
                                 'DROP TYPE IF EXISTS type;'])


def initdb(db_conn):
    """Initialize the database.
    """
    db.queries_execute(db_conn, [
        ("""CREATE TYPE type
            AS ENUM(%s, %s, %s, %s);""",
            (Type.commit.value,
             Type.tree.value,
             Type.blob.value,
             Type.tag.value)),
        """CREATE TABLE IF NOT EXISTS files
              (id bigserial PRIMARY KEY,
              ctime timestamp DEFAULT current_timestamp,
              sha1 char(40) UNIQUE,
              size integer CONSTRAINT no_null not null,
              sha1_git char(40) CONSTRAINT no_null not null,
              UNIQUE(sha1, size));""",
        """CREATE TABLE IF NOT EXISTS git_objects
               (id bigserial PRIMARY KEY,
               ctime timestamp DEFAULT current_timestamp,
               sha1 char(40),
               type type CONSTRAINT no_null not null,
               stored bool DEFAULT false);"""])


def add_blob(db_conn, obj_sha, size, obj_git_sha):
    """Insert a new file
    """
    db.query_execute(db_conn, ("""INSERT INTO files (sha1, size, sha1_git)
                                  VALUES (%s, %s, %s);""",
                               (obj_sha, size, obj_git_sha)))


def add_object(db_conn, obj_sha, obj_type):
    """Insert a new object
    """
    db.query_execute(db_conn, ("""INSERT INTO git_objects (sha1, type)
                                  VALUES (%s, %s);""",
                               (obj_sha, obj_type.value)))


def find_blob(db_conn, obj_sha, obj_type=None):
    """Find a file by its hash.
obj_type is not used (implementation detail).
    """
    return db.query_fetchone(db_conn, ("""SELECT sha1 FROM files
                                          WHERE sha1=%s;""",
                                       (obj_sha,)))


def find_object(db_conn, obj_sha, obj_type):
    """Find an object by its hash.
    """
    return db.query_fetchone(db_conn, ("""SELECT sha1 FROM git_objects
                                          WHERE sha1=%s
                                          AND type=%s;""",
                                       (obj_sha, obj_type.value)))


def find_unknowns(db_conn, file_sha1s):
    """Given a sha1s map (lazy), returns the objects list of sha1 non-presents in
    models.
    """
    db.queries_execute(db_conn,
                       ("DROP TABLE IF EXISTS TMP_FILTER_SHA1;",
                        "CREATE TABLE TMP_FILTER_SHA1(sha1 char(40));"))
    db.copy_from(db_conn, file_sha1s, 'TMP_FILTER_SHA1')
    return db.query_fetch(db_conn, ("""WITH sha1_union as (
                                         SELECT sha1 FROM git_objects
                                         UNION
                                         SELECT sha1 FROM files
                                      )
                                      (SELECT sha1 FROM tmp_filter_sha1)
                                      EXCEPT
                                      (SELECT sha1 FROM sha1_union);""",
    ), trace=True)


def count_files(db_conn):
    """Count the number of blobs.
    """
    row = db.query_fetchone(db_conn, "SELECT count(*) FROM files;")
    return row[0]


def count_objects(db_conn, obj_type):
    """Count the number of objects with obj_type.
    """
    row = db.query_fetchone(db_conn, ("""SELECT count(*) FROM git_objects
                                         WHERE type=%s;""",
                                      (obj_type.value,)))
    return row[0]
