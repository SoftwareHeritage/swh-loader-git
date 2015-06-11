# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum

from swh import db


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
    db.queries_execute(db_conn, ["drop table if exists files;",
                                 "drop table if exists git_objects;",
                                 "drop type if exists type;"])


def initdb(db_conn):
    """Initialize the database.
    """
    db.queries_execute(db_conn, [
        ("""CREATE TYPE type
            as ENUM(%s, %s, %s, %s);""",
            (Type.commit.value,
             Type.tree.value,
             Type.blob.value,
             Type.tag.value)),
        """create table if not exists files
              (id bigserial primary key,
              sha1 bytea unique,
              size integer constraint no_null not null,
              ctime timestamp default current_timestamp,
              sha1_git bytea constraint no_null not null,
              UNIQUE(sha1, size));""",
        """create table if not exists git_objects
               (id bigserial primary key,
               sha1 bytea,
               type type constraint no_null not null,
               ctime timestamp default current_timestamp,
               stored bool default false);"""])


def add_blob(db_conn, obj_sha, size, obj_git_sha):
    """Insert a new file
    """
    db.query_execute(db_conn, ("""insert into files (sha1, size, sha1_git)
                                  values (%s, %s, %s);""",
                               (obj_sha, size, obj_git_sha)))


def add_object(db_conn, obj_sha, obj_type):
    """Insert a new object
    """
    db.query_execute(db_conn, ("""insert into git_objects (sha1, type)
                                  values (%s, %s);""",
                               (obj_sha, obj_type.value)))


def find_blob(db_conn, obj_sha):
    """Find a file by its hash.
    """
    return db.query_fetchone(db_conn, ("""select sha1 from files
                                          where sha1=%s;""",
                                       (obj_sha,)))


def find_object(db_conn, obj_sha, obj_type):
    """Find an object by its hash.
    """
    return db.query_fetchone(db_conn, ("""select sha1 from git_objects
                                          where sha1=%s
                                          and type=%s;""",
                                       (obj_sha, obj_type.value)))


def count_files(db_conn):
    """Count the number of blobs."""
    row = db.query_fetchone(db_conn, "select count(*) from files;")
    return row[0]


def count_objects(db_conn, obj_type):
    """Count the number of objects with obj_type."""
    row = db.query_fetchone(db_conn, ("""select count(*) from git_objects
                                         where type=%s;""",
                                      (obj_type.value,)))
    return row[0]
