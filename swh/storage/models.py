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
    # git specific
    commit = 'commit' # useless
    tree = 'tree'     # useless
    blob = 'blob'     # useless
    tag = 'tag'       # useless
    # abstract type
    revision = 'revision'
    directory = 'directory'
    content = 'content'
    release = 'release'


def add_content(db_conn, obj_sha1, obj_sha1_content, obj_sha256_content, size):
    """Insert a new content.
    """
    db.query_execute(db_conn,
                     ("""INSERT INTO content (id, sha1, sha256, length)
                         VALUES (%s, %s, %s, %s)""",
                      (obj_sha1, obj_sha1_content, obj_sha256_content, size)))


def add_directory(db_conn, obj_sha, name, id, type, perms,
                  atime, mtime, ctime, directory):
    """Insert a new directory.
    """
    with db_conn.cursor() as cur:
        db.query_execute(cur,
                         ("""INSERT INTO directory (id)
                             VALUES (%s)""",
                          (obj_sha,)))

        db.query_execute(cur,
                         ("""INSERT INTO directory_entry
                             (name, id, type, perms, atime, mtime, ctime,
                              directory)
                             VALUES (%s, %s, %s, %s, %s, %s, %s,
                              %s)""",
                          (name, id, type, perms, atime, mtime, ctime,
                           directory)))


def add_revision(db_conn, obj_sha, date, directory, message, author, committer,
                 parent_sha):
    """Insert a new revision.
    """
    with db_conn.cursor() as cur:
        db.execute(cur,
                   ("""INSERT INTO revision
                       (id, date, directory, message, author, committer)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (obj_sha, date, directory, message, author, committer)))

        db.execute(cur,
                   ("""INSERT INTO revision_history
                       (id, parent_id)
                       VALUES (%s, %s)""",
                    (obj_sha, parent_sha)))


def find_revision(db_conn, obj_sha):
    """Find a revision by its obj_sha.
    """
    return _find_object(db_conn, obj_sha, Type.revision)


def find_directory(db_conn, obj_sha):
    """Find a directory by its obj_sha.
    """
    return _find_object(db_conn, obj_sha, Type.directory)


def find_content(db_conn, obj_sha):
    """Find a content by its obj_sha.
    """
    return _find_object(db_conn, obj_sha, Type.content)


def _find_object(db_conn, obj_sha, obj_type):
    """Find an object of obj_type by its obj_sha.
    """
    query = 'select id from ' + obj_type.value + ' where sha1=%s'
    return db.query_fetchone(db_conn, (query, (obj_sha,)))


def find_unknowns(db_conn, file_sha1s):
    """Given a list of sha1s (inside the file_sha1s reference),
    returns the objects list of sha1 non-presents in db.
    """
    with db_conn.cursor() as cur:
        # explicit is better than implicit
        # simply creating the temporary table seems to be enough
        # (no drop, nor truncate) but this is not explained in documentation
        db.execute(cur, """CREATE TEMPORARY TABLE IF NOT EXISTS filter_sha1(
                             id git_object_id)
                           ON COMMIT DELETE ROWS;""")
        db.copy_from(cur, file_sha1s, 'filter_sha1')
        db.execute(cur, ("""WITH sha1_union as (
                                 SELECT id FROM revision
                                 UNION
                                 SELECT id FROM directory
                                 UNION
                                 SELECT id FROM content
                              )
                            (SELECT id FROM filter_sha1)
                            EXCEPT
                            (SELECT id FROM sha1_union);"""))
        return cur.fetchall()


def _count_objects(db_conn, type):
    return db.query_fetchone(db_conn, 'SELECT count(*) FROM ' + type.value)[0]


def count_revisions(db_conn):
    """Count the number of revisions.
    """
    return _count_objects(db_conn, Type.revision)


def count_directories(db_conn, obj_type):
    """Count the number of directories.
    """
    return _count_objects(db_conn, Type.directory)


def count_contents(db_conn):
    """Count the number of contents.
    """
    return _count_objects(db_conn, Type.content)
