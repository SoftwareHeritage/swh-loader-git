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
    # # git specific
    # commit = 'commit' # useless
    # tree = 'tree'     # useless
    # blob = 'blob'     # useless
    # tag = 'tag'       # useless
    # abstract type
    occurence = 'occurence'             # ~git branch
    release = 'release'                 # ~git annotated tag
    revision = 'revision'               # ~git commit
    directory = 'directory'             # ~git tree
    directory_entry = 'directory_entry' # ~git tree_entry
    content = 'content'                 # ~git blob

def initdb(db_conn):
    """For retrocompatibility.
    """
    pass


def cleandb(db_conn):
    db.queries_execute(db_conn, ['TRUNCATE TABLE release CASCADE',
                                 'TRUNCATE TABLE revision CASCADE',
                                 'TRUNCATE TABLE directory CASCADE',
                                 'TRUNCATE TABLE content CASCADE'])


def add_content(db_conn, sha1, sha1_content, sha256_content, size):
    """Insert a new content.
    """
    db.query_execute(db_conn,
                     ("""INSERT INTO content (id, sha1, sha256, length)
                         VALUES (%s, %s, %s, %s)""",
                      (sha1, sha1_content, sha256_content, size)))


def add_directory(db_conn, obj_sha):
    """Insert a new directory.
    """
    db.query_execute(db_conn,
                     ("""INSERT INTO directory (id)
                         VALUES (%s)""",
                      (obj_sha,)))


def add_directory_entry(db_conn, name, sha, type, perms,
                        atime, mtime, ctime, parent):
    """Insert a new directory.
    """
    db.query_execute(db_conn,
                     ("""INSERT INTO directory_entry
                         (name, id, type, perms, atime, mtime, ctime,
                         directory)
                         VALUES (%s, %s, %s, %s, %s, %s, %s,
                                 %s)""",
                      (name, sha, type, perms, atime, mtime, ctime,
                       parent)))


def add_revision(db_conn, sha, date, directory, message, author, committer,
                 parent_sha=None):
    """Insert a new revision.
    """
    with db_conn.cursor() as cur:
        db.execute(cur,
                   ("""INSERT INTO revision
                       (id, date, directory, message, author, committer)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (sha, date, directory, message, author, committer)))

        if parent_sha:  # initial commit has no parent
            db.execute(cur,
                   ("""INSERT INTO revision_history
                       (id, parent_id)
                       VALUES (%s, %s)""",
                    (sha, parent_sha)))


def add_release(db_conn, obj_sha, revision, date, name, comment):
    """Insert a release.
    """
    db.query_execute(db_conn,
                     ("""INSERT INTO release (id, revision, date, name, comment)
                         VALUES (%s, %s, %s, %s, %s)""",
                      (obj_sha, revision, date, name, comment)))


def add_occurence(db_conn, reference, revision, date):
    """Insert an occurence.
       Check if occurence history already present.
       If present do nothing, otherwise insert
    """
    with db_conn.cursor() as cur:
        occ = find_occurence(cur, reference, revision, date)
        if occ is None:
            db.execute(cur,
                       ("""INSERT INTO occurence_history
                           (reference, revision, validity)
                           VALUES (%s, %s, %s)""",
                        (reference, revision, date)))


def find_revision(db_conn, obj_sha):
    """Find a revision by its obj_sha.
    """
    return find_object(db_conn, obj_sha, Type.revision)


def find_directory(db_conn, obj_sha):
    """Find a directory by its obj_sha.
    """
    return find_object(db_conn, obj_sha, Type.directory)


def find_content(db_conn, obj_sha):
    """Find a content by its obj_sha.
    """
    return find_object(db_conn, obj_sha, Type.content)


def find_occurence(cur, reference, revision, date):
    """Find an ocurrence with reference reference pointing on revision revision
    still valid for date.
    """
    return db.fetchone(cur, (""" SELECT id FROM occurence_history
                                 WHERE reference=%
                                 AND revision=%s
                                 AND validity >= %s""",
                             (reference, revision, date)))


def find_object(db_conn, obj_sha, obj_type):
    """Find an object of obj_type by its obj_sha.
    """
    table = obj_type if isinstance(obj_type, str) else obj_type.value
    query = 'select id from ' + table + ' where id=%s'
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


def count_occurence(db_conn):
    """Count the number of occurence.
    """
    return _count_objects(db_conn, Type.occurence)


def count_release(db_conn):
    """Count the number of occurence.
    """
    return _count_objects(db_conn, Type.release)
