# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from enum import Enum

from . import db


class Type(Enum):
    """Types of git objects.
    """
    occurrence = 'occurrence'            # ~git branch
    release = 'release'                  # ~git annotated tag
    revision = 'revision'                # ~git commit
    directory = 'directory'              # ~git tree
    directory_entry = 'directory_entry'  # ~git tree_entry
    content = 'content'                  # ~git blob
    origin = 'origin'
    person = 'person'                    # committer, tagger, author


def initdb(db_conn):
    """For retrocompatibility.
    """
    pass


def cleandb(db_conn):
    db.queries_execute(db_conn, ['TRUNCATE TABLE content CASCADE;',
                                 'TRUNCATE TABLE organization CASCADE;',
                                 'TRUNCATE TABLE list_history CASCADE;',
                                 'TRUNCATE TABLE origin CASCADE;',
                                 'TRUNCATE TABLE fetch_history CASCADE;',
                                 'TRUNCATE TABLE project CASCADE;',
                                 'TRUNCATE TABLE project_history CASCADE;',
                                 'TRUNCATE TABLE directory CASCADE;',
                                 'TRUNCATE TABLE directory_entry_dir CASCADE;',
                                 'TRUNCATE TABLE directory_list_dir CASCADE;',
                                 'TRUNCATE TABLE directory_entry_file CASCADE;',
                                 'TRUNCATE TABLE directory_list_file CASCADE;',
                                 'TRUNCATE TABLE person CASCADE;',
                                 'TRUNCATE TABLE revision CASCADE;',
                                 'TRUNCATE TABLE revision_history CASCADE;',
                                 'TRUNCATE TABLE occurrence_history CASCADE;',
                                 'TRUNCATE TABLE occurrence CASCADE;',
                                 'TRUNCATE TABLE release CASCADE;',
                                 ])


def add_origin(db_conn, url, type, parent=None):
    """Insert origin and returns the newly inserted id.
    """
    return db.insert(db_conn,
                     ("""INSERT INTO origin (type, url, parent_id)
                         VALUES (%s, %s, %s)
                         RETURNING id""",
                      (type, url, parent)))

def add_person(db_conn, name, email):
    """Insert author and returns the newly inserted id.
    """
    return db.insert(db_conn,
                     ("""INSERT INTO person (name, email)
                         VALUES (%s, %s)
                         RETURNING id""",
                      (name, email)))


def add_content(db_conn, sha1, sha1_git, sha256_content, size):
    """Insert a new content.
    """
    db.query_execute(db_conn,
                     ("""INSERT INTO content (sha1, sha1_git, sha256, length)
                         VALUES (%s, %s, %s, %s)""",
                      (sha1, sha1_git, sha256_content, size)))


def add_directory(db_conn, obj_sha):
    """Insert a new directory.
    """
    return db.insert(db_conn,
                     ("""INSERT INTO directory (id)
                         VALUES (%s)
                         RETURNING id""",
                      (obj_sha,)))


def add_directory_entry_dir(db_conn, name, sha, perms,
                            atime, mtime, ctime, parent_id):
    """Insert a new directory entry dir.
    """
    dir_entry_id = db.insert(db_conn,
                             ("""INSERT INTO directory_entry_dir
                                 (name, target, perms, atime, mtime, ctime)
                                 VALUES (%s, %s, %s, %s, %s, %s)
                                 RETURNING id""",
                              (name, sha, perms, atime, mtime, ctime)))
    db.query_execute(db_conn,
                     ("""INSERT INTO directory_list_dir
                         (dir_id, entry_id)
                         VALUES (%s, %s)""",
                      (parent_id, dir_entry_id)))


def add_directory_entry_file(db_conn, name, sha, perms,
                             atime, mtime, ctime, parent_id):
    """Insert a new directory entry file.
    """
    dir_entry_id = db.insert(db_conn,
                             ("""INSERT INTO directory_entry_file
                                 (name, target, perms, atime, mtime, ctime)
                                 VALUES (%s, %s, %s, %s, %s, %s)
                                 RETURNING id""",
                              (name, sha, perms, atime, mtime, ctime)))

    db.query_execute(db_conn,
                     ("""INSERT INTO directory_list_file
                         (dir_id, entry_id)
                         VALUES (%s, %s)""",
                      (parent_id, dir_entry_id)))


def add_revision(db_conn, sha, date, directory, message, author, committer,
                 parent_shas=None):
    """Insert a revision.
    """
    db.query_execute(db_conn,
                     ("""INSERT INTO revision
                         (id, date, type, directory, message, author, committer)
                         VALUES (%s, %s, %s, %s, %s,
                                 (select id from person where name=%s and email=%s),
                                 (select id from person where name=%s and email=%s))""",
                      (sha, date, 'git', directory, message,
                       author['name'], author['email'],
                       committer['name'], committer['email'])))


def add_revision_history(db_conn, couple_parents):
    """Store the revision history graph.
    """
    tuples = ','.join(["('%s','%s')" % couple for couple in couple_parents])
    query = 'INSERT INTO revision_history (id, parent_id) VALUES ' + tuples
    db.query_execute(db_conn, query)


def add_release(db_conn, obj_sha, revision, date, name, comment, author):
    """Insert a release.
    """
    db.query_execute(db_conn,
                     ("""INSERT INTO release (id, revision, date, name, comment, author)
                         VALUES (%s, %s, %s, %s, %s,
                                 (select id from person where name=%s and email=%s))""",
                      (obj_sha, revision, date, name, comment, author['name'], author['email'])))


def add_occurrence(db_conn, url_origin, branch, revision):
    """Insert an occurrence.
       Check if occurrence history already present.
       If present do nothing, otherwise insert
    """
    with db_conn.cursor() as cur:
        occ = find_occurrence(cur, branch, revision, url_origin)
        if not occ:
            db.execute(
                cur,
                ("""INSERT INTO occurrence
                    (origin, branch, revision)
                    VALUES ((select id from origin where url=%s), %s, %s)""",
                 (url_origin, branch, revision)))


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
    return find_object(db_conn, obj_sha, Type.content, column='sha1')


def find_occurrences_for_revision(db_conn, revision, type):
    """Find all occurences for a specific revisions.
    type is not used (implementation detail).
    """
    return db.query_fetch(db_conn, ("""SELECT *
                                       FROM occurrence
                                       WHERE revision=%s""",
                                    (revision,)))


def find_origin(db_conn, origin_url, origin_type):
    """Find all origins matching an url and an origin type.
    """
    return db.query_fetchone(db_conn, ("""SELECT *
                                       FROM origin
                                       WHERE url=%s
                                       AND type=%s""",
                                       (origin_url, origin_type)))

def find_person(db_conn, email, name):
    """Find a person uniquely identified by email and name.
    """
    return db.query_fetchone(db_conn, ("""SELECT id
                                          FROM person
                                          WHERE email=%s
                                          AND name=%s""",
                                       (email, name)))


def find_occurrence(cur, branch, revision, url_origin):
    """Find an ocurrence with branch pointing on valid revision for date.
    """
    return db.fetchone(
        cur,
        ("""SELECT *
            FROM occurrence oc
            WHERE branch=%s
            AND revision=%s
            AND origin = (select id from origin where url = %s)""",
         (branch, revision, url_origin)))


def find_object(db_conn, obj_sha, obj_type, column='id'):
    """Find an object of obj_type by its obj_sha.
    """
    table = obj_type if isinstance(obj_type, str) else obj_type.value
    query = 'select '+ column + ' from ' + table + ' where ' + column + '=%s'
    return db.query_fetchone(db_conn, (query, (obj_sha,)))


def filter_unknown_objects(db_conn, file_sha1s,
                           table_to_filter, tbl_tmp_name,
                           column_to_filter='id', nature_column='sha1_git'):
    """Given a list of sha1s, filter the unknown object between this list and
    the content of the table table_to_filter.
    tbl_tmp_name is the temporary table used to filter.
    """
    with db_conn.cursor() as cur:
        # explicit is better than implicit
        # simply creating the temporary table seems to be enough
        db.execute(cur, """CREATE TEMPORARY TABLE IF NOT EXISTS %s(
                             %s %s)
                           ON COMMIT DELETE ROWS;""" %
                   (tbl_tmp_name, column_to_filter, nature_column))
        db.copy_from(cur, file_sha1s, tbl_tmp_name)
        db.execute(cur, '(SELECT %s FROM %s) EXCEPT (SELECT %s FROM %s);' %
                   (column_to_filter, tbl_tmp_name,
                    column_to_filter, table_to_filter))
        return cur.fetchall()


def find_unknown_revisions(db_conn, file_sha1s):
    """Filter unknown revisions from file_sha1s.
    """
    return filter_unknown_objects(db_conn, file_sha1s,
                                  'revision', 'filter_sha1_revision')


def find_unknown_directories(db_conn, file_sha1s):
    """Filter unknown directories from file_sha1s.
    """
    return filter_unknown_objects(db_conn, file_sha1s,
                                  'directory', 'filter_sha1_directory')


def find_unknown_contents(db_conn, file_sha1s):
    """Filter unknown contents from file_sha1s.
    """
    return filter_unknown_objects(db_conn, file_sha1s,
                                  'content',
                                  'filter_sha1_content',
                                  'sha1', 'sha1')


def _count_objects(db_conn, type):
    return db.query_fetchone(db_conn, 'SELECT count(*) FROM ' + type.value)[0]


def count_revisions(db_conn):
    """Count the number of revisions.
    """
    return _count_objects(db_conn, Type.revision)


def count_directories(db_conn):
    """Count the number of directories.
    """
    return _count_objects(db_conn, Type.directory)


def count_contents(db_conn):
    """Count the number of contents.
    """
    return _count_objects(db_conn, Type.content)


def count_occurrence(db_conn):
    """Count the number of occurrence.
    """
    return _count_objects(db_conn, Type.occurrence)


def count_release(db_conn):
    """Count the number of occurrence.
    """
    return _count_objects(db_conn, Type.release)


def count_person(db_conn):
    """Count the number of occurrence.
    """
    return _count_objects(db_conn, Type.person)
