# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime


# FIXME: Find the pythonic way to use Higher-order
# functions (too much duplication here) -> lambda with varargs parameters?

def cleandb(db_conn, only_truncate=False):
    """Clean the database.
    """
    cur = db_conn.cursor()

    action = "truncate table" if only_truncate else "drop table if exists"

    cur.execute("{} blob_cache;".format(action))
    cur.execute("{} object_cache;".format(action))

    db_conn.commit()
    cur.close()


def initdb(db_conn):
    """Initialize the database.
    """
    cur = db_conn.cursor()
    cur.execute("""create table if not exists blob_cache
         (sha1 bytea primary key,
          path varchar(255),
          last_seen date);""")
    cur.execute("""create table if not exists object_cache (sha1 bytea primary key,
                                              type integer,
                                              last_seen date);""")
    db_conn.commit()
    cur.close()


def add_blob(db_conn, sha, filepath):
    """Insert a new file
    """
    cur = db_conn.cursor()
    cur.execute("""insert into blob_cache (sha1, path, last_seen)
                   values (%s, %s, %s);""",
                (sha, filepath, datetime.now()))
    db_conn.commit()
    cur.close()


def add_object(db_conn, sha, type):
    """Insert a new object
    """
    cur = db_conn.cursor()
    cur.execute("""insert into object_cache (sha1, type, last_seen)
                   values (%s, %s, %s);""",
                (sha, type, datetime.now()))
    db_conn.commit()
    cur.close()


def find_blob(db_conn, sha):
    """Find a file by its hash.
    """
    cur = db_conn.cursor()
    cur.execute("""select sha1 from blob_cache
                   where sha1=%s;""",
                (sha,))
    res = cur.fetchone()
    cur.close()
    return res


def find_object(db_conn, sha, type):
    """Find an object by its hash.
    """
    cur = db_conn.cursor()
    cur.execute("""select sha1 from object_cache
                   where sha1=%s
                   and type=%s;""",
                (sha,type))
    res = cur.fetchone()
    cur.close()
    return res


def count_files(db_conn):
    """Count the number of objects inside the tablename."""
    cur = db_conn.cursor()

    cur.execute("""select count(*) from file_cache;""")

    res = cur.fetchone()[0]
    cur.close()
    return res


def count_objects(db_conn, type):
    """Count the number of objects with type `type` inside the tablename."""
    cur = db_conn.cursor()

    cur.execute("""select count(*) from object_cache
                   where type=%s;""", (type,))

    res = cur.fetchone()[0]
    cur.close()
    return res
