# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime


#FIXME: Find the pythonic way to use Higher-order functions (too much duplication here)

def cleandb(db_conn, only_truncate=False):
    """Clean the database.
    """
    cur = db_conn.cursor()

    action = "truncate table" if only_truncate else "drop table if exists"

    cur.execute("{} file_cache;".format(action))
    cur.execute("{} object_cache;".format(action))

    db_conn.commit()
    cur.close()


def initdb(db_conn):
    """Initialize the database.
    """
    cur = db_conn.cursor()
    cur.execute("""create table if not exists file_cache
         (sha256 varchar(65) primary key,
          status_write boolean,
          path varchar(255),
          last_seen date);""")
    cur.execute("""create table if not exists object_cache (sha1 varchar(41) primary key,
                                              type integer,
                                              last_seen date);""")
    db_conn.commit()
    cur.close()


def add_file(db_conn, sha, filepath, status):
    """Insert a new file
    """
    cur = db_conn.cursor()
    cur.execute("""insert into file_cache (sha256, path, last_seen, status_write)
                   values (%s, %s, %s, %s);""",
                (sha, filepath, datetime.now(), status))
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


def find_file(db_conn, sha):
    """Find a file by its hash.
    """
    cur = db_conn.cursor()
    cur.execute("""select sha256 from file_cache
                   where 1=%s and sha256=%s;""",
                (1, sha))  # ? need to have at least 2 otherwise fails!
    res = cur.fetchone()
    cur.close()
    return res


def find_object(db_conn, sha):
    """Find an object by its hash.
    """
    cur = db_conn.cursor()
    cur.execute("""select sha1 from object_cache
                   where 1=%s and sha1 = %s;""",
                (1, sha))
    res = cur.fetchone()
    cur.close()
    return res


# not working!
# def count(db_conn, tablename, type = None):
#     """Count the number of objects inside the tablename."""
#     cur = db_conn.cursor()
#     count_str = " where type = {}".format(type) if type == 0 or type == 1 else ""

#     cur.execute("""select count(*) from %s%s;""", (tablename, count_str))

#     res = cur.fetchone()[0]
#     cur.close()
#     return res


def count_files(db_conn):
    """Count the number of objects inside the tablename."""
    cur = db_conn.cursor()

    cur.execute("""select count(*) from file_cache;""")

    res = cur.fetchone()[0]
    cur.close()
    return res


def count_objects(db_conn, type):
    """Count the number of objects inside the tablename."""
    cur = db_conn.cursor()

    cur.execute("""select count(*) from object_cache
                   where 1=%s and type=%s;""", (1, type))

    res = cur.fetchone()[0]
    cur.close()
    return res
