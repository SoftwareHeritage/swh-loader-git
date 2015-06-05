# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from datetime import datetime

from sgloader.db_utils import db_connect


def cleandb(db_url, only_truncate=False):
    """Clean the database.
    """
    conn = db_connect(db_url)
    cur = conn.cursor()

    action = "truncate table" if only_truncate else "drop table if exists"

    cur.execute("{} file_cache;".format(action))
    cur.execute("{} object_cache;".format(action))

    conn.commit()
    cur.close()


def initdb(db_url):
    """Initialize the database.
    """
    conn = db_connect(db_url)
    cur = conn.cursor()
    cur.execute("""create table if not exists file_cache (sha256 varchar(65) primary key,
                                            path varchar(255),
                                            last_seen date);""")
    cur.execute("""create table if not exists object_cache (sha1 varchar(41) primary key,
                                              type integer,
                                              last_seen date);""")
    conn.commit()
    cur.close()


def add_file(db_url, sha, filepath):
    """Insert a new file"""
    conn = db_connect(db_url)
    cur = conn.cursor()
    cur.execute("""insert into file_cache (sha256, path, last_seen)
                   values (%s, %s, %s);""",
                (sha, filepath, datetime.now()))
    conn.commit()
    cur.close()


def add_object(db_url, sha, type):
    """insert a new object"""
    conn = db_connect(db_url)
    cur = conn.cursor()
    cur.execute("""insert into object_cache (sha1, type, last_seen)
                   values (%s, %s, %s);""",
                (sha, type, datetime.now()))
    conn.commit()
    cur.close()


def find_file(db_url, sha):
    """find a file by its hash.
    """
    conn = db_connect(db_url)
    cur = conn.cursor()
    cur.execute("""select sha256 from file_cache
                   where 1=%s and sha256=%s;""",
                (1, sha))  # ? need to have at least 2 otherwise fails!
    res = cur.fetchone()
    cur.close()
    return res


def find_object(db_url, sha):
    """Find an object by its hash.
    """
    conn = db_connect(db_url)
    cur = conn.cursor()
    cur.execute("""SELECT sha1 from object_cache
                   where 1=%s and sha1 = %s;""",
                (1, sha))
    res = cur.fetchone()
    cur.close()
    return res
