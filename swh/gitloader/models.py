# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2
import logging

from contextlib import contextmanager
from datetime import datetime


@contextmanager
def db_connect(db_url):
    """Open db connection.
    """
    db_conn = psycopg2.connect(db_url)
    try:
        yield db_conn
    finally:
        db_conn.close()


@contextmanager
def execute(db_conn):
    """Execute sql insert, create, dropb, delete query to db.
    """
    cur = db_conn.cursor()
    try:
        yield cur
    except:
        logging.error("An error has happenned, rollback db!")
        db_conn.rollback()
        raise
    finally:
        db_conn.commit()
        cur.close()

@contextmanager
def fetch(db_conn):
    """Execute sql select query to db.
    """
    cur = db_conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def cleandb(db_conn, only_truncate=False):
    """Clean the database.
    """
    with execute(db_conn) as cur:
        action = "truncate table" if only_truncate else "drop table if exists"
        cur.execute("%s files;" % action)
        cur.execute("%s git_objects;" % action)


def initdb(db_conn):
    """Initialize the database.
    """
    with execute(db_conn) as cur:
        cur.execute("""create table if not exists files
             (sha1 bytea primary key,
              size integer,
              sha1_git bytea,
              ctime date);""")
        cur.execute("""create table if not exists git_objects (
                                   sha1 bytea primary key,
                                   type integer,
                                   ctime date);""")


def add_blob(db_conn, obj_sha, size, obj_git_sha):
    """Insert a new file
    """
    with execute(db_conn) as cur:
        cur.execute("""insert into files (sha1, size, sha1_git, ctime)
                       values (%s, %s, %s, %s);""",
                    (obj_sha, size, obj_git_sha, datetime.now()))


def add_object(db_conn, obj_sha, obj_type):
    """Insert a new object
    """
    with execute(db_conn) as cur:
        cur.execute("""insert into git_objects (sha1, type, ctime)
                       values (%s, %s, %s);""",
                    (obj_sha, obj_type.value, datetime.now()))


def find_blob(db_conn, obj_sha):
    """Find a file by its hash.
    """
    with fetch(db_conn) as cur:
        cur.execute("""select sha1 from files
                       where sha1=%s;""",
                    (obj_sha,))
        return cur.fetchone()


def find_object(db_conn, obj_sha, obj_type):
    """Find an object by its hash.
    """
    with fetch(db_conn) as cur:
        cur.execute("""select sha1 from git_objects
                       where sha1=%s
                       and type=%s;""",
                    (obj_sha, obj_type.value))
        return cur.fetchone()


def count_files(db_conn):
    """Count the number of blobs."""
    with fetch(db_conn) as cur:
        cur.execute("""select count(*) from files;""")
        return cur.fetchone()[0]


def count_objects(db_conn, obj_type):
    """Count the number of objects with obj_type."""
    with fetch(db_conn) as cur:
        cur.execute("""select count(*) from git_objects
                       where type=%s;""", (obj_type.value,))
        return cur.fetchone()[0]
