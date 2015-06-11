# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2
import logging

from contextlib import contextmanager


@contextmanager
def connect(db_url):
    """Open db connection.
    """
    db_conn = psycopg2.connect(db_url)
    try:
        yield db_conn
    finally:
        db_conn.close()


@contextmanager
def _execute(db_conn):
    """Execute sql insert, create, dropb, delete query to db.
    """
    cur = db_conn.cursor()
    try:
        yield cur
    except:
        logging.error("An error has happened, rollback db!")
        db_conn.rollback()
        raise
    finally:
        db_conn.commit()
        cur.close()


@contextmanager
def _fetch(db_conn):
    """Execute sql select query to db.
    """
    cur = db_conn.cursor()
    try:
        yield cur
    finally:
        cur.close()


def query_execute(db_conn, query_params):
    """Execute one query.
       Type of sql queries: insert, delete, drop, create...
    """
    queries_execute(db_conn, [query_params])


def queries_execute(db_conn, queries_params):
    """Execute multiple queries without any result expected.
       Type of sql queries: insert, delete, drop, create...
    """
    with _execute(db_conn) as cur:
        for query_params in queries_params:
            if isinstance(query_params, str):
                cur.execute(query_params)
            else:
                query, params = query_params
                cur.execute(cur.mogrify(query, params))


def query_fetchone(db_conn, query_params):
    """Execute sql query which returns one result.
    """
    with _fetch(db_conn) as cur:
        if isinstance(query_params, str):
            cur.execute(query_params)
        else:
            query, params = query_params
            cur.execute(cur.mogrify(query, params))
        return cur.fetchone()
