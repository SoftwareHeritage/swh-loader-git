# Copyright (C) 2015  Stefano Zacchiroli <zack@upsilon.cc>,
#                     Antoine R. Dumont <antoine.romain.dumont@gmail.com>
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import psycopg2


def connect(db_url):
    """Open db connection.
    """
    return psycopg2.connect(db_url)


def _execute(cur, query_params):
    """Execute the query_params.
    query_params is expected to be either:
    - a sql query (string)
    - a tuple (sql query, params)
    """
    if isinstance(query_params, str):
        cur.execute(query_params)
    else:
        cur.execute(*query_params)


def query_execute(db_conn, query_params):
    """Execute one query.
    Type of sql queries: insert, delete, drop, create...
    query_params is expected to be either:
    - a sql query (string)
    - a tuple (sql query, params)
    """
    with db_conn.cursor() as cur:
        _execute(cur, query_params)


def queries_execute(db_conn, queries_params):
    """Execute multiple queries without any result expected.
    Type of sql queries: insert, delete, drop, create...
    query_params is expected to be a list of mixed:
    - sql query (string)
    - tuple (sql query, params)
    """
    with db_conn.cursor() as cur:
        for query_params in queries_params:
            _execute(cur, query_params)


def query_fetchone(db_conn, query_params):
    """Execute sql query which returns one result.
    query_params is expected to be either:
    - a sql query (string)
    - a tuple (sql query, params)
    """
    with db_conn.cursor() as cur:
        _execute(cur, query_params)
        return cur.fetchone()


def query_fetch(db_conn, query_params):
    """Execute sql query which returns results.
    query_params is expected to be either:
    - a sql query (string)
    - a tuple (sql query, params)
    """
    with db_conn.cursor() as cur:
        _execute(cur, query_params)
        return cur.fetchall()
