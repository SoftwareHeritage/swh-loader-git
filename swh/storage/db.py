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


def execute(cur, query_params, trace=None):
    """Execute the query_params.
    query_params is expected to be either:
    - a sql query (string)
    - a tuple (sql query, params)
    """
    if isinstance(query_params, str):
        cur.execute(query_params)
    else:
        if trace is not None:
            print("mogrify: ", cur.mogrify(*query_params).decode())
        cur.execute(*query_params)


def copy_from(cur, file, table):
    """Copy the content of a file to the db in the table table.
    """
    cur.copy_from(file, table)


def insert(db_conn, query_params, trace=None):
    """Execute an insertion and returns the identifier.
    Expect an insert query with the right returning clause.
    No check is done.
    """
    with db_conn.cursor() as cur:
        execute(cur, query_params)
        result = cur.fetchone()
        return result[0]


def query_execute(db_conn, query_params, trace=None):
    """Execute one query.
    Type of sql queries: insert, delete, drop, create...
    query_params is expected to be either:
    - a sql query (string)
    - a tuple (sql query, params)
    """
    with db_conn.cursor() as cur:
        return execute(cur, query_params, trace)


def queries_execute(db_conn, queries_params, trace=None):
    """Execute multiple queries without any result expected.
    Type of sql queries: insert, delete, drop, create...
    query_params is expected to be a list of mixed:
    - sql query (string)
    - tuple (sql query, params)
    """
    with db_conn.cursor() as cur:
        for query_params in queries_params:
            execute(cur, query_params, trace)


def query_fetchone(db_conn, query_params, trace=None):
    """Execute sql query which returns one result.
    query_params is expected to be either:
    - a sql query (string)
    - a tuple (sql query, params)
    """
    with db_conn.cursor() as cur:
        return fetchone(cur, query_params, trace)


def fetchone(cur, query_params, trace=None):
    """Execute sql query and returns one result.
    """
    execute(cur, query_params, trace)
    return cur.fetchone()


def query_fetch(db_conn, query_params, trace=None):
    """Execute sql query which returns results.
    query_params is expected to be either:
    - a sql query (string)
    - a tuple (sql query, params)
    """
    with db_conn.cursor() as cur:
        execute(cur, query_params, trace)
        return cur.fetchall()
