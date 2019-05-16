import logging
from urllib.parse import urlparse
import psycopg2
from psycopg2.extensions import connection, cursor
from contextlib import contextmanager


@contextmanager
def connect(database_url: str, auto_commit: bool = False) -> cursor:
    with create_connection(database_url) as conn, create_transaction(conn, auto_commit=auto_commit) as cur:
        yield cur


@contextmanager
def create_connection(database_url: str) -> connection:
    logging.debug(f'Connecting to: {database_url}')
    url = urlparse(database_url)
    conn = psycopg2.connect(
        database=url.path[1:],
        user=url.username,
        password=url.password,
        host=url.hostname,
        port=url.port
    )
    try:
        yield conn
    finally:
        logging.debug(f'Closing: {database_url}')
        conn.close()


@contextmanager
def create_transaction(conn: connection, auto_commit: bool = False) -> cursor:
    cursor = conn.cursor()
    try:
        yield cursor
    except Exception:
        logging.debug('Rolling back')
        conn.rollback()
        raise
    else:
        if auto_commit:
            logging.debug('Committing')
            conn.commit()
        else:
            logging.debug('Rolling back')
            conn.rollback()


def get_version(cur: cursor) -> str:
    cur.execute("SELECT version();")
    result = cur.fetchone()
    return result

def get_citus_version(cur: cursor) -> str:
    cur.execute("SELECT citus_version()")
    result = cur.fetchone()
    return result
