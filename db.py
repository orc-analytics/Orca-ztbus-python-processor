import psycopg2
from psycopg2 import pool
import os

POSTGRES_CONFIG = {
    "host": os.environ.get("PG_HOST"),
    "database": os.environ.get("PG_DB"),
    "user": os.environ.get("PG_USER"),
    "password": os.environ.get("PG_PASS"),
    "port": os.environ.get("PG_PORT"),
}

_pool = pool.SimpleConnectionPool(minconn=1, maxconn=10, **POSTGRES_CONFIG)


def GetConnection():
    return _pool.getconn()


def ReturnConnection(conn):
    _pool.putconn(conn)


def ClosePool():
    _pool.closeall()
