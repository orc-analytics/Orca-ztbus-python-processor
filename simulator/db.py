import os
from psycopg2 import pool
from contextlib import contextmanager


class PostgresPool:
    def __init__(self):
        self._pool: pool.SimpleConnectionPool | None = None

    def init_pool(self, minconn: int = 1, maxconn: int = 10):
        if self._pool is None:
            print(os.environ["ZTBUS_PORT"])
            self._pool = pool.SimpleConnectionPool(
                minconn=minconn,
                maxconn=maxconn,
                host=os.environ["ZTBUS_ADDR"],
                database=os.environ["ZTBUS_DB"],
                user=os.environ["ZTBUS_USER"],
                password=os.environ["ZTBUS_PASS"],
                port=os.environ["ZTBUS_PORT"],
            )

    def close_pool(self):
        if self._pool:
            self._pool.closeall()
            self._pool = None

    @contextmanager
    def connection(self):
        if not self._pool:
            raise RuntimeError("Connection pool is not initialised")
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)


db_pool = PostgresPool()
