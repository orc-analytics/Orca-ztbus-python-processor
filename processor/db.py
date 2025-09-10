import os
from psycopg2 import pool
from contextlib import contextmanager
from psycopg2.extensions import connection as PGConnection
from typing import Generator


class PostgresPool:
    def __init__(self) -> None:
        self._pool: pool.SimpleConnectionPool | None = None

    def init_pool(self, minconn: int = 1, maxconn: int = 10) -> None:
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

    def close_pool(self) -> None:
        if self._pool:
            self._pool.closeall()
            self._pool = None

    @contextmanager
    def connection(self) -> Generator[PGConnection, None, None]:
        if not self._pool:
            raise RuntimeError("Connection pool is not initialised")
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)


db_pool = PostgresPool()
