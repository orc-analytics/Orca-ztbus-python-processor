import datetime as dt
from orca_python import EmitWindow, Window
import psycopg2.extras
from fastapi import FastAPI, Depends
from typing import TypedDict
from db import db_pool
from psycopg2.extensions import connection as PGConnection
from windows import EveryMinute


class ReadSimlogRow(TypedDict):
    id: int
    start_time: dt.datetime
    end_time: dt.datetime


class CreateSimlogEntryParams(TypedDict):
    start_time: dt.datetime
    end_time: dt.datetime


def CreateSimLogsTable(conn: PGConnection) -> None:
    query = """
        CREATE TABLE IF NOT EXISTS sim_logs (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL
        );
    """

    conn = None
    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()


app = FastAPI()


@app.on_event("startup")
def on_startup():
    # Initialize pool when app starts
    db_pool.init_pool(minconn=1, maxconn=5)


@app.on_event("shutdown")
def on_shutdown():
    # Close pool when app stops
    db_pool.close_pool()


def get_db_conn():
    with db_pool.connection() as conn:
        yield conn


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/")
def FindAndEmitMinuteWindow(conn=Depends(get_db_conn)):
    query = """
        SELECT
            s.id,
            s.start_time, 
            s.end_time
        FROM sim_logs s
        ORDER BY s.end_time
        DESC LIMIT 1;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        results = cur.fetchall()
        res = [ReadSimlogRow(**row) for row in results]
        if len(res) > 0:
            simlog = res[0]
        else:
            simlog = res

    if len(simlog) == 0:
        # earliest time where both busses are active: 2021-03-09 14:15:05.000
        start_time = dt.datetime(2021, 3, 9, 14, 15)
        end_time = start_time + dt.timedelta(seconds=60)
    else:
        end_time = simlog.get("end_time")
        start_time = end_time
        end_time = end_time + dt.timedelta(seconds=60)

    query = """INSERT INTO sim_logs (start_time,end_time) VALUES (%(start_time)s, %(end_time)s)"""

    params = CreateSimlogEntryParams(start_time=start_time, end_time=end_time)

    with conn.cursor() as cur:
        cur.execute(query, params)
        conn.commit()

    EmitWindow(
        Window(
            time_from=params["start_time"],
            time_to=params["end_time"],
            name=EveryMinute.name,
            version=EveryMinute.version,
            origin="simulator",
        )
    )
