import datetime as dt
from orca_python import EmitWindow, Window
import psycopg2.extras
from fastapi import FastAPI, Depends
from typing import TypedDict, Generator, Union, List
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

    with conn.cursor() as cur:
        cur.execute(query)
        conn.commit()


app = FastAPI()


@app.on_event("startup")
def on_startup() -> None:
    # Initialize pool when app starts
    db_pool.init_pool(minconn=1, maxconn=5)


@app.on_event("shutdown")
def on_shutdown() -> None:
    # Close pool when app stops
    db_pool.close_pool()


def get_db_conn() -> Generator[PGConnection, None, None]:
    with db_pool.connection() as conn:
        yield conn


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _helper(conn: PGConnection) -> None:
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
        res: List[ReadSimlogRow] = [
            ReadSimlogRow(
                {
                    "id": row["id"],
                    "end_time": row["end_time"],
                    "start_time": row["start_time"],
                }
            )
            for row in results
        ]

        simlog: Union[ReadSimlogRow, List[ReadSimlogRow]]
        if len(res) > 0:
            simlog = res[0]
        else:
            simlog = res

    if len(simlog) == 0:
        # earliest time where both busses are active: 2021-03-09 14:15:05.000
        start_time = dt.datetime(2021, 3, 9, 14, 15)
        end_time = start_time + dt.timedelta(seconds=60)
    else:
        # simlog is guaranteed to be ReadSimlogRow here since len(simlog) != 0
        assert isinstance(simlog, dict)
        end_time = simlog["end_time"]
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


@app.post("/")
def FindAndEmitMinuteWindow(conn: PGConnection = Depends(get_db_conn)) -> None:
    _helper(next(conn))


if __name__ == "__main__":
    import schedule

    conn = next(get_db_conn())

    schedule.every(1).second.do(_helper(conn))

    while True:
        schedule.run_pending()
