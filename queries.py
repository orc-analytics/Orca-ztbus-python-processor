import datetime as dt
from typing import TypedDict, Optional, List, Generator
from db import GetConnection, ReturnConnection
import psycopg2.extras


class ReadTelemParams(TypedDict):
    trip_id: int
    time_from: Optional[dt.datetime]
    time_to: Optional[dt.datetime]


class ReadTelemResultRow(TypedDict):
    id: int
    trip_id: int
    time: dt.datetime
    electric_power_demand: float
    temperature_ambient: float
    traction_brake_pressure: float
    traction_traction_force: float
    gnss_altitude: Optional[float]
    gnss_course: Optional[float]
    gnss_latitude: Optional[float]
    gnss_longitude: Optional[float]
    itcs_bus_route_id: int
    itcs_number_of_passengers: int
    itcs_stop_name: str
    odometry_articulation_angle: float
    odometry_steering_angle: float
    odometry_vehicle_speed: float
    odometry_wheel_speed_fl: float
    odometry_wheel_speed_fr: float
    odometry_wheel_speed_ml: float
    odometry_wheel_speed_mr: float
    odometry_wheel_speed_rl: float
    odometry_wheel_speed_rr: float
    status_door_is_open: bool
    status_grid_is_available: bool
    status_halt_brake_is_active: bool
    status_park_brake_is_active: bool


def ReadTelemetryForTripAndTime(params: ReadTelemParams) -> Generator[ReadTelemResultRow, None, None]:
    BASE_QUERY = """
        SELECT
            id,
            trip_id,
            time,
            electric_power_demand,
            temperature_ambient,
            traction_brake_pressure,
            traction_traction_force,
            gnss_altitude,
            gnss_course,
            gnss_latitude,
            gnss_longitude,
            itcs_bus_route_id,
            itcs_number_of_passengers,
            itcs_stop_name,
            odometry_articulation_angle,
            odometry_steering_angle,
            odometry_vehicle_speed,
            odometry_wheel_speed_fl,
            odometry_wheel_speed_fr,
            odometry_wheel_speed_ml,
            odometry_wheel_speed_mr,
            odometry_wheel_speed_rl,
            odometry_wheel_speed_rr,
            status_door_is_open,
            status_grid_is_available,
            status_halt_brake_is_active,
            status_park_brake_is_active
        FROM telemetry
        WHERE trip_id = %(trip_id)s
    """

    if params.get("time_from") and params.get("time_to"):
        BASE_QUERY += " AND time BETWEEN %(time_from)s AND %(time_to)s"
    elif params.get("time_from"):
        BASE_QUERY += " AND time >= %(time_from)s"
    elif params.get("time_to"):
        BASE_QUERY += " AND time <= %(time_to)s"

    conn = None

    try:
        conn = GetConnection()
        with conn.cursor(name='telem_cursor', cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(BASE_QUERY, params)
            for row in cur:
                yield ReadTelemResultRow(**row)
    finally:
        if conn:
            ReturnConnection(conn)

class ReadTripsFromTripIdParams(TypedDict):
    trip_id: int

class ReadTripsFromTripIdRow(TypedDict):
    id: int
    name: str
    bus_id: int
    route_id: int
    start_time: dt.datetime
    end_time: dt.datetime
    driven_distance_km: float
    energy_consumption_kwh: float
    itcs_passengers_mean: float
    itcs_passengers_min: int
    itcs_passengers_max: int
    grid_available_mean: float
    amb_temperature_mean: float
    amb_temperature_min: float
    amb_temperature_max: float 

def ReadTripsFromTripId(params: ReadTripsFromTripIdParams) -> ReadTripsFromTripIdRow:
    query = """
        SELECT
            t.id,
            t.name,
            t.bus_id,
            t.route_id,
            t.start_time,
            t.end_time,
            t.driven_distance_km,
            t.energy_consumption_kwh,
            t.itcs_passengers_mean,
            t.itcs_passengers_min,
            t.itcs_passengers_max,
            t.grid_available_mean,
            t.amb_temperature_mean,
            t.amb_temperature_min,
            t.amb_temperature_max
        FROM trips t
        WHERE t.id = %(trip_id)s LIMIT 1;
    """

    conn = None
    try:
        conn = GetConnection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            results = cur.fetchall()
            return [ReadTelemResultRow(**row) for row in results][0]
    finally:
        if conn:
            ReturnConnection(conn)

def CreateSimLogsTable() -> None:
    query = """
        CREATE TABLE IF NOT EXISTS sim_logs (
            id SERIAL PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP NOT NULL
        );
    """

    conn = None
    try:
        conn = GetConnection()
        with conn.cursor() as cur:
            cur.execute(query)
            conn.commit()
    except Exception as e:
        raise Exception(f"Could not create simlogs table: {e}")
    finally:
        if conn:
            ReturnConnection(conn)

class CreateSimlogEntryParams(TypedDict):
    start_time: dt.datetime
    end_time: dt.datetime


def CreateSimLogEntry(params: CreateSimlogEntryParams) -> None:
    QUERY = """INSERT INTO sim_logs (start_time,end_time) VALUES (%(start_time)s, %(end_time)s)"""

    conn = None

    try:
        conn = GetConnection()
        # with conn.cursor(name='simlogs_insert_cursor', cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        with conn.cursor() as cur:
            cur.execute(QUERY, params)
            conn.commit()
    except Exception as e:
        raise Exception(f"could not insert simlog: {e}")
    finally:
        if conn:
            ReturnConnection(conn)

class ReadSimlogRow(TypedDict):
    id: int
    start_time: dt.datetime
    end_time: dt.datetime

def ReadLatestSimlog() -> ReadSimlogRow:
    query = """
        SELECT
            s.id,
            s.start_time, 
            s.end_time
        FROM sim_logs s
        ORDER BY s.end_time
        DESC LIMIT 1;
    """

    conn = None
    try:
        conn = GetConnection()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query)
            results = cur.fetchall()
            res = [ReadSimlogRow(**row) for row in results]
            if len(res) >0:
                return res[0]
            else:
                return res
    finally:
        if conn:
            ReturnConnection(conn)
