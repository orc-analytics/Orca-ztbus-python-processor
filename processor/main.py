from orca_python import (
    Processor,
    ExecutionParams,
    EmitWindow,
    Window,
    WindowType,
    StructResult,
    ValueResult,
)
import datetime as dt
import pandas as pd
from db import db_pool
from psycopg2.extensions import connection as PGConnection

from windows import (
    EveryMinute,
    EveryMinutePerTripPerBus,
)

from typing import TypedDict, Optional, Callable, ParamSpec, TypeVar, List
import psycopg2.extras
import functools
from frozendict import frozendict


proc = Processor("analyser")

P = ParamSpec("P")
T = TypeVar("T")


def freezeargs(func: Callable[P, T]) -> Callable[P, T]:
    """
    Convert a mutable dictionary into immutable.
    Useful to be compatible with cache
    """

    @functools.wraps(func)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> T:
        # Convert mutable dicts to frozendict in args
        frozen_args = tuple(
            frozendict(arg) if isinstance(arg, dict) else arg for arg in args
        )
        # Convert mutable dicts to frozendict in kwargs
        frozen_kwargs = {
            k: frozendict(v) if isinstance(v, dict) else v for k, v in kwargs.items()
        }
        return func(*frozen_args, **frozen_kwargs)  # type: ignore

    return wrapped


class ReadTelemParams(TypedDict):
    trip_id: Optional[int]
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


@freezeargs
@functools.lru_cache
def _get_telemetry_query_and_params(params: ReadTelemParams) -> tuple[str, dict]:
    """Return the query and parameters - cacheable without connection"""
    # validate that at least one parameter is provided
    if not any([params.get("trip_id"), params.get("time_from"), params.get("time_to")]):
        raise ValueError(
            "at least one of trip_id, time_from, or time_to must be provided"
        )

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
        WHERE 1=1
    """

    # add trip_id condition if provided
    if params.get("trip_id"):
        BASE_QUERY += " AND trip_id = %(trip_id)s"

    # add time conditions
    if params.get("time_from") and params.get("time_to"):
        BASE_QUERY += " AND time BETWEEN %(time_from)s AND %(time_to)s"
    elif params.get("time_from"):
        BASE_QUERY += " AND time >= %(time_from)s"
    elif params.get("time_to"):
        BASE_QUERY += " AND time <= %(time_to)s"

    return BASE_QUERY, params


def ReadTelemetryForTripAndTime(
    params: ReadTelemParams, conn: PGConnection
) -> List[ReadTelemResultRow]:
    query, query_params = _get_telemetry_query_and_params(params)
    with conn.cursor(
        name="telem_cursor", cursor_factory=psycopg2.extras.RealDictCursor
    ) as cur:
        cur.execute(query, params)
        return [ReadTelemResultRow(**row) for row in cur]  # type: ignore


class ReadActiveBussesParams(TypedDict):
    time_from: dt.datetime
    time_to: dt.datetime


class ReadActiveBussesRow(TypedDict):
    trip_id: int
    bus_id: int
    route_id: int


def ReadActiveBusses(
    params: ReadActiveBussesParams, conn: PGConnection
) -> List[ReadActiveBussesRow]:
    query = """
        SELECT DISTINCT t.trip_id, tr.bus_id, tr.route_id 
        FROM telemetry t 
        JOIN trips tr ON t.trip_id = tr.id 
        WHERE t."time" BETWEEN %(time_from)s AND %(time_to)s
    """
    with conn.cursor(
        name="telem_cursor", cursor_factory=psycopg2.extras.RealDuctCursor
    ) as cur:
        cur.execute(query, params)
        return [ReadActiveBussesRow(**row) for row in cur]  # type: ignore


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


def ReadTripsFromTripId(
    params: ReadTripsFromTripIdParams, conn: PGConnection
) -> ReadTripsFromTripIdRow:
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

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params)
        results = cur.fetchall()
        return [ReadTelemResultRow(**row) for row in results][0]  # type: ignore


def _find_contiguous_chunks_and_emit(
    df: pd.DataFrame,
    tgt_column: str,
    time_column: str,
    trip_id: int,
    params: ExecutionParams,
    emitting_window: WindowType,
    origin: str,
    conn: PGConnection,
    lookback_window: dt.timedelta = dt.timedelta(seconds=20),  # seconds
    max_lookback_iterations: int = 20,
) -> pd.DataFrame:
    # if the first value is true, then we need to look back
    if df.loc[0, tgt_column]:
        ii = 0
        _lookback_time_from = params.window.time_from
        _lookback_time_to = params.window.time_to
        while ii < max_lookback_iterations:
            ii += 1
            _lookback_time_to = _lookback_time_from
            _lookback_time_from = _lookback_time_from - lookback_window

            _telem_lookback = pd.DataFrame(
                ReadTelemetryForTripAndTime(
                    ReadTelemParams(
                        time_from=_lookback_time_from,
                        time_to=_lookback_time_to,
                        trip_id=trip_id,
                    ),
                    conn,
                )
            )

            if _telem_lookback.empty:  # then no more data
                break
            _telem_lookback.sort_values("time", ascending=True)
            _telem_lookback.reset_index(inplace=True, drop=True)

            # if all true, then have not looked back far enough
            if _telem_lookback[tgt_column].all():
                df = pd.concat([_telem_lookback, df])
                df.reset_index(inplace=True, drop=True)
                continue
            else:
                # ground previous groups as they would have already been captured
                _idxMin = _telem_lookback[tgt_column][::-1].idxmin()
                _telem_lookback.loc[
                    : int(_idxMin),
                    tgt_column,
                ] = False
                df = pd.concat([_telem_lookback, df])
                df.reset_index(inplace=True, drop=True)
                break

    # strip out leading false elements
    _idxMax = df[tgt_column].idxmax()
    df = df[int(_idxMax) :].reset_index(drop=True)

    # find chunks where the brake is applied - via finite automata
    in_window = False
    start_idx = 0
    windows_emitted = 0
    windows = []
    for ii, row in df.iterrows():
        if row[tgt_column] and not in_window:
            in_window = True
            start_idx = ii
            continue
        if not row[tgt_column] and in_window:
            in_window = False
            windows_emitted += 1

            # emit the window
            start_time = df.loc[start_idx, time_column]
            end_time = df.loc[ii - 1, time_column]

            # Convert to datetime if needed and get timestamp
            if isinstance(start_time, dt.datetime):
                start_timestamp = start_time.timestamp()
            else:
                # Convert pandas timestamp or other types to datetime
                start_timestamp = pd.to_datetime(start_time).timestamp()

            if isinstance(end_time, dt.datetime):
                end_timestamp = end_time.timestamp()
            else:
                # Convert pandas timestamp or other types to datetime
                end_timestamp = pd.to_datetime(end_time).timestamp()

            windows.append(
                EmitWindow(
                    Window(
                        time_from=dt.datetime.fromtimestamp(start_timestamp),
                        time_to=dt.datetime.fromtimestamp(end_timestamp),
                        name=emitting_window.name,
                        version=emitting_window.version,
                        origin=origin,
                        metadata={"trip_id": trip_id},
                    )
                )
            )
    return windows_emitted


# --- Find whether a trip is ongoing ---
@proc.algorithm("FindActiveBusses", "1.0.0", EveryMinute)
def FindActiveBuses(params: ExecutionParams) -> ValueResult:
    with db_pool.connection() as conn:
        # get telemetry for this window
        buses = ReadActiveBusses(
            ReadActiveBussesParams(
                time_from=params.window.time_from,
                time_to=params.window.time_to,
            ),
            conn,
        )
        count = 0
        for bus in buses:
            count += 1
            EmitWindow(
                Window(
                    time_from=params.time_from,
                    time_to=params.time_to,
                    name=EveryMinutePerTripPerBus.name,
                    version=EveryMinutePerTripPerBus.version,
                    origin="active_bus_emitter",
                    metadata={
                        "trip_id": bus.get("trip_id"),
                        "bus_id": bus.get("bus_id"),
                        "route_id": bus.get("route_id"),
                    },
                )
            )

    return ValueResult(count)


# @proc.algorithm("FindHaltBrakeWindows", "1.0.0", EveryMinute)
# def find_when_applying_halt_brake(params: ExecutionParams) -> ValueResult:
#     with db_pool.connection() as conn:
#         # get telemetry for this window
#         telem = ReadTelemetryForTripAndTime(
#             ReadTelemParams(
#                 time_from=params.window.time_from,
#                 time_to=params.window.time_to,
#                 trip_id=None,
#             ),
#             conn,
#         )
#         df = pd.DataFrame(telem)
#         if df.empty:
#             return ValueResult(0)
#
#         windowsEmitted = 0
#         for trip_id, trip_df in df.groupby("trip_id"):
#             # Ensure trip_id is an integer
#             trip_id_int = int(trip_id)
#
#             trip_df.sort_values("time", ascending=True, inplace=True)
#             trip_df.reset_index(inplace=True, drop=True)
#
#             windowsEmitted += _find_contiguous_chunks_and_emit(
#                 df=trip_df,
#                 tgt_column="status_halt_brake_is_active",
#                 time_column="time",
#                 trip_id=trip_id_int,
#                 params=params,
#                 emitting_window=HaltBrakeApplied,
#                 origin="halt_brake_emitter",
#                 conn=conn,
#             )
#
#     return ValueResult(windowsEmitted)
#
#
# @proc.algorithm("FindParkBrakeWindows", "1.0.0", EveryMinute)
# def find_when_applying_park_brake(params: ExecutionParams) -> ValueResult:
#     with db_pool.connection() as conn:
#         # get telemetry for this window
#         telem = ReadTelemetryForTripAndTime(
#             ReadTelemParams(
#                 time_from=params.window.time_from,
#                 time_to=params.window.time_to,
#                 trip_id=None,
#             ),
#             conn,
#         )
#         df = pd.DataFrame(telem)
#         if df.empty:
#             return ValueResult(0)
#         windowsEmitted = 0
#         for trip_id, trip_df in df.groupby("trip_id"):
#             # Ensure trip_id is an integer
#             trip_id_int = int(trip_id)
#
#             trip_df.sort_values("time", ascending=True, inplace=True)
#             trip_df.reset_index(inplace=True, drop=True)
#
#             windowsEmitted += _find_contiguous_chunks_and_emit(
#                 df=trip_df,
#                 tgt_column="status_park_brake_is_active",
#                 time_column="time",
#                 trip_id=trip_id_int,
#                 params=params,
#                 emitting_window=ParkBrakeApplied,
#                 origin="park_brake_emitter",
#                 conn=conn,
#             )
#     return ValueResult(windowsEmitted)


# --- Temperature ---
@proc.algorithm("AmbientTemperature", "1.0.0", EveryMinutePerTripPerBus)
def ambient_temperature_per_minute(params: ExecutionParams) -> StructResult:
    with db_pool.connection() as conn:
        trip_id = params.window.metadata["trip_id"]
        if trip_id is None:
            raise Exception("Require trip_id as metadata to the window")

        telem = ReadTelemetryForTripAndTime(
            ReadTelemParams(
                time_from=params.window.time_from,
                time_to=params.window.time_to,
                trip_id=trip_id,
            ),
            conn,
        )
    df = pd.DataFrame(telem)
    median = df["temperature_ambient"].median()
    return StructResult(
        {
            "50p": median,
        }
    )


# --- Energy Efficiency ---
@proc.algorithm("EnergyEfficiencyPerMinute", "1.0.0", EveryMinutePerTripPerBus)
def energy_efficiency_per_minute(params: ExecutionParams) -> StructResult:
    with db_pool.connection() as conn:
        telem = ReadTelemetryForTripAndTime(
            ReadTelemParams(
                time_from=params.window.time_from,
                time_to=params.window.time_to,
                trip_id=None,
            ),
            conn,
        )

    df = pd.DataFrame(telem)
    if df.empty:
        return StructResult(
            {"kwh": None, "kwh_per_km": None, "kwh_per_passenger_km": None}
        )

    # Energy in kWh: power demand [kW] * time [h]
    df["energy_kwh"] = df["electric_power_demand"].fillna(0) / 3600.0  # 1s samples
    total_kwh = df["energy_kwh"].sum()

    # Distance travelled from odometry speed (m/s * 1s)
    df["dist_m"] = df["odometry_vehicle_speed"].fillna(0)
    df["dist_m"] = df["dist_m"] * 1.0  # one second
    total_km = df["dist_m"].sum() / 1000.0

    # Passenger-km
    passenger_km = (
        df["itcs_number_of_passengers"].fillna(0) * df["dist_m"]
    ).sum() / 1000.0

    return StructResult(
        {
            "kwh": total_kwh,
            "kwh_per_km": total_kwh / total_km if total_km > 0 else None,
            "kwh_per_passenger_km": total_kwh / passenger_km
            if passenger_km > 0
            else None,
        }
    )


# --- Service Efficiency ---
@proc.algorithm("ServiceEfficiencyPerMinute", "1.0.0", EveryMinutePerTripPerBus)
def service_efficiency_per_minute(params: ExecutionParams) -> StructResult:
    with db_pool.connection() as conn:
        telem = ReadTelemetryForTripAndTime(
            ReadTelemParams(
                time_from=params.window.time_from,
                time_to=params.window.time_to,
                trip_id=None,
            ),
            conn,
        )
    df = pd.DataFrame(telem)
    if df.empty:
        return StructResult({"dwell_time_s": None, "door_open_fraction": None})

    total_time = len(df)  # seconds
    dwell_time = df[
        (df["status_door_is_open"]) & (df["odometry_vehicle_speed"] < 0.1)
    ].shape[0]

    return StructResult(
        {
            "dwell_time_s": dwell_time,
            "door_open_fraction": dwell_time / total_time if total_time > 0 else None,
        }
    )


# --- Comfort & Safety ---
@proc.algorithm("ComfortAndSafetyPerMinute", "1.0.0", EveryMinute)
def comfort_and_safety_per_minute(params: ExecutionParams) -> StructResult:
    with db_pool.connection() as conn:
        telem = ReadTelemetryForTripAndTime(
            ReadTelemParams(
                time_from=params.window.time_from,
                time_to=params.window.time_to,
                trip_id=None,
            ),
            conn,
        )
    df = pd.DataFrame(telem)
    if df.empty or "odometry_vehicle_speed" not in df.columns:
        return StructResult({"mean_accel": None, "std_accel": None, "jerk_95p": None})

    # Approximate acceleration (m/s^2) from 1s sampling
    df["accel"] = df["odometry_vehicle_speed"].diff().fillna(0)
    # Jerk (rate of change of acceleration)
    df["jerk"] = df["accel"].diff().fillna(0)

    return StructResult(
        {
            "mean_accel": df["accel"].mean(),
            "std_accel": df["accel"].std(),
            "jerk_95p": df["jerk"].quantile(0.95),
        }
    )


# --- Asset Stress ---
@proc.algorithm("AssetStressPerMinute", "1.0.0", EveryMinutePerTripPerBus)
def asset_stress_per_minute(params: ExecutionParams) -> StructResult:
    with db_pool.connection() as conn:
        telem = ReadTelemetryForTripAndTime(
            ReadTelemParams(
                time_from=params.window.time_from,
                time_to=params.window.time_to,
                trip_id=None,
            ),
            conn,
        )

    df = pd.DataFrame(telem)
    if df.empty:
        return StructResult({"articulation_var": None, "brake_pressure_mean": None})

    return StructResult(
        {
            "articulation_var": df["odometry_articulation_angle"].var(),
            "brake_pressure_mean": df["traction_brake_pressure"].mean(),
        }
    )


if __name__ == "__main__":
    proc.Register()
    proc.Start()
