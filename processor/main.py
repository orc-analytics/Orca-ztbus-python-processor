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
from queries import ReadTelemetryForTripAndTime, ReadTelemParams
import pandas as pd
from db import db_pool
from psycopg2.extensions import connection as PGConnection

from windows import EveryMinute, HaltBrakeApplied, ParkBrakeApplied

proc = Processor("analyser")


def get_db_conn() -> PGConnection:
    with db_pool.connection() as conn:
        yield conn


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
):
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
            windows.append(
                EmitWindow(
                    Window(
                        time_from=dt.datetime.fromtimestamp(
                            df.loc[start_idx, time_column].timestamp()
                        ),
                        time_to=dt.datetime.fromtimestamp(
                            df.loc[ii - 1, time_column].timestamp()
                        ),
                        name=emitting_window.name,
                        version=emitting_window.version,
                        origin=origin,
                        metadata={"trip_id": trip_id},
                    )
                )
            )
    return windows_emitted


@proc.algorithm("FindHaltBrakeWindows", "1.0.0", EveryMinute)
def find_when_applying_halt_brake(params: ExecutionParams) -> ValueResult:
    conn = get_db_conn()

    # get telemetry for this window
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
        return ValueResult(0)

    windowsEmitted = 0
    for trip_id, trip_df in df.groupby("trip_id"):
        trip_df.sort_values("time", ascending=True, inplace=True)
        trip_df.reset_index(inplace=True, drop=True)

        windowsEmitted += _find_contiguous_chunks_and_emit(
            df=trip_df,
            tgt_column="status_halt_brake_is_active",
            time_column="time",
            trip_id=trip_id,
            params=params,
            emitting_window=HaltBrakeApplied,
            origin="halt_brake_emitter",
            conn=conn,
        )

    return ValueResult(windowsEmitted)


@proc.algorithm("FindParkBrakeWindows", "1.0.0", EveryMinute)
def find_when_applying_park_brake(params: ExecutionParams) -> ValueResult:
    conn = get_db_conn()

    # get telemetry for this window
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
        return ValueResult(0)
    windowsEmitted = 0
    for trip_id, trip_df in df.groupby("trip_id"):
        trip_df.sort_values("time", ascending=True, inplace=True)
        trip_df.reset_index(inplace=True, drop=True)

        windowsEmitted += _find_contiguous_chunks_and_emit(
            df=trip_df,
            tgt_column="status_park_brake_is_active",
            time_column="time",
            trip_id=trip_id,
            params=params,
            emitting_window=ParkBrakeApplied,
            origin="park_brake_emitter",
            conn=conn,
        )
    return ValueResult(windowsEmitted)


# --- Temperature ---
@proc.algorithm("HaltBrakeAmbientTemperature", "1.0.0", HaltBrakeApplied)
def halt_brake_ambient_temperature(params: ExecutionParams) -> StructResult:
    conn = get_db_conn()

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


@proc.algorithm("ParkBrakeAmbientTemperature", "1.0.0", ParkBrakeApplied)
def park_brake_ambient_temperature(params: ExecutionParams) -> StructResult:
    conn = get_db_conn()

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
@proc.algorithm("EnergyEfficiencyPerMinute", "1.0.0", EveryMinute)
def energy_efficiency_per_minute(params: ExecutionParams) -> StructResult:
    conn = get_db_conn()

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
@proc.algorithm("ServiceEfficiencyPerMinute", "1.0.0", EveryMinute)
def service_efficiency_per_minute(params: ExecutionParams) -> StructResult:
    conn = get_db_conn()

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
    conn = get_db_conn()

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
@proc.algorithm("AssetStressPerMinute", "1.0.0", EveryMinute)
def asset_stress_per_minute(params: ExecutionParams) -> StructResult:
    conn = get_db_conn()

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
