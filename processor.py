from orca_python import (
    Processor,
    ExecutionParams,
    EmitWindow,
    Window,
    WindowType,
    StructResult,
    NoneResult,
)
from windows import EveryMinute, HaltBrakeApplied, ParkBrakeApplied
import datetime as dt
from queries import ReadTelemetryForTripAndTime, ReadTelemParams
import pandas as pd

proc = Processor("ztbus_analyser")


def _find_contiguous_chunks_and_emit(
    df: pd.DataFrame,
    tgt_column: str,
    time_column: str,
    trip_id: int,
    params: ExecutionParams,
    emitting_window: WindowType,
    origin: str,
    lookback_window: int = 20,
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
                        time_from=dt.datetime.fromtimestamp(_lookback_time_from),
                        time_to=dt.datetime.fromtimestamp(_lookback_time_to),
                        trip_id=trip_id,
                    )
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
                    :int(_idxMin),
                    tgt_column,
                ] = False
                df = pd.concat([_telem_lookback, df])
                df.reset_index(inplace=True, drop=True)
                break

    # strip out leading false elements
    _idxMax = df[tgt_column].idxmax()
    df = df[int(_idxMax):].reset_index(drop=True)

    # find chunks where the brake is applied - via finite automata
    in_window = False
    start_idx = 0
    windows_emitted = 0
    for ii, row in df.iterrows():
        if row[tgt_column] and not in_window:
            in_window = True
            start_idx = ii
            continue
        if not row[tgt_column] and in_window:
            in_window = False
            windows_emitted += 1

            # emit the window
            EmitWindow(
                window=Window(
                    time_from=int(df.loc[start_idx, time_column].timestamp()),
                    time_to=int(df.loc[ii - 1, time_column].timestamp()),
                    name=emitting_window.name,
                    version=emitting_window.version,
                    origin=origin,
                    metadata={"trip_id": trip_id},
                )
            )
    return windows_emitted


@proc.algorithm("FindHaltBrakeWindows", "1.0.0", EveryMinute)
def find_when_applying_halt_brake(params: ExecutionParams) -> NoneResult:
    # get telemetry for this window
    telem = ReadTelemetryForTripAndTime(
        ReadTelemParams(
            time_from=dt.datetime.fromtimestamp(params.window.time_from),
            time_to=dt.datetime.fromtimestamp(params.window.time_to),
            trip_id=None,
        )
    )
    df = pd.DataFrame(telem)
    if df.empty:
        return NoneResult()
    for trip_id, trip_df in df.groupby("trip_id"):
        trip_df.sort_values("time", ascending=True, inplace=True)
        trip_df.reset_index(inplace=True, drop=True)

        _find_contiguous_chunks_and_emit(
            df=trip_df,
            tgt_column="status_halt_brake_is_active",
            time_column="time",
            trip_id=trip_id,
            params=params,
            emitting_window=HaltBrakeApplied,
            origin="halt_brake_emitter",
        )

    return NoneResult()


@proc.algorithm("FindParkBrakeWindows", "1.0.0", EveryMinute)
def find_when_applying_park_brake(params: ExecutionParams) -> NoneResult:
    # get telemetry for this window
    telem = ReadTelemetryForTripAndTime(
        ReadTelemParams(
            time_from=dt.datetime.fromtimestamp(params.window.time_from),
            time_to=dt.datetime.fromtimestamp(params.window.time_to),
            trip_id=None,
        )
    )
    df = pd.DataFrame(telem)
    if df.empty:
        return NoneResult()
    for trip_id, trip_df in df.groupby("trip_id"):
        trip_df.sort_values("time", ascending=True, inplace=True)
        trip_df.reset_index(inplace=True, drop=True)

        _find_contiguous_chunks_and_emit(
            df=trip_df,
            tgt_column="status_halt_brake_is_active",
            time_column="time",
            trip_id=trip_id,
            params=params,
            emitting_window=ParkBrakeApplied,
            origin="halt_brake_emitter",
        )
    return NoneResult()


# algorithms
@proc.algorithm("HaltBrakeAmbientTemperature", "1.0.0", HaltBrakeApplied)
def halt_brake_ambient_temperature(params: ExecutionParams) -> StructResult:
    trip_id = params.window.metadata["trip_id"]
    if trip_id is None:
        raise Exception("Require trip_id as metadata to the window")

    telem = ReadTelemetryForTripAndTime(
        ReadTelemParams(
            time_from=dt.datetime.fromtimestamp(params.window.time_from),
            time_to=dt.datetime.fromtimestamp(params.window.time_to),
            trip_id=trip_id,
        )
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
    trip_id = params.window.metadata["trip_id"]
    if trip_id is None:
        raise Exception("Require trip_id as metadata to the window")

    telem = ReadTelemetryForTripAndTime(
        ReadTelemParams(
            time_from=dt.datetime.fromtimestamp(params.window.time_from),
            time_to=dt.datetime.fromtimestamp(params.window.time_to),
            trip_id=trip_id,
        )
    )
    df = pd.DataFrame(telem)
    median = df["temperature_ambient"].median()
    return StructResult(
        {
            "50p": median,
        }
    )


def _helper(column: str, params: ExecutionParams) -> StructResult:
    trip_id = params.window.metadata["trip_id"]
    if trip_id is None:
        raise Exception("Require trip_id as metadata to the window")

    telem = ReadTelemetryForTripAndTime(
        ReadTelemParams(
            time_from=dt.datetime.fromtimestamp(params.window.time_from),
            time_to=dt.datetime.fromtimestamp(params.window.time_to),
            trip_id=trip_id,
        )
    )
    df = pd.DataFrame(telem)
    if df.empty:
        return StructResult(
            {
                "mean": None,
                "std": None,
                "min": None,
                "25p": None,
                "50p": None,
                "75p": None,
                "max": None,
            }
        )

    stats = df[column].describe()
    return StructResult(
        {
            "mean": stats["mean"],
            "std": stats["std"],
            "min": stats["min"],
            "25p": stats["25%"],
            "50p": stats["50%"],
            "75p": stats["75%"],
            "max": stats["max"],
        }
    )


@proc.algorithm("ElectricPowerDemandHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def electric_power_demand_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("electric_power_demand", params)


@proc.algorithm("TractionBrakePressureHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def traction_brake_pressure_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("traction_brake_pressure", params)


@proc.algorithm("TractionTractionForceHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def traction_traction_force_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("traction_traction_force", params)


@proc.algorithm("GnssAltitudeHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def gnss_altitude_halt_brake_stats(params: ExecutionParams) -> StructResult:
    return _helper("gnss_altitude", params)


@proc.algorithm("GnssCourseHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def gnss_course_halt_brake_stats(params: ExecutionParams) -> StructResult:
    return _helper("gnss_course", params)


@proc.algorithm("GnssLatitudeHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def gnss_latitude_halt_brake_stats(params: ExecutionParams) -> StructResult:
    return _helper("gnss_latitude", params)


@proc.algorithm("GnssLongitudeHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def gnss_longitude_halt_brake_stats(params: ExecutionParams) -> StructResult:
    return _helper("gnss_longitude", params)


@proc.algorithm("OdometryArticulationAngleHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_articulation_angle_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_articulation_angle", params)


@proc.algorithm("OdometrySteeringAngleHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_steering_angle_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_steering_angle", params)


@proc.algorithm("OdometryVehicleSpeedHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_vehicle_speed_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_vehicle_speed", params)


@proc.algorithm("OdometryWheelSpeedFlHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_wheel_speed_fl_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_fl", params)


@proc.algorithm("OdometryWheelSpeedFrHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_wheel_speed_fr_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_fr", params)


@proc.algorithm("OdometryWheelSpeedMlHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_wheel_speed_ml_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_ml", params)


@proc.algorithm("OdometryWheelSpeedMrHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_wheel_speed_mr_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_mr", params)


@proc.algorithm("OdometryWheelSpeedRlHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_wheel_speed_rl_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_rl", params)


@proc.algorithm("OdometryWheelSpeedRrHaltBrakeStats", "1.0.0", HaltBrakeApplied)
def odometry_wheel_speed_rr_halt_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_rr", params)


@proc.algorithm("ElectricPowerDemandParkBrakeStats", "1.0.0", ParkBrakeApplied)
def electric_power_demand_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("electric_power_demand", params)


@proc.algorithm("TractionBrakePressureParkBrakeStats", "1.0.0", ParkBrakeApplied)
def traction_brake_pressure_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("traction_brake_pressure", params)


@proc.algorithm("TractionTractionForceParkBrakeStats", "1.0.0", ParkBrakeApplied)
def traction_traction_force_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("traction_traction_force", params)


@proc.algorithm("GnssAltitudeParkBrakeStats", "1.0.0", ParkBrakeApplied)
def gnss_altitude_park_brake_stats(params: ExecutionParams) -> StructResult:
    return _helper("gnss_altitude", params)


@proc.algorithm("GnssCourseParkBrakeStats", "1.0.0", ParkBrakeApplied)
def gnss_course_park_brake_stats(params: ExecutionParams) -> StructResult:
    return _helper("gnss_course", params)


@proc.algorithm("GnssLatitudeParkBrakeStats", "1.0.0", ParkBrakeApplied)
def gnss_latitude_park_brake_stats(params: ExecutionParams) -> StructResult:
    return _helper("gnss_latitude", params)


@proc.algorithm("GnssLongitudeParkBrakeStats", "1.0.0", ParkBrakeApplied)
def gnss_longitude_park_brake_stats(params: ExecutionParams) -> StructResult:
    return _helper("gnss_longitude", params)


@proc.algorithm("OdometryArticulationAngleParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_articulation_angle_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_articulation_angle", params)


@proc.algorithm("OdometrySteeringAngleParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_steering_angle_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_steering_angle", params)


@proc.algorithm("OdometryVehicleSpeedParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_vehicle_speed_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_vehicle_speed", params)


@proc.algorithm("OdometryWheelSpeedFlParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_wheel_speed_fl_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_fl", params)


@proc.algorithm("OdometryWheelSpeedFrParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_wheel_speed_fr_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_fr", params)


@proc.algorithm("OdometryWheelSpeedMlParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_wheel_speed_ml_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_ml", params)


@proc.algorithm("OdometryWheelSpeedMrParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_wheel_speed_mr_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_mr", params)


@proc.algorithm("OdometryWheelSpeedRlParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_wheel_speed_rl_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_rl", params)


@proc.algorithm("OdometryWheelSpeedRrParkBrakeStats", "1.0.0", ParkBrakeApplied)
def odometry_wheel_speed_rr_park_brake_stats(
    params: ExecutionParams,
) -> StructResult:
    return _helper("odometry_wheel_speed_rr", params)
