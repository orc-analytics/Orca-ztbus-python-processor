from orca_python import Processor, ExecutionParams, EmitWindow, Window, WindowType
from windows import EveryMinute, HaltBrakeApplied, ParkBrakeApplied
import datetime as dt
from queries import ReadTelemetryForTripAndTime, ReadTelemParams
from orca_python.main import pb
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
            if _telem_lookback["status_halt_brake_is_active"].all():
                df = pd.concat([_telem_lookback, df])
                df.reset_index(inplace=True, drop=True)
                continue
            else:
                # ground previous groups as they would have already been captured
                _telem_lookback.loc[
                    : _telem_lookback["status_halt_brake_is_active"][::-1].idxmin(),
                    "status_halt_brake_is_active",
                ] = False
                df = pd.concat([_telem_lookback, df])
                df.reset_index(inplace=True, drop=True)
                break

    # strip out leading false elements
    df = df[df["status_halt_brake_is_active"].idxmax() :].reset_index(drop=True)

    # find chunks where the brake is applied - via finite automota
    in_window = False
    start_idx = 0
    for ii, row in df.iterrows():
        if row[tgt_column] and not in_window:
            in_window = True
            start_idx = ii
            continue
        if not row[tgt_column] and in_window:
            in_window = False

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


@proc.algorithm("FindHaltBrakeWindows", "1.0.0", EveryMinute)
def find_when_applying_halt_brake(params: ExecutionParams) -> None:
    # get telemetry for this window
    telem = ReadTelemetryForTripAndTime(
        ReadTelemParams(
            time_from=dt.datetime.fromtimestamp(params.window.time_from),
            time_to=dt.datetime.fromtimestamp(params.window.time_to),
            trip_id=None,
        )
    )
    df = pd.DataFrame(telem)
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


@proc.algorithm("FindParkBrakeWindows", "1.0.0", EveryMinute)
def find_when_applying_park_brake(params: ExecutionParams) -> None:
    # get telemetry for this window
    telem = ReadTelemetryForTripAndTime(
        ReadTelemParams(
            time_from=dt.datetime.fromtimestamp(params.window.time_from),
            time_to=dt.datetime.fromtimestamp(params.window.time_to),
            trip_id=None,
        )
    )
    df = pd.DataFrame(telem)
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


@proc.algorithm("StubParkBrake", "1.0.0", ParkBrakeApplied)
def stub_park_break_algo(params: ExecutionParams) -> None:
    print("STUB PARK BRAKE")
    ...


@proc.algorithm("StubHaltBrake", "1.0.0", HaltBrakeApplied)
def stub_halt_brake_algo(params: ExecutionParams) -> None:
    print("STUB HALT BRAKE")
    ...


if __name__ == "__main__":
    proc.Register()

    start_time = dt.datetime(2021, 3, 9, 14, 15)
    end_time = start_time + dt.timedelta(seconds=60)
    window = pb.Window(
        time_from=int(start_time.timestamp()),
        time_to=int(end_time.timestamp()),
        window_type_name=EveryMinute.name,
        window_type_version=EveryMinute.version,
        origin="test",
    )
    params = ExecutionParams(window=window)
    find_when_applying_park_brake(params=params)
    find_when_applying_halt_brake(params=params)
