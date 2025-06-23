from orca_python import Processor, ExecutionParams
from windows import EveryMinute
import datetime as dt
from windows import AppliedBreaks, EveryMinute
from queries import ReadTelemetryForTripAndTime, ReadTelemParams

proc = Processor("ztbus_analyser")

@proc.algorithm("FindBreakWindows", "1.0.0", EveryMinute)
def find_when_breaking_windows(params: ExecutionParams) -> None:
    # get telemetry for this window
    telem = ReadTelemetryForTripAndTime(ReadTelemParams(
        time_from=dt.datetime.fromtimestamp(params.window.time_from), 
        time_to = dt.fromtimestamp(params.window.time_to)
    ))
    print(telem)

    ...
