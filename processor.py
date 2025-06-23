from orca_python import Processor, ExecutionParams
from windows import EveryMinute
import time
from windows import AppliedBreaks, EveryMinute
from quer
proc = Processor("ztbus_analyser")

@proc.algorithm("FindBreakWindows", "1.0.0", EveryMinute)
def find_when_breaking_windows(params: ExecutionParams) -> None:
    # get telemetry for this window

    ...
