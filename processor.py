from orca_python import Processor
from windows import EveryMinute
import time
from windows import AppliedBreaks, EveryMinute

proc = Processor("ztbus_analyser")

@proc.algorithm("FindBreakWindows", "1.0.0", EveryMinute)
def find_when_breaking_windows(*args, **kwargs) -> None:
    print("KWARGS")
    print(kwargs)
    print("ARGS")
    print(args)
