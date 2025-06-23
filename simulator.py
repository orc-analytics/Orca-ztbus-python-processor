import datetime as dt
import time
from orca_python import EmitWindow, Window
from queries import CreateSimlogEntryParams, CreateSimLogEntry, ReadLatestSimlog, CreateSimLogsTable
from windows import EveryMinute
import schedule


def FindAndEmitMinuteWindow():
    simLog = ReadLatestSimlog()

    if len(simLog) == 0:
        # earliest time where both busses are active: 2021-03-09 14:15:05.000
        start_time = dt.datetime(2021, 3, 9, 14, 15)
        end_time = start_time + dt.timedelta(seconds=60)
        CreateSimLogEntry(
            CreateSimlogEntryParams(start_time=start_time, end_time=end_time)
        )
    else:
        end_time = simLog.get("end_time")
        start_time = end_time
        end_time = end_time + dt.timedelta(seconds=60)
        CreateSimLogEntry(
            CreateSimlogEntryParams(start_time=start_time, end_time=end_time)
        )
    print("Emitted window")
    EmitWindow(Window(time_from=int(start_time.timestamp()), time_to=int(end_time.timestamp()), name=EveryMinute.name, version=EveryMinute.version, origin="simulator"))


def Simulate():
    schedule.every(1).second.do(FindAndEmitMinuteWindow)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__=="__main__":
    CreateSimLogsTable()
    Simulate()


