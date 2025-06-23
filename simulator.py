import datetime as dt
import time
from orca_python import EmitWindow, Window
from queries import CreateSimlogEntryParams, CreateSimLogEntry, ReadLatestSimlog
from typing import TypedDict
import schedule


EveryMinute = WindowType(name="EveryMinute", version="1.0.0", origin="simulator")

def FindAndEmitMinuteWindow():

    simLog = ReadLatestSimlog()

    if len(simLog) == 0:
        # earliest time where both busses are active: 2021-03-09 14:15:05.000
        start_time = dt.datetime(2021,3,9,14,15)
        end_time = start_time + dt.timedelta(seconds=60)
        CreateSimLogEntry(CreateSimlogEntryParams(
            start_time=start_time,
            end_time=end_time
        ))
    else:
        end_time = simLog.get("end_time") 
        start_time = end_time
        end_time = end_time + dt.timedelta(seconds=60)
    
    EmitWindow(Window(time_from=start_time, time_to=end_time, **EveryMinute))

def Simulate():
    schedule.every(1).minute.do(FindAndEmitMinuteWindow)
    while True:
        schedule.run_pending()
        time.sleep(1)
