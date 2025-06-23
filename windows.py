from typing import TypedDict

class WindowType(TypedDict):
    name: string
    version: string 
    origin: simulator

EveryMinute = WindowType(name="EveryMinute", version="1.0.0", origin="simulator")
