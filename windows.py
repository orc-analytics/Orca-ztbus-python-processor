from typing import TypedDict


class WindowType(TypedDict):
    name: str
    version: str
    origin: str


EveryMinute = WindowType(name="EveryMinute", version="1.0.0", origin="simulator")
