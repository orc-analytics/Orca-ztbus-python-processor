from orca_python import WindowType

EveryMinute = WindowType(
    name="EveryMinute", version="1.0.0", description="Triggered every minute"
)

HaltBrakeApplied = WindowType(
    name="HaltBrakeApplied",
    version="1.0.0",
    description="When the halt brake is applied",
)

ParkBrakeApplied = WindowType(
    name="ParkBrakeApplied",
    version="1.0.0",
    description="When the park brake is applied",
)
