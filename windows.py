from orca_python import WindowType, MetadataField

trip_id = MetadataField(name="trip_id", description="Unique identifier of the trip")
bus_id = MetadataField(name="bus_id", description="Unique identifier of the bus")
route_id = MetadataField(name="route_id", description="Unique identifier for the route")

EveryMinute = WindowType(
    name="EveryMinute", version="1.0.0", description="Triggered every minute"
)

EveryMinutePerTripPerBus = WindowType(
    name="EveryMinutePerTripPerBus",
    version="1.0.0",
    description="Triggered every minute for an ongoing trip per bus",
    metadataFields=[trip_id, bus_id, route_id],
)

TripEnd = WindowType(
    name="TripEnd",
    version="1.1.0",
    description="Triggered when a trip ends",
    metadataFields=[trip_id, bus_id, route_id],
)

HaltBrakeApplied = WindowType(
    name="HaltBrakeApplied",
    version="2.1.0",
    description="When the halt brake is applied",
    metadataFields=[trip_id, bus_id, route_id],
)

ParkBrakeApplied = WindowType(
    name="ParkBrakeApplied",
    version="2.1.0",
    description="When the park brake is applied",
    metadataFields=[trip_id, bus_id, route_id],
)
