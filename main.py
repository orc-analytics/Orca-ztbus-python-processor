from queries import ReadTelemetryForTripAndTime, ReadTelemParams, ReadTripsFromTripId, ReadTripsFromTripIdParams, CreateSimLogsTable, CreateSimlogEntryParams, CreateSimLogEntry, ReadLatestSimlog
import datetime as dt
def main():
    # results = ReadTelemetryForTripAndTime(ReadTelemParams(trip_id=1))
    # for row in tqdm.tqdm(results):
    #     print(row)
    #     print()
    results = ReadTripsFromTripId(ReadTripsFromTripIdParams(trip_id=43))
    print(results)
    CreateSimLogsTable()
    CreateSimLogEntry(CreateSimlogEntryParams(
        start_time = dt.datetime.utcnow() - dt.timedelta(seconds=60),
        end_time = dt.datetime.utcnow()
    ))
    simlog = ReadLatestSimlog()
    print(simlog)


if __name__=="__main__":
    main()
