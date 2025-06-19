from queries import ReadTelemetryForTripAndTime, ReadTelemParams, ReadTripsFromTripId, ReadTripsFromTripIdParams
import tqdm

def main():
    # results = ReadTelemetryForTripAndTime(ReadTelemParams(trip_id=1))
    # for row in tqdm.tqdm(results):
    #     print(row)
    #     print()
    results = ReadTripsFromTripId(ReadTripsFromTripIdParams(trip_id=43))
    print(results)

if __name__=="__main__":
    main()
