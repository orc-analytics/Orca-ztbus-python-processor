from queries import ReadTelemetryForTripAndTime, ReadTelemParams
import tqdm

def main():
    results = ReadTelemetryForTripAndTime(ReadTelemParams(trip_id=1))
    for row in tqdm.tqdm(results):
        print(row)
        print()

if __name__=="__main__":
    main()
