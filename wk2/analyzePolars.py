import polars as pl
import sys
from datetime import datetime
from time import perf_counter_ns

def main():

    start = perf_counter_ns()
    # Check number of command line arguments
    if len(sys.argv) != 5:
        print("usage: analyze.py YYYY-MM-DD HH YYYY-MM-DD HH")
        return
    
    # Try to convert command line arguments to datetime
    dateFormat = "%Y-%m-%d %H"
    startTimeStr = sys.argv[1] + " " + sys.argv[2]
    endTimeStr = sys.argv[3] + " " + sys.argv[4]
    try:
        startTime = datetime.strptime(startTimeStr, dateFormat)
        endTime = datetime.strptime(endTimeStr, dateFormat)
    except: 
        print("Invalid date and time")
        return
    
    # Check if end time is after start time
    if (endTime <= startTime):
        print("endTime must be after startTime")
        return
    

    # Convert timestamp string to datetime and filter by time
    dateFormats = ["%Y-%m-%d %H:%M:%S.%f UTC", "%Y-%m-%d %H:%M:%S UTC"]
    df = (pl.scan_csv('2022_place_canvas_history.csv')
        .with_columns(
            pl.coalesce(
                pl.col('timestamp').str.to_datetime(format=dateFormat, strict=False) for dateFormat in dateFormats
            )
        ).filter((pl.col('timestamp') > startTime) & (pl.col('timestamp') < endTime))
    )

    # Find most occuring color and coordinate
    result = df.select(
        [pl.col('pixel_color').mode(),
         pl.col('coordinate').mode()]
    ).collect(engine='streaming')
    
    mostPlacedColor = result['pixel_color'][0]
    mostPlacedCoordinate = result['coordinate'][0]

    stop = perf_counter_ns()
    print(f"Timeframe: {startTimeStr} to {endTimeStr}")
    print(f"Execution time: {(stop-start)/(10**9):.2f} seconds")
    print(f"Most placed color: {mostPlacedColor}")
    print(f"Most placed pixel location: {mostPlacedCoordinate}")

    return

if __name__ == "__main__":
    main()