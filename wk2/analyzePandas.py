import sys
from datetime import datetime, timezone
from time import perf_counter
import pandas as pd

def main():

    start = perf_counter()
    # Check number of command line arguments
    if len(sys.argv) != 5:
        print("usage: analyze.py YYYY-MM-DD HH YYYY-MM-DD HH")
        return
    
    # Try to convert command line arguments to datetime
    dateFormat = "%Y-%m-%d %H"
    startTimeStr = sys.argv[1] + " " + sys.argv[2]
    endTimeStr = sys.argv[3] + " " + sys.argv[4]
    try:
        startTime = datetime.strptime(startTimeStr, dateFormat).replace(tzinfo=timezone.utc)
        endTime = datetime.strptime(endTimeStr, dateFormat).replace(tzinfo=timezone.utc)
    except: 
        print("Invalid date and time")
        return
    
    # Check if end time is after start time
    if (endTime <= startTime):
        print("endTime must be after startTime")
        return
    
    colorCount = pd.Series(dtype='int64')
    coordinateCount = pd.Series(dtype='int64')

    # Read csv in chunks of 1M rows
    for chunk in pd.read_csv("2022_place_canvas_history.csv", chunksize=10**6, usecols=['timestamp', 'pixel_color', 'coordinate']):
        
        # Convert timestamp from str to datetime
        chunk['timestamp'] = pd.to_datetime(chunk['timestamp'], format="mixed")

        # Only keep rows within the timeframe
        filtered = chunk[(chunk['timestamp'] > startTime) & (chunk['timestamp'] < endTime)]

        # Count colors and coordinates
        chunkColorCount = filtered['pixel_color'].value_counts()
        chunkCoordinateCount = filtered['coordinate'].value_counts()

        # Add to total colorCount and coordinateCount series
        colorCount = colorCount.add(chunkColorCount, fill_value=0)
        coordinateCount = coordinateCount.add(chunkCoordinateCount, fill_value=0)

    stop = perf_counter()
    print(f"Timeframe: {startTimeStr} to {endTimeStr}")
    print(f"Execution time: {(stop-start)/60:.2f} minutes")
    print(f"Most placed color: {colorCount.idxmax()}")
    print(f"Most placed pixel location: {coordinateCount.idxmax()}")


if __name__ == "__main__":
    main()