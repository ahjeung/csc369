import duckdb 
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

    # Query
    result = duckdb.sql(f"""SELECT mode(pixel_color),
                        mode(coordinate)
                        FROM read_csv_auto('2022_place_canvas_history.csv')
                        WHERE timestamp >= '{startTime}'
                        AND timestamp < '{endTime}'""")
    
    result = result.fetchone()
    mostPlacedColor = result[0]
    mostPlacedCoordinate = result[1]
    
    stop = perf_counter_ns()
    print(f"Timeframe: {startTimeStr} to {endTimeStr}")
    print(f"Execution time: {(stop-start)/(10**9):.2f} seconds")
    print(f"Most placed color: {mostPlacedColor}")
    print(f"Most placed pixel location: {mostPlacedCoordinate}")

    return

if __name__ == "__main__":
    main()