import sys
import csv
from datetime import datetime
from time import perf_counter

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
        startTime = datetime.strptime(startTimeStr, dateFormat)
        endTime = datetime.strptime(endTimeStr, dateFormat)
    except: 
        print("Invalid date and time")
        return
    
    # Check if end time is after start time
    if (endTime <= startTime):
        print("endTime must be after startTime")
        return
    
    with open('2022_place_canvas_history.csv', mode='r') as rplaceDataFile:
        rplaceCsv = csv.reader(rplaceDataFile)
        header = next(rplaceCsv)
        dateFormat1 = "%Y-%m-%d %H:%M:%S.%f UTC"
        dateFormat2 = "%Y-%m-%d %H:%M:%S UTC"

        # colorDict is a dict with color (in hex) as keys and its count as values
        # coordinateDict is a dict with coordinate as keys and its count as values
        colorDict = {}
        coordinateDict = {}
        
        # Iterate row by row in csv file
        for row in rplaceCsv:
            # Convert timestamp to datetime
            try:
                time = datetime.strptime(row[0], dateFormat1)
            except:
                time = datetime.strptime(row[0], dateFormat2)

            # If time is within startTime and endTime, access its color and coordinate
            # in colorDict and coordinateDict and add 1 to its count 
            if ((time >= startTime) and (time <= endTime)):
                if (row[2] in colorDict):
                    colorDict[row[2]]+=1
                else:
                    colorDict[row[2]] = 1
                if (row[3] in coordinateDict):
                    coordinateDict[row[3]]+=1
                else:
                    coordinateDict[row[3]] = 1
    
        mostPlacedColor = max(colorDict, key=colorDict.get)
        mostPlacedLocation = max(coordinateDict, key=coordinateDict.get)
        stop = perf_counter()

        print(f"Timeframe: {startTimeStr} to {endTimeStr}")
        print(f"Execution time: {(stop-start)/60:.2f} minutes")
        print(f"Most placed color: {mostPlacedColor}")
        print(f"Most placed pixel location: {mostPlacedLocation}")

    return

if __name__ == "__main__":
    main()
