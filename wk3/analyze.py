import polars as pl
import sys
from datetime import datetime
from time import perf_counter_ns

def main():

    colorDict = {"#FFA800": "gold",
                "#9C6926": "gold brown",
                "#FFB470": "tan",
                "#7EED56": "light green",
                "#00CC78": "green",
                "#515252": "gray",
                "#009EAA": "sky blue",
                "#493AC1": "purple",
                "#3690EA": "light blue",
                "#FF4500": "orange",
                "#00A368": "green",
                "#FFD635": "yellow",
                "#00CCC0": "sky blue",
                "#51E9F4": "sky blue",
                "#2450A4": "blue",
                "#D4D7D9": "white",
                "#FF99AA": "pink",
                "#B44AC0": "purple",
                "#DE107F": "magenta",
                "#FFF8B8": "light yellow",
                "#00756F": "turquoise",
                "#94B3FF": "light blue",
                "#FF3881": "pink",
                "#898D90": "light gray",
                "#BE0039": "red",
                "#FFFFFF": "white",
                "#6D001A": "dark red",
                "#E4ABFF": "light purple",
                "#000000": "black",
                "#811E9F" : "purple",
                "#6D482F": "brown",
                "#6A5CFF": "blue-purple"
    }

 
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
    
    df = pl.scan_parquet("2022_place_canvas_history.parquet")
    dfFiltered = df.filter((pl.col('timestamp') > startTime) & (pl.col('timestamp') < endTime))

    # Group by pixel_color, then find how many users placed that color
    colorRanking = dfFiltered.group_by('pixel_color')\
                    .agg(pl.col('user_id').n_unique())\
                    .sort("user_id", descending=True)\
                    .collect()

    # Filter df so we only end up with rows where the user placed 2 or more
    # pixels in the timeframe. Group by user_id and calculate time between pixels.
    # Consider it a new session if this time is greater than 900 seconds.
    # Group sessions together, sum up the times between pixels in each session
    # and find avg.
    session = dfFiltered.filter(pl.len().over("user_id") >= 2)\
            .sort('timestamp')\
            .group_by('user_id')\
            .agg(pl.col('timestamp').diff().dt.total_seconds().alias('diff'))\
            .explode(pl.col('diff'))\
            .with_columns((pl.col('diff').is_null() | pl.col('diff') > 900).fill_null(True).alias('newSession'))\
            .with_columns(pl.col("newSession").cum_sum().over("user_id").alias('sessionID'))\
            .group_by(['user_id', 'sessionID'])\
            .agg(pl.col('diff').sum().alias('sessionDuration'))\
            .collect()

    avgSessionLen = session.select(pl.col('sessionDuration').mean())

    # Group by user_id and see how long each group is to see how many pixels
    # a user placed during the timeframe
    pixelPlacement = dfFiltered.group_by('user_id').len().select([
        pl.col('len').quantile(0.50).alias('50th'), # 50th percentile
        pl.col('len').quantile(0.75).alias('75th'), # 75th percentile
        pl.col('len').quantile(0.90).alias('90th'), # 90th percentile
        pl.col('len').quantile(0.99).alias('99th')  # 99th percentile
    ]).collect()

    # Group by user_id to find first time the user placed a pixel and check
    # if that is during the timeframe
    firstPlacement = df.group_by('user_id')\
                    .agg(pl.col('timestamp').min().alias('firstTimestamp'))\
                    .filter((pl.col('firstTimestamp') > startTime) & (pl.col('firstTimestamp') < endTime))\
                    .collect()


    print(f"Timeframe: {startTimeStr} to {endTimeStr}")
    print("Ranking of Colors by Distinct Users:")
    for i in range(3):
        print(f"\t{i+1}. {colorDict[colorRanking.row(i)[0]]}: {colorRanking.row(i)[1]} users")
    print(f"Average Session Length: {avgSessionLen['sessionDuration'][0]:.2f} seconds")
    print("Percentiles of Number of Pixels Placed:")
    print(f"\t50th percentile: {int(pixelPlacement.row(0)[0])} pixels")
    print(f"\t75th percentile: {int(pixelPlacement.row(0)[1])} pixels")
    print(f"\t90th percentile: {int(pixelPlacement.row(0)[2])} pixels")
    print(f"\t99th percentile: {int(pixelPlacement.row(0)[3])} pixels")
    print(f"Count of First Time Users: {firstPlacement.height} users")
    stop = perf_counter_ns()
    print(f"Execution time: {(stop-start)/(10**9):.2f} seconds")

if __name__ == "__main__":
    main()