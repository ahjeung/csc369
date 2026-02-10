import duckdb
import datetime
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import matplotlib.colors as clr

# Plot first pixel placed at each coordinate
query = """
    SELECT DISTINCT ON (x, y) x, y, pixel_color, timestamp
    FROM '2022_place_canvas_history.parquet'
    ORDER BY x, y, timestamp ASC
"""

df = duckdb.sql(query).to_df()
print(df)

canvas = np.ones((2000, 2000, 3))
for row in df.itertuples():
    canvas[row.y, row.x] = clr.to_rgb(row.pixel_color)

plt.figure(figsize=(10,10))
plt.imshow(canvas)
plt.axis('off')
plt.title("2022 r/place First Placed Pixels")
plt.savefig("rplaceFirstPixels.png", dpi=200)

# Plot pixels placed within 10 minutes of each start time. There are 3 startTimes 
# because r/place expanded on 4/2 and 4/3. 

startTime1 = datetime.datetime(year=2022, month=4, day=1, hour=13, minute=0, second=0)
startTime2 = datetime.datetime(year=2022, month=4, day=2, hour=16, minute=25, second=0)
startTime3 = datetime.datetime(year=2022, month=4, day=3, hour=19, minute=0, second=0)
df = pl.scan_parquet('2022_place_canvas_history.parquet')
df = df.filter(
    (pl.col("timestamp") <= startTime1 + pl.duration(minutes=10)) | # started 1000x1000 on 4/1
    ((pl.col('x') > 1000) & (pl.col('timestamp') <= (startTime2 + pl.duration(minutes=10)))) | # expanded to 2000x1000 on 4/2
    ((pl.col('y') > 1000) & (pl.col('timestamp') <= (startTime3 + pl.duration(minutes=10)))) # expanded to 2000x2000 on 4/3
).collect()

canvas = np.ones((2000, 2000, 3))
for x, y in df.select(['x', 'y']).iter_rows():
    canvas[y, x] = [1.0, 0, 0]

plt.figure(figsize=(10,10))
plt.imshow(canvas)
plt.axis('off')
plt.title("Pixels Placed Within 10 Minutes")
plt.savefig("pixelsPlacedWithinTenMinutes.png", dpi=200)
