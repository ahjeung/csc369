import polars as pl

df = pl.scan_csv("2022_place_canvas_history.csv")

# hash user_id column to int64
df = df.with_columns(pl.col("user_id").hash())

# convert timestamp string to datetime
dateFormats = ["%Y-%m-%d %H:%M:%S.%f UTC", "%Y-%m-%d %H:%M:%S UTC"]
df = df.with_columns(pl.coalesce(
                pl.col('timestamp').str.to_datetime(format=dateFormat, strict=False) for dateFormat in dateFormats
            ))

# write parquet
df.sink_parquet("2022_place_canvas_history.parquet")