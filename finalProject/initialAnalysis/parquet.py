import polars as pl
from datetime import datetime
df = pl.scan_csv('flightData23-25.csv')

# Probably don't need this many columns in df
df = df.select(['YEAR', 'MONTH', 'DAY_OF_MONTH', 'DAY_OF_WEEK', 'OP_UNIQUE_CARRIER', 'TAIL_NUM', 'OP_CARRIER_FL_NUM', 'ORIGIN_AIRPORT_ID', 'ORIGIN_CITY_MARKET_ID', 
                'ORIGIN', 'DEST_AIRPORT_ID', 'DEST_CITY_MARKET_ID', 'DEST', 'CRS_DEP_TIME', 'DEP_TIME', 'DEP_DELAY', 'TAXI_OUT', 'WHEELS_OFF', 
                'WHEELS_ON', 'TAXI_IN', 'CRS_ARR_TIME', 'ARR_TIME', 'ARR_DELAY', 'CANCELLED', 'CANCELLATION_CODE', 'DIVERTED', 
                'CRS_ELAPSED_TIME', 'ACTUAL_ELAPSED_TIME', 'AIR_TIME', 'DISTANCE', 'CARRIER_DELAY', 'WEATHER_DELAY', 'NAS_DELAY', 'SECURITY_DELAY', 'LATE_AIRCRAFT_DELAY',
                'FIRST_DEP_TIME', 'TOTAL_ADD_GTIME', 'LONGEST_ADD_GTIME', 'DIV_AIRPORT_LANDINGS', 'DIV_REACHED_DEST', 'DIV_ACTUAL_ELAPSED_TIME'
                ])

# Convert YEAR, MONTH, DAY_OF_MONTH to date
df = df.with_columns(pl.date(pl.col('YEAR'), pl.col('MONTH'), pl.col('DAY_OF_MONTH')).alias('FL_DATE'))

# Convert time in str to time
timeFormats = ["%H%M", "%-H%M"]
df = df.with_columns(pl.coalesce(pl.col('DEP_TIME').cast(str).str.strptime(pl.Time, timeFormat, strict=False) for timeFormat in timeFormats).alias('DEP_TIME'))
df = df.with_columns(pl.coalesce(pl.col('ARR_TIME').cast(str).str.strptime(pl.Time, timeFormat, strict=False) for timeFormat in timeFormats).alias('ARR_TIME'))
df = df.drop(['YEAR', 'MONTH', 'DAY_OF_MONTH'])

# Filter our diverted and cancelled flights
df = df.with_columns(pl.when(pl.col('CANCELLED') == 1).then(pl.lit(True)).otherwise(pl.lit(False)).alias('CANCELLED'))
df = df.with_columns(pl.when(pl.col('DIVERTED') == 1).then(pl.lit(True)).otherwise(pl.lit(False)).alias('DIVERTED'))
df = df.filter((pl.col('DIVERTED') == False) & (pl.col('CANCELLED') == False))

# Make new column that is True when flight departed or arrived > 15 min past schedule. 
df = df.with_columns(pl.when(pl.col('DEP_DELAY') > 15).then(pl.lit(True)).otherwise(pl.lit(False)).alias('DEP_DEL_15'))
df = df.with_columns(pl.when(pl.col('ARR_DELAY') > 15).then(pl.lit(True)).otherwise(pl.lit(False)).alias('ARR_DEL_15'))

df = df.with_columns(pl.when(pl.col('CANCELLED') == 1).then(pl.lit(True)).otherwise(pl.lit(False)).alias('CANCELLED'))
df = df.with_columns(pl.when(pl.col('DIVERTED') == 1).then(pl.lit(True)).otherwise(pl.lit(False)).alias('DIVERTED'))


# write parquet
df.sink_parquet("flightData23-25.parquet")

