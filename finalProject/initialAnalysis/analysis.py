import polars as pl
import matplotlib.pyplot as plt

df = pl.scan_parquet('flightData23-25.parquet')

topHalfAirports = df.group_by('ORIGIN_AIRPORT_ID')

# Find rate of non-diverted, non-cancelled flights that departed/arrived late from each airport in dataset
depDelay = df.group_by('ORIGIN_AIRPORT_ID').agg(
    [pl.len().alias('numDepartures'),
     (pl.col('DEP_DEL_15') == True).sum().alias('numDepDelays')]
    ).filter(
        pl.col('numDepartures') > pl.col('numDepartures').median()
    ).with_columns(
        (pl.col('numDepDelays') / pl.col('numDepartures')).alias('depDelPercentage')
    ).sort('depDelPercentage', descending=True).collect() 

arrDelay = df.group_by('DEST_AIRPORT_ID').agg(
    [pl.len().alias('numArrivals'),
     (pl.col('ARR_DEL_15') == True).sum().alias('numArrDelays')]
    ).filter(
        pl.col('numArrivals') > pl.col('numArrivals').median()
    ).with_columns(
        (pl.col('numArrDelays') / pl.col('numArrivals')).alias('arrDelPercentage')
    ).sort('arrDelPercentage', descending=True).collect() 

# See if airports with more frequent departure delays have more frequent arrival delays
depArrDelay = depDelay.join(arrDelay, left_on='ORIGIN_AIRPORT_ID', right_on='DEST_AIRPORT_ID')
depDelayList = depArrDelay['depDelPercentage'].to_list()
arrDelayList = depArrDelay['arrDelPercentage'].to_list()
plt.scatter(x=depDelayList, y=arrDelayList)
plt.title('Departure and Arrival Delay Rates of Top 180 Busiest Airports')
plt.xlabel('Departure Delay Rate')
plt.ylabel('Arrival Delay Rate')
plt.savefig('depArrDelayRates.png')


delayPropagation = df.select(['TAIL_NUM', 'FL_DATE', 'DEP_TIME', 'ARR_DEL_15', 'DEP_DEL_15', 'ORIGIN_AIRPORT_ID'])\
    .sort(['TAIL_NUM', 'FL_DATE', 'DEP_TIME'])\
    .with_columns(
        pl.col('ARR_DEL_15')
        .shift(1)
        .over("TAIL_NUM")
        .alias("prevArrDel15")
    ).filter(pl.col("prevArrDel15").is_not_null())

# See how often if a plane arrives late, its next flight also departs late 
tailNumDelayPropagation = delayPropagation\
    .select([
        pl.col('prevArrDel15').sum().alias('numPrevDelay'), # Number of flights where previous flight arrived late
        ((pl.col('prevArrDel15')==True) & (pl.col('DEP_DEL_15')==True)).sum().alias('numPrevAndCurDelay')])\
    .with_columns((pl.col('numPrevAndCurDelay') / pl.col('numPrevDelay')).alias("cuz")).collect()
print(tailNumDelayPropagation)

# See which airport has the most occurences (by rate) of a plane arriving on time but its next flight departs late
airportDelayPropagation = delayPropagation.with_columns(
        ((pl.col('prevArrDel15') == False) & (pl.col('DEP_DEL_15') == True)).alias('sourceDelay'))\
    .group_by('ORIGIN_AIRPORT_ID')\
    .agg([
        pl.len().alias('numDepartures'),
        pl.col('sourceDelay').sum().alias('numSourceDelay')])\
    .with_columns((pl.col('numSourceDelay') / pl.col('numDepartures')).alias('sourceDelayPercentage'))\
    .sort('sourceDelayPercentage', descending=True)\
    .filter(pl.col('numDepartures') > pl.col('numDepartures').median())\
    .collect()

print(airportDelayPropagation)
