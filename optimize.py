from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f, types as t
from pyspark.sql import SparkSession
import requests

taxiPath = "hdfs:///tmp/data/emr/nyc-taxi/taxi-data/output/section2/json/"
taxiLookupPath = "hdfs:///tmp/data/emr/nyc-taxi/zone-lookup/output/section2/json/"
spark = None

def write_sorted_parquet(inputDF): 
    (inputDF.orderBy('passenger_count', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount', 'tip_amount', 'tpep_dropoff_datetime', 'tpep_pickup_datetime')
        .coalesce(5)     
        .write
        .mode("overwrite")
        .partitionBy('pickup_month')
        .parquet("hdfs:///tmp/data/emr/nyc-taxi/taxi-data/output/section3/sorted"))
    
def get_taxi_df():
    taxiDF = spark.read.parquet("hdfs:///tmp/data/emr/nyc-taxi/taxi-data/output/section3/sorted")
    taxi_lookup = spark.read.json(taxiLookupPath)
    taxi_filtered = (taxiDF
     .filter(taxiDF.pickup_datetime.isNotNull())
     .filter(taxiDF.dropoff_datetime.isNotNull()))
                     
    groupDF = taxi_filtered.join(taxi_lookup.select("Borough", "LocationID"), taxi_filtered.PULocationID == taxi_lookup.LocationID)
    return groupDF

def get_zip_code_mapping_df():
    zipReadDF = spark.read.option('header', True).csv("s3://data-scale-oreilly/data/borough-zip-mapping/ny-zip-codes.csv")
    returnZipDF = zipReadDF.select('Borough', 'Neighborhood', f.explode(f.split('ZIP Codes', ',')).alias('zip'))
    return returnZipDF

    joinAirDF = airQualityDF.withColumn('air_day', f.date_format("dateObserved", "yyyyMMdd")).withColumnRenamed("zip", "air_zip")
    zipDF = zipDF.withColumn('zipBorough', f.col('Borough'))
    taxiZipDF = taxiDF.join(zipDF.select('zip', 'ZipBorough'), zipDF.zipBorough == taxiDF.Borough)
    joinTaxiDF = (taxiZipDF.withColumn("pickup_day", f.date_format("pickup_datetime", "yyyyMMdd"))
                        .withColumn("pickup_month", f.date_format("pickup_datetime", "yyyyMM")))
    joinCondition = [joinTaxiDF.pickup_day == joinAirDF.air_day, joinTaxiDF.zip == joinAirDF.air_zip]
    joinedAirDF = joinTaxiDF.join(joinAirDF.select('categoryNumber', 'air_day', 'air_zip'), joinCondition)
    aggDF = (joinedAirDF.groupBy('pickup_month', 'pickup_day', 'zip')
                        .agg(f.count('pickup_day').alias('count_rides'), f.avg('categoryNumber').alias('avg_cat')))
    
    win = Window.partitionBy("zip", "pickup_month").orderBy(f.desc("avg_cat", 'count_rides'))
    aggDF = aggDF.withColumn("row_num", f.row_number().over(win)).where("row_num >= 10")

    return aggDF

def get_air_quality_df(zipDF):
    zipList = [x[0] for x in zipDF.select('zip').collect()]
    airQualitySchema = t.StructType([
        t.StructField("dateObserved", t.DateType(), True),
        t.StructField("hourObserved", t.IntegerType(), True),
        t.StructField("localTimeZone", t.StringType(), True),
        t.StructField("reportingArea", t.StringType(), True),
        t.StructField("stateCode", t.StringType(), True),
        t.StructField("latitude", t.IntegerType(), True),
        t.StructField("longitude", t.IntegerType(), True),
        t.StructField("parameterName", t.StringType(), True),
        t.StructField("aqi", t.IntegerType(), True),
        t.StructField("categorNumber", t.IntegerType(), True),
        t.StructField("categoryName", t.StringType(), True),
        t.StructField("zip", t.IntegerType(), True),
        ])
    
    airDF = spark.createDataFrame([], airQualitySchema)

    for zipCode in zipList:
        apiPath = f"http://www.airnowapi.org/aq/observation/zipCode/historical/?format=json&zipCode={zipCode}&distance=25&API_KEY=B79C872D-FC7A-4037-BFA6-F064276F2EB3"
        request = requests.get(apiPath)
        print(request)
        print(request.json())
        requestDF = spark.createDataFrame(request.json(), airQualitySchema).withColumn('zip', f.lit(zipCode))
        returnAirDF = airDF.unionAll(requestDF)
    
    return returnAirDF

def save_data(df, output_path, data_format, write_mode):
    print(f"Saving data to {output_path}")
    (df.write
    .mode(write_mode)
    .format(data_format)
    .save(output_path))
    
if __name__ == '__main__':
    spark = SparkSession.builder.appName("OptimizeJob").getOrCreate()
    inputDF = (spark
              .read
              .json(taxiPath)
              .withColumn("pickup_month", f.date_format("pickup_datetime", "yyyyMM")))
    write_sorted_parquet(inputDF)
    taxi_data = get_taxi_df()
    zip_mapping = get_zip_code_mapping_df()
    airQualityDF = get_air_quality_df(zip_mapping)
    hottest_days = calculate_hottest_days(taxi_data, airQualityDF)
    
    # Probably want to change this, just testing for now
    save_data(taxi_data, "s3://data-scale-oreilly/data/EMR/taxi_data", "parquet", "overwrite")
    save_data(zip_mapping, "s3://data-scale-oreilly/data/EMR/zip_mapping", "parquet", "overwrite")
    save_data(hottest_days, "s3://data-scale-oreilly/data/EMR/hottest_days", "csv", "overwrite")
    