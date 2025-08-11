import dlt
from pyspark.sql.functions import *

# Bronze: Customers raw
@dlt.table(
    comment="Raw streaming customer data from UC Volumes"
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/lakeflow/divya/volume/raw/customer/")
        .withColumn("ingestion_timestamp", current_timestamp())
    )

# Bronze: Trips raw
@dlt.table(
    comment="Raw streaming trip data from UC Volumes"
)
def bronze_trips():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load("/Volumes/lakeflow/divya/volume/raw/trips/")
        .withColumn("ingestion_timestamp", current_timestamp())
    )
