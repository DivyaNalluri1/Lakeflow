import dlt
from pyspark.sql.functions import col, hour, count, avg

@dlt.table(
    comment="City-wise trip volumes"
)
def gold_city_trip_volumes1():
    return (
        dlt.read("silver_trips_enriched")
        .groupBy("city")
        .agg(count("*").alias("total_trips"))
    )

@dlt.table(
    comment="Peak hour trip volumes"
)
def gold_peak_hours1():
    return (
        dlt.read("silver_trips_enriched")
        .withColumn("pickup_hour", hour(col("pickup_time")))
        .groupBy("pickup_hour")
        .agg(count("*").alias("trips_in_hour"))
    )

@dlt.table(
    comment="Customer activity trends"
)
def gold_customer_activity1():
    return (
        dlt.read("silver_trips_enriched")
        .groupBy("customer_id", "first_name", "last_name")
        .agg(count("*").alias("total_trips"), avg("fare_amount").alias("avg_fare"))
    )