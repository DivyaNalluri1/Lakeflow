# ==============================================================
# SILVER LAYER - CLEANED & ENRICHED DATA
# --------------------------------------------------------------
# This layer takes raw Bronze streaming data and:
# Removes rescued data columns (system-captured invalid fields).
# Renames ingestion timestamps for clarity.
# Deduplicates records based on primary keys (customer_id / trip_id).
# Filters out null IDs to ensure data quality.
# Joins customers and trips to enrich trip data with customer details.
#  Adds a "full_name" column using a reusable UDF from utilities.py.
#
# Write mode in DLT:
# - silver_customers / silver_trips: append + update (merge) due to deduplication.
# - silver_trips_enriched: append + update (merge) because it depends on Silver sources.



import dlt
from pyspark.sql.functions import col
from utilities import utils # import the udf

# -----------------------------
# SILVER - CUSTOMERS
# -----------------------------
@dlt.table(
    comment="Cleaned customers data with deduplication"
)
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .drop("_rescued_data")
        .withColumnRenamed("ingestion_timestamp", "customer_ingestion_timestamp")
        .dropDuplicates(["customer_id"])
        .filter(col("customer_id").isNotNull())
    )

# -----------------------------
# SILVER - TRIPS
# -----------------------------
@dlt.table(
    comment="Cleaned trips data with deduplication"
)
def silver_trips():
    return (
        dlt.read_stream("bronze_trips")
        .drop("_rescued_data")
        .withColumnRenamed("ingestion_timestamp", "trip_ingestion_timestamp")
        .dropDuplicates(["trip_id"])
        .filter(col("trip_id").isNotNull())
    )

# -----------------------------
# SILVER - TRIPS ENRICHED
# -----------------------------
@dlt.table(
    comment="Joined trips with customer details and full name"
)
def silver_trips_enriched():
    trips_df = dlt.read("silver_trips")
    customers_df = dlt.read("silver_customers")

    enriched_df = (
        trips_df.join(customers_df, on="customer_id", how="left")
        .withColumn("full_name", utils.full_name(col("first_name"), col("last_name")))  # Apply UDF
    )

    return enriched_df
