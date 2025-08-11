import dlt
from pyspark.sql.functions import col

# -----------------------------
# SILVER - CUSTOMERS
# -----------------------------
@dlt.table(
    comment="Cleaned customers data with deduplication"
)
def silver_customers():
    return (
        dlt.read_stream("bronze_customers")
        .drop("_rescued_data")  # Drop rescued data column
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
        .drop("_rescued_data")  # Drop rescued data column
        .withColumnRenamed("ingestion_timestamp", "trip_ingestion_timestamp")
        .dropDuplicates(["trip_id"])
        .filter(col("trip_id").isNotNull())
    )

# -----------------------------
# SILVER - TRIPS ENRICHED
# -----------------------------
@dlt.table(
    comment="Joined trips with customer details"
)
def silver_trips_enriched():
    trips_df = dlt.read("silver_trips")
    customers_df = dlt.read("silver_customers")

    enriched_df = (
        trips_df.join(customers_df, on="customer_id", how="left")
    )

    return enriched_df
