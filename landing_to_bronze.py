import dlt
from pyspark.sql.functions import col, to_date, current_timestamp, lit, max

CUSTOMERS_VOLUME_PATH = "/Volumes/lakeflow_demo/landing/cdc_data/customers"
ORDERS_VOLUME_PATH = "/Volumes/lakeflow_demo/landing/cdc_data/orders"

TABLE_CONFIG = {
    "customers": f"{CUSTOMERS_VOLUME_PATH}",
    "orders": f"{ORDERS_VOLUME_PATH}"
}

def create_stream(table_name, path):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.header", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.validateOptions", "false")
            .load(path)
            .withColumn("ingestion_timestamp", current_timestamp())
    )

# Dynamically create a function for each table
for table_name, path in TABLE_CONFIG.items():
    def make_fn(name=table_name, p=path):
        @dlt.table(
            name=name,
            comment=f"Raw {name} CDC data ingested from volume, append-only."
        )
        def _func():
            return create_stream(name, p)
    make_fn()
