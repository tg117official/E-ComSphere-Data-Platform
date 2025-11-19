from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

def main():
    spark = (
        SparkSession.builder
        .appName("hudi_initial_insert_demo")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------------
    # 1. Sample data for initial load (snapshot)
    # ------------------------------------------------------------------
    initial_data = [
        # order_id, customer_id, amount, status,       update_ts,             order_date
        ("o1",      "c1",        100.0,  "NEW",        "2025-11-18 10:00:00", "2025-11-18"),
        ("o2",      "c2",        150.0,  "NEW",        "2025-11-18 10:05:00", "2025-11-18"),
        ("o3",      "c3",        200.0,  "NEW",        "2025-11-18 10:10:00", "2025-11-18"),
    ]

    columns = ["order_id", "customer_id", "amount", "status", "update_ts", "order_date"]

    df_initial = (
        spark.createDataFrame(initial_data, schema=columns)
             .withColumn("update_ts", to_timestamp("update_ts"))
             .withColumn("order_date", col("order_date").cast("date"))
    )

    # ------------------------------------------------------------------
    # 2. Hudi config (same values must be used in the upsert script)
    # ------------------------------------------------------------------
    hudi_base_path = "s3://ecom-medallion-tg117/silver/orders_hudi_demo"  # <-- change
    hudi_table_name = "orders_hudi_demo"
    hudi_db_name    = "ecom_silver"
    record_key      = "order_id"
    partition_field = "order_date"
    precombine_key  = "update_ts"

    hudi_options = {
        "hoodie.table.name": hudi_table_name,
        "hoodie.database.name": hudi_db_name,


        # Table type (CoW is simpler for analytics)
        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",

        # Key fields
        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.precombine.field": precombine_key,
        "hoodie.datasource.write.partitionpath.field": partition_field,
        "hoodie.datasource.write.hive_style_partitioning": "true",

        # Operation = INSERT for first load
        "hoodie.datasource.write.operation": "insert",

        # Glue / Hive sync
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms",  # EMR + Glue
        "hoodie.datasource.hive_sync.database": hudi_db_name,
        "hoodie.datasource.hive_sync.table": hudi_table_name,
        "hoodie.datasource.hive_sync.partition_fields": partition_field,
        "hoodie.datasource.hive_sync.partition_extractor_class":
            "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",

    }

    # ------------------------------------------------------------------
    # 3. Initial INSERT write (bootstrapping the Hudi table)
    # ------------------------------------------------------------------
    (df_initial.write
        .format("hudi")
        .options(**hudi_options)
        .mode("overwrite")          # only for very first load
        .save(hudi_base_path)
    )

    print("==== After initial INSERT (snapshot) ====")
    spark.read.format("hudi").load(hudi_base_path).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
