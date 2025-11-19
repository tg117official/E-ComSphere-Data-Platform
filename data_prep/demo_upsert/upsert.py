from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

def main():
    spark = (
        SparkSession.builder
        .appName("hudi_upsert_batch_demo")
        .getOrCreate()
    )

    # ------------------------------------------------------------------
    # 1. Sample data for UPSERT batch
    #    - Updates order o1 (later update_ts)
    #    - Inserts new order o4
    # ------------------------------------------------------------------
    upsert_data = [
        # updated existing order
        ("o1", "c1", 120.0, "UPDATED", "2025-11-18 11:00:00", "2025-11-18"),
        # new order
        ("o4", "c4", 300.0, "NEW",     "2025-11-18 11:05:00", "2025-11-18"),
    ]

    columns = ["order_id", "customer_id", "amount", "status", "update_ts", "order_date"]

    df_upsert = (
        spark.createDataFrame(upsert_data, schema=columns)
             .withColumn("update_ts", to_timestamp("update_ts"))
             .withColumn("order_date", col("order_date").cast("date"))
    )

    # ------------------------------------------------------------------
    # 2. Hudi config (MUST MATCH initial script)
    # ------------------------------------------------------------------
    hudi_base_path = "s3://ecom-medallion-tg117/silver/orders_hudi_demo"  # same as initial script
    hudi_table_name = "orders_hudi_demo"
    hudi_db_name    = "ecom_silver"
    record_key      = "order_id"
    partition_field = "order_date"
    precombine_key  = "update_ts"

    hudi_options = {
        "hoodie.table.name": hudi_table_name,
        "hoodie.database.name": hudi_db_name,

        "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",

        "hoodie.datasource.write.recordkey.field": record_key,
        "hoodie.datasource.write.precombine.field": precombine_key,
        "hoodie.datasource.write.partitionpath.field": partition_field,
        "hoodie.datasource.write.hive_style_partitioning": "true",

        # Operation = UPSERT (insert + update based on key)
        "hoodie.datasource.write.operation": "upsert",

        # Glue / Hive sync
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.database": hudi_db_name,
        "hoodie.datasource.hive_sync.table": hudi_table_name,
        "hoodie.datasource.hive_sync.partition_fields": partition_field,
        "hoodie.datasource.hive_sync.partition_extractor_class":
            "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
    }

    # ------------------------------------------------------------------
    # 3. UPSERT write (Spark mode=append, Hudi handles upsert logic)
    # ------------------------------------------------------------------
    (df_upsert.write
        .format("hudi")
        .options(**hudi_options)
        .mode("append")         # IMPORTANT: append, not overwrite
        .save(hudi_base_path)
    )

    # ------------------------------------------------------------------
    # 4. Read back latest snapshot to verify:
    #    - o1 should be UPDATED
    #    - o4 should be present as a new row
    # ------------------------------------------------------------------
    print("==== After UPSERT (latest snapshot) ====")
    snapshot_df = spark.read.format("hudi").load(hudi_base_path)
    snapshot_df.orderBy("order_id").show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
