from pyspark.sql import SparkSession
import os
import glob

def main():
    # ------------------------------------------------------------------
    # 1. Create SparkSession
    # ------------------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("TSV to Parquet Converter")
        .getOrCreate()
    )

    # ------------------------------------------------------------------
    # 2. Find all TSV files in the current working directory
    # ------------------------------------------------------------------
    # Example: customer.tsv, orders.tsv, payment.tsv, etc.
    tsv_files = glob.glob("*")

    print(f"Found {len(tsv_files)} TSV file(s):")
    for f in tsv_files:
        print(f"  - {f}")

    # ------------------------------------------------------------------
    # 3. For each TSV file:
    #    - Read with header & tab separator
    #    - Infer schema
    #    - Write as Parquet (one folder per table)
    # ------------------------------------------------------------------
    for tsv_path in tsv_files:
        table_name = os.path.splitext(os.path.basename(tsv_path))[0]
        output_path = f"{table_name}_parquet"

        print(f"\nProcessing table '{table_name}' from file '{tsv_path}'")

        # Read TSV
        df = (
            spark.read
            .option("header", True)       # first line has column names
            .option("sep", "\t")          # tab-separated
            .option("inferSchema", True)  # let Spark infer column types
            .option("nullValue", "")      # treat empty fields as null
            .csv(tsv_path)
        )

        print(f"  Columns: {df.columns}")
        print(f"  Row count (approx): {df.count()}")

        # Write as Parquet (overwrite if already exists)
        (
            df.write
            .mode("overwrite")
            .parquet(output_path)
        )

        print(f"  -> Written Parquet to: {output_path}")

    # ------------------------------------------------------------------
    # 4. Stop Spark
    # ------------------------------------------------------------------
    spark.stop()
    print("\nAll TSV files converted to Parquet successfully.")

if __name__ == "__main__":
    main()
