# main.py

import argparse
import os

from pyspark.sql import SparkSession

from src.create_dataframes import create_all_dataframes
from src.data_cleaning import clean_all_dataframes
from src.data_enrichment import enrich_all_dataframes
from src.transform_silver import build_star_schema, write_star_schema_tables
from src.transform_gold import build_gold_tables, write_gold_tables

def parse_args():
    parser = argparse.ArgumentParser(description="Modular Spark App for E-commerce Medallion Project")

    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        help="Environment name, e.g. 'dev' or 'prod' (default: dev)"
    )

    parser.add_argument(
        "--config",
        type=str,
        default="config.yaml",
        help="Path to YAML config file (default: config.yaml)"
    )

    parser.add_argument(
        "--show-summary",
        action="store_true",
        help="If set, prints schema & row counts for star & gold tables (for debugging)"
    )

    return parser.parse_args()


def build_spark_session(app_name: str = "EcomMedallionApp"):
    """
    Build a basic SparkSession.
    On EMR, this will use the cluster's Spark + Glue/Redshift configs.
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )
    return spark


def main():
    args = parse_args()

    # Resolve config path relative to current working directory
    config_path = os.path.abspath(args.config)

    print(f"[MAIN] Using environment: {args.env}")
    print(f"[MAIN] Using config file: {config_path}")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # 1) Bronze → DataFrames
        all_dfs = create_all_dataframes(
            spark=spark,
            config_path=config_path,
            env=args.env
        )

        # 2) Cleaning / standardization layer
        cleaned_dfs = clean_all_dataframes(all_dfs)

        # 3) Enrichment layer
        enriched_dfs = enrich_all_dataframes(cleaned_dfs)

        # 4) Build Star Schema (Silver)
        star_dfs = build_star_schema(enriched_dfs)

        if args.show_summary:
            print("\n[SUMMARY] Star Schema Tables:")
            for name, df in star_dfs.items():
                print(f"\n--- Star Table: {name} ---")
                df.printSchema()
                print(f"Row count: {df.count()}")

        # 5) Write Star Schema (Silver) to destination
        write_star_schema_tables(
            spark=spark,
            star_dfs=star_dfs,
            config_path=config_path,
            env=args.env
        )

        # 6) Build Gold Summary Tables from Star Schema
        gold_dfs = build_gold_tables(star_dfs)

        if args.show_summary:
            print("\n[SUMMARY] Gold Tables:")
            for name, df in gold_dfs.items():
                print(f"\n--- Gold Table: {name} ---")
                df.printSchema()
                print(f"Row count: {df.count()}")

        # 7) Write Gold Tables (dev: local, prod: Redshift)
        write_gold_tables(
            spark=spark,
            gold_dfs=gold_dfs,
            config_path=config_path,
            env=args.env
        )

    finally:
        spark.stop()
        print("[MAIN] Spark session stopped.")


if __name__ == "__main__":
    main()
