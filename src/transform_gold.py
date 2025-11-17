# src/transform_gold.py

from typing import Dict

import os

from pyspark.sql import SparkSession, DataFrame, functions as F

from src.create_dataframes import load_config


# ============================================================
# Build Gold Summary Tables from Star Schema
# ============================================================

def build_gold_tables(star_dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Build Gold-layer summary tables from star schema DataFrames.

    Inputs expected in star_dfs:
      - fact_orders
      - fact_order_items
      - fact_payments
      - fact_product_reviews
      - dim_product_variant (for product/brand/category enrichment)
      - dim_customer (optional for customer attributes)

    Outputs:
      - gold_daily_sales_product
      - gold_daily_sales_customer
      - gold_product_review_summary
      - gold_payment_summary
    """
    gold_dfs: Dict[str, DataFrame] = {}

    # --------------------------------------------------------
    # 1) Daily Sales by Product (and variant)
    # --------------------------------------------------------
    if "fact_order_items" in star_dfs:
        foi = star_dfs["fact_order_items"]
        dpv = star_dfs.get("dim_product_variant")

        sales = foi

        # join product attributes if available
        if dpv is not None:
            prod_sel = dpv.select(
                F.col("product_variant_key"),
                "product_id",
                "product_name",
                "brand_id",
                "brand_name",
                "default_category_id",
                "default_category_name",
            )
            sales = sales.join(prod_sel, on="product_variant_key", how="left")

        # ensure order_date_key is present
        if "order_date_key" not in sales.columns and "order_id" in sales.columns:
            # if not present, we could derive from order_date via another join
            pass

        grp_cols = ["order_date_key", "product_variant_key", "product_id",
                    "product_name", "brand_id", "brand_name",
                    "default_category_id", "default_category_name"]

        # Keep only columns that actually exist
        grp_cols = [c for c in grp_cols if c in sales.columns]

        gold_daily_sales_product = (
            sales
            .groupBy(*grp_cols)
            .agg(
                F.sum("quantity").alias("total_quantity_sold"),
                F.sum("gross_amount").alias("total_gross_sales"),
                F.sum("discount_amount").alias("total_discount_given"),
                F.sum("tax_amount").alias("total_tax_collected"),
                F.sum("shipping_amount").alias("total_shipping_charged"),
                F.sum("net_line_amount").alias("total_net_sales"),
                F.countDistinct("order_id").alias("distinct_orders"),
            )
        )

        gold_dfs["gold_daily_sales_product"] = gold_daily_sales_product

    # --------------------------------------------------------
    # 2) Daily Sales by Customer
    # --------------------------------------------------------
    if "fact_orders" in star_dfs:
        fo = star_dfs["fact_orders"]
        dc = star_dfs.get("dim_customer")

        cust_sales = fo

        # join minimal customer info if available
        if dc is not None:
            dc_sel = dc.select(
                F.col("customer_key"),
                "age_band",
                "is_active_flag",
                "city_normalized",
                "state_normalized",
                "is_metro_city",
            )
            cust_sales = cust_sales.join(dc_sel, on="customer_key", how="left")

        grp_cols = ["order_date_key", "customer_key",
                    "age_band", "is_active_flag",
                    "city_normalized", "state_normalized", "is_metro_city"]
        grp_cols = [c for c in grp_cols if c in cust_sales.columns]

        gold_daily_sales_customer = (
            cust_sales
            .groupBy(*grp_cols)
            .agg(
                F.countDistinct("order_id").alias("order_count"),
                F.sum("grand_total_amount").alias("total_revenue"),
                F.avg("grand_total_amount").alias("avg_order_value"),
                F.sum(F.when(F.col("is_first_order") == True, 1).otherwise(0)).alias("first_order_count"),
                F.sum(F.when(F.col("is_first_order") == False, 1).otherwise(0)).alias("repeat_order_count"),
            )
        )

        gold_dfs["gold_daily_sales_customer"] = gold_daily_sales_customer

    # --------------------------------------------------------
    # 3) Product Review Summary
    # --------------------------------------------------------
    if "fact_product_reviews" in star_dfs:
        fr = star_dfs["fact_product_reviews"]

        # derive review_date_key if not present
        if "review_date" in fr.columns:
            fr = fr.withColumn(
                "review_date_key",
                F.date_format(F.to_date("review_date"), "yyyyMMdd").cast("int")
            )

        grp_cols = ["product_id", "review_date_key"]
        grp_cols = [c for c in grp_cols if c in fr.columns]

        gold_product_review_summary = (
            fr
            .groupBy(*grp_cols)
            .agg(
                F.count("review_id").alias("total_reviews"),
                F.avg("rating").alias("avg_rating"),
                F.sum(F.when(F.col("rating") >= 4, 1).otherwise(0)).alias("positive_reviews"),
                F.sum(F.when(F.col("rating") == 3, 1).otherwise(0)).alias("neutral_reviews"),
                F.sum(F.when(F.col("rating") <= 2, 1).otherwise(0)).alias("negative_reviews"),
                F.sum(F.when(F.col("is_helpful_review") == True, 1).otherwise(0)).alias("helpful_reviews"),
            )
        )

        gold_dfs["gold_product_review_summary"] = gold_product_review_summary

    # --------------------------------------------------------
    # 4) Payment Summary
    # --------------------------------------------------------
    if "fact_payments" in star_dfs:
        fp = star_dfs["fact_payments"]

        # derive payment_date_key
        if "payment_date" in fp.columns:
            fp = fp.withColumn(
                "payment_date_key",
                F.date_format(F.to_date("payment_date"), "yyyyMMdd").cast("int")
            )

        grp_cols = ["payment_date_key", "payment_method_code", "payment_status_code"]
        grp_cols = [c for c in grp_cols if c in fp.columns]

        gold_payment_summary = (
            fp
            .groupBy(*grp_cols)
            .agg(
                F.count("payment_id").alias("payment_attempts"),
                F.sum(F.when(F.col("is_successful") == True, 1).otherwise(0)).alias("successful_payments"),
                F.sum(F.when(F.col("is_failed") == True, 1).otherwise(0)).alias("failed_payments"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_payment_amount"),
            )
        )

        gold_dfs["gold_payment_summary"] = gold_payment_summary

    return gold_dfs


# ============================================================
# Write Gold Tables (DEV: local, PROD: Redshift)
# ============================================================

def write_gold_tables(
    spark: SparkSession,
    gold_dfs: Dict[str, DataFrame],
    config_path: str,
    env: str
) -> None:
    """
    Write Gold summary tables based on environment config.

    DEV:
      - Writes Parquet to local path:
          <local_base_path>/<table_name>/

    PROD:
      - Writes to Amazon Redshift via JDBC:
          schema.table_prefix + table_name
    """
    env_cfg = load_config(config_path, env)
    gold_cfg = env_cfg.get("gold_output")

    if gold_cfg is None:
        raise ValueError(f"'gold_output' section missing in config for env='{env}'")

    fmt = gold_cfg.get("format", "parquet")
    mode = gold_cfg.get("write_mode", "overwrite")

    # DEV: local filesystem
    if "local_base_path" in gold_cfg:
        base_path = os.path.abspath(gold_cfg["local_base_path"])
        print(f"[GOLD WRITE] DEV mode: writing Gold tables to local path: {base_path}")

        os.makedirs(base_path, exist_ok=True)

        for name, df in gold_dfs.items():
            output_path = os.path.join(base_path, name)
            print(f"[GOLD WRITE] Writing Gold table '{name}' to '{output_path}' as {fmt}, mode={mode}")
            (
                df.write
                .mode(mode)
                .format(fmt)
                .save(output_path)
            )

    # PROD: Redshift via JDBC
    elif "redshift" in gold_cfg:
        rs_cfg = gold_cfg["redshift"]
        jdbc_url = rs_cfg["jdbc_url"]
        user = rs_cfg["user"]
        password = rs_cfg["password"]
        schema = rs_cfg.get("schema", "public")
        table_prefix = rs_cfg.get("table_prefix", "")

        print(f"[GOLD WRITE] PROD mode: writing Gold tables to Redshift schema: {schema}")

        for name, df in gold_dfs.items():
            table_name = f"{table_prefix}{name}"
            full_table = f"{schema}.{table_name}"

            print(f"[GOLD WRITE] Writing Gold table '{full_table}' to Redshift via JDBC, mode={mode}")

            (
                df.write
                .mode(mode)
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", full_table)
                .option("user", user)
                .option("password", password)
                # You may need to set driver class depending on environment:
                # .option("driver", "com.amazon.redshift.jdbc.Driver")
                .save()
            )

    else:
        raise ValueError(
            f"Invalid 'gold_output' config for env='{env}'. "
            f"Expected either 'local_base_path' (dev) or 'redshift' config (prod)."
        )
