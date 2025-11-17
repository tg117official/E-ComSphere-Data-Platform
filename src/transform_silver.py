# src/transform_silver.py

from typing import Dict

import os

from pyspark.sql import DataFrame, SparkSession, functions as F, Window

from src.create_dataframes import load_config


# ============================================================
# Build Star Schema DataFrames from enriched DataFrames
# ============================================================

def build_star_schema(enriched_dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Build star schema tables (dimensions + facts) from enriched dataframes.

    Expected keys in enriched_dfs:
        - customer, customer_address
        - product, product_variant, brand, category
        - orders, order_item, payment, order_promotion, promotion
        - product_reviews
        - plus lookup tables like order_status, order_channel, payment_method, etc.

    Returns:
        {
          "dim_customer": df,
          "dim_product": df,
          "dim_product_variant": df,
          "dim_date": df,
          "dim_promotion": df,
          "fact_orders": df,
          "fact_order_items": df,
          "fact_payments": df,
          "fact_product_reviews": df
        }
    """
    star_dfs: Dict[str, DataFrame] = {}

    # --------------------------------------------------------
    # Dimension: Date (based on order_date)
    # --------------------------------------------------------
    if "orders" in enriched_dfs:
        orders = enriched_dfs["orders"]

        date_dim = (
            orders
            .select(F.to_date("order_date").alias("date"))
            .where(F.col("date").isNotNull())
            .distinct()
        )

        date_dim = (
            date_dim
            .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("day", F.dayofmonth("date"))
            .withColumn("weekofyear", F.weekofyear("date"))
            .withColumn("quarter", F.quarter("date"))
            .withColumn("day_of_week", F.date_format("date", "E"))
            .withColumn("month_name", F.date_format("date", "MMMM"))
        )

        star_dfs["dim_date"] = (
            date_dim
            .select(
                "date_key",
                "date",
                "year",
                "quarter",
                "month",
                "month_name",
                "weekofyear",
                "day",
                "day_of_week",
            )
        )

    # --------------------------------------------------------
    # Dimension: Customer (with default HOME address if available)
    # --------------------------------------------------------
    if "customer" in enriched_dfs:
        customer = enriched_dfs["customer"]
        cust_addr = enriched_dfs.get("customer_address")

        dim_customer = customer

        if cust_addr is not None:
            # Pick default HOME address per customer if possible
            addr_filtered = (
                cust_addr
                .where(
                    (F.col("is_default") == True)
                    & (F.col("address_type_code") == "HOME")
                )
            )

            # In case of multiple default addresses (bad data), pick earliest created_at
            w = Window.partitionBy("customer_id").orderBy(F.col("created_at").asc())
            addr_ranked = addr_filtered.withColumn("addr_rank", F.row_number().over(w))
            addr_unique = addr_ranked.where(F.col("addr_rank") == 1).drop("addr_rank")

            # IMPORTANT: avoid ambiguous ingestion_ts after join
            if "ingestion_ts" in addr_unique.columns:
                addr_unique = addr_unique.withColumnRenamed("ingestion_ts", "addr_ingestion_ts")

            dim_customer = (
                customer
                .join(addr_unique, on="customer_id", how="left")
            )

        # Customer key = natural key for now
        dim_customer = dim_customer.withColumnRenamed("customer_id", "customer_key")

        dim_customer = dim_customer.select(
            "customer_key",
            "first_name",
            "last_name",
            "full_name",
            "email",
            "email_is_valid",
            "phone_number",
            "phone_is_valid",
            "gender",
            "date_of_birth",
            "birth_year",
            "age_years",
            "age_band",
            "status_code",
            "is_active_flag",
            # address fields (nullable if no HOME address)
            "address_line1",
            "address_line2",
            "landmark",
            "city",
            "state",
            "postal_code",
            "country_code",
            "city_normalized",
            "state_normalized",
            "is_metro_city",
            # keep customer's ingestion_ts as main lineage column
            "ingestion_ts",
            # if you want, you can also expose address ingestion_ts:
            # "addr_ingestion_ts",
        )

        star_dfs["dim_customer"] = dim_customer

    # --------------------------------------------------------
    # Dimension: Product + Category + Brand
    # --------------------------------------------------------
    if "product" in enriched_dfs:
        product = enriched_dfs["product"]
        brand = enriched_dfs.get("brand")
        category = enriched_dfs.get("category")

        dim_product = product

        if brand is not None:
            dim_product = dim_product.join(brand.select("brand_id", "brand_name"), on="brand_id", how="left")

        if category is not None:
            cat = category.select(
                F.col("category_id").alias("default_category_id"),
                F.col("category_name").alias("default_category_name"),
                "parent_category_id",
                "category_level",
            )
            dim_product = dim_product.join(cat, on="default_category_id", how="left")

        dim_product = dim_product.withColumnRenamed("product_id", "product_key")

        star_dfs["dim_product"] = dim_product.select(
            "product_key",
            "product_name",
            "product_name_search",
            "brand_id",
            "brand_name",
            "default_category_id",
            "default_category_name",
            "parent_category_id",
            "category_level",
            "short_description",
            "is_active",
            "is_active_flag",
            "created_at",
            "updated_at",
            "ingestion_ts",
        )

    # --------------------------------------------------------
    # Dimension: Product Variant (flattened with product info)
    # --------------------------------------------------------
    if "product_variant" in enriched_dfs:
        variant = enriched_dfs["product_variant"]
        prod_dim = star_dfs.get("dim_product")

        dim_product_variant = variant

        if prod_dim is not None:
            prod_select = prod_dim.select(
                F.col("product_key").alias("product_id"),
                "product_name",
                "product_name_search",
                "brand_id",
                "brand_name",
                "default_category_id",
                "default_category_name",
            )
            dim_product_variant = dim_product_variant.join(prod_select, on="product_id", how="left")

        dim_product_variant = dim_product_variant.withColumnRenamed(
            "product_variant_id", "product_variant_key"
        )

        star_dfs["dim_product_variant"] = dim_product_variant.select(
            "product_variant_key",
            "product_id",
            "sku",
            "sku_normalized",
            "variant_name",
            "variant_key",
            "product_name",
            "product_name_search",
            "brand_id",
            "brand_name",
            "default_category_id",
            "default_category_name",
            "is_active",
            "created_at",
            "updated_at",
            "ingestion_ts",
        )

    # --------------------------------------------------------
    # Dimension: Promotion
    # --------------------------------------------------------
    if "promotion" in enriched_dfs:
        promotion = enriched_dfs["promotion"]

        dim_promotion = promotion.withColumnRenamed("promotion_id", "promotion_key")

        star_dfs["dim_promotion"] = dim_promotion.select(
            "promotion_key",
            "promotion_code",
            "promotion_name",
            "discount_type",
            "discount_value",
            "min_order_amount",
            "max_discount_amount",
            "start_date",
            "end_date",
            "is_active",
            "is_percentage_discount",
            "is_flat_discount",
            "is_currently_active",
            "ingestion_ts",
        )

    # --------------------------------------------------------
    # Fact: Orders (order-level grain)
    # --------------------------------------------------------
    if "orders" in enriched_dfs:
        orders = enriched_dfs["orders"]

        fact_orders = orders

        # derive date_key from order_date
        fact_orders = fact_orders.withColumn(
            "order_date_key",
            F.date_format(F.to_date("order_date"), "yyyyMMdd").cast("int")
        )

        # rename customer_id to customer_key for consistency
        if "customer_id" in fact_orders.columns:
            fact_orders = fact_orders.withColumnRenamed("customer_id", "customer_key")

        star_dfs["fact_orders"] = fact_orders.select(
            "order_id",
            "customer_key",
            "order_date",
            "order_date_key",
            "order_status_code",
            "order_status_is_valid",
            "order_channel_code",
            "currency_code",
            "billing_address_id",
            "shipping_address_id",
            "total_item_amount",
            "total_discount_amount",
            "total_tax_amount",
            "total_shipping_amount",
            "grand_total_amount",
            "order_value_band",
            "customer_order_sequence",
            "is_first_order",
            "amount_mismatch_flag",
            "created_at",
            "updated_at",
            "ingestion_ts",
        )

    # --------------------------------------------------------
    # Fact: Order Items (line-level grain)
    # --------------------------------------------------------
    if "order_item" in enriched_dfs:
        order_item = enriched_dfs["order_item"]
        orders = enriched_dfs.get("orders")

        fact_oi = order_item

        # join to get customer_key, order_date_key where possible
        if orders is not None:
            orders_sel = (
                orders
                .select(
                    "order_id",
                    "customer_id",
                    F.date_format(F.to_date("order_date"), "yyyyMMdd").cast("int").alias("order_date_key")
                )
            )

            fact_oi = fact_oi.join(orders_sel, on="order_id", how="left")
            fact_oi = fact_oi.withColumnRenamed("customer_id", "customer_key")

        # rename product_variant_id to product_variant_key
        if "product_variant_id" in fact_oi.columns:
            fact_oi = fact_oi.withColumnRenamed("product_variant_id", "product_variant_key")

        star_dfs["fact_order_items"] = fact_oi.select(
            "order_item_id",
            "order_id",
            "customer_key",
            "product_variant_key",
            "order_date_key",
            "quantity",
            "base_unit_price",
            "discount_amount",
            "tax_amount",
            "shipping_amount",
            "gross_amount",
            "effective_discount_pct",
            "net_line_amount",
            "created_at",
            "updated_at",
            "ingestion_ts",
        )

    # --------------------------------------------------------
    # Fact: Payments
    # --------------------------------------------------------
    if "payment" in enriched_dfs:
        payment = enriched_dfs["payment"]

        fact_payments = payment

        # rename payment_id as payment_key if you'd like, but it's okay as-is
        star_dfs["fact_payments"] = fact_payments.select(
            "payment_id",
            "order_id",
            "payment_method_code",
            "payment_status_code",
            "is_successful",
            "is_failed",
            "payment_attempt_number",
            "amount",
            "currency_code",
            "transaction_reference",
            "payment_date",
            "created_at",
            "updated_at",
            "ingestion_ts",
        )

    # --------------------------------------------------------
    # Fact: Product Reviews
    # --------------------------------------------------------
    if "product_reviews" in enriched_dfs:
        pr = enriched_dfs["product_reviews"]

        fact_reviews = pr

        star_dfs["fact_product_reviews"] = fact_reviews.select(
            "review_id",
            "product_id",
            "customer_id",
            "rating",
            "rating_was_out_of_range",
            "review_title",
            "review_text",
            "review_length_chars",
            "review_sentiment_label",
            "helpful_votes",
            "is_helpful_review",
            "source",
            "status",
            "review_status_is_valid",
            "review_date",
            "metadata",
            "ingestion_ts",
        )

    return star_dfs


# ============================================================
# Write Star Schema Tables based on env (dev / prod)
# ============================================================

def write_star_schema_tables(
    spark: SparkSession,
    star_dfs: Dict[str, DataFrame],
    config_path: str,
    env: str
) -> None:
    """
    Write star schema tables to destination based on environment config.

    DEV:
      - Writes Parquet to local path:
          <local_base_path>/<table_name>/

    PROD:
      - Writes Parquet to S3 and registers tables in Glue Catalog:
          s3_base_path/<table_name>/
          glue_catalog_db.table_prefix + table_name
    """
    env_cfg = load_config(config_path, env)
    silver_cfg = env_cfg.get("silver_output")

    if silver_cfg is None:
        raise ValueError(f"'silver_output' section missing in config for env='{env}'")

    fmt = silver_cfg.get("format", "parquet")
    mode = silver_cfg.get("write_mode", "overwrite")

    # Decide dev-style (local) or prod-style (S3 + Glue) based on keys present
    if "local_base_path" in silver_cfg:
        # DEV: Local Parquet writes
        base_path = os.path.abspath(silver_cfg["local_base_path"])
        print(f"[WRITE] DEV mode: writing star schema tables to local path: {base_path}")

        os.makedirs(base_path, exist_ok=True)

        for name, df in star_dfs.items():
            output_path = os.path.join(base_path, name)
            print(f"[WRITE] Writing table '{name}' to '{output_path}' as {fmt}, mode={mode}")
            (
                df.write
                .mode(mode)
                .format(fmt)
                .save(output_path)
            )

    elif "s3_base_path" in silver_cfg and "glue_catalog_db" in silver_cfg:
        # PROD: S3 + Glue
        s3_base = silver_cfg["s3_base_path"].rstrip("/")
        glue_db = silver_cfg["glue_catalog_db"]
        table_prefix = silver_cfg.get("table_prefix", "")

        print(f"[WRITE] PROD mode: writing star schema tables to S3 base: {s3_base}")
        print(f"[WRITE] Registering tables in Glue Catalog DB: {glue_db}")

        for name, df in star_dfs.items():
            table_name = f"{table_prefix}{name}"
            s3_path = f"{s3_base}/{name}"

            print(f"[WRITE] Writing table '{table_name}' to S3 path '{s3_path}' as {fmt}, mode={mode}")

            (
                df.write
                .mode(mode)
                .format(fmt)
                .option("path", s3_path)
                .saveAsTable(f"{glue_db}.{table_name}")
            )

    else:
        raise ValueError(
            f"Invalid 'silver_output' config for env='{env}'. "
            f"Expected either 'local_base_path' (dev) or 's3_base_path' + 'glue_catalog_db' (prod)."
        )
