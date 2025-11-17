# src/data_enrichment.py

from typing import Dict

from pyspark.sql import DataFrame, functions as F, Window


# ============================================================
# CUSTOMER ENRICHMENT
# ============================================================

def enrich_customer_df(df: DataFrame) -> DataFrame:
    """
    Enrich customer data:
      - full_name
      - birth_year
      - age (approx based on current_date)
      - age_band (18-25, 26-35, 36-45, 46+)
      - is_active_flag
    """
    # full_name
    if all(c in df.columns for c in ["first_name", "last_name"]):
        df = df.withColumn(
            "full_name",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
        )

    # birth_year, age, age_band (if date_of_birth exists)
    if "date_of_birth" in df.columns:
        df = df.withColumn("birth_year", F.year("date_of_birth"))

        # approximate age in years
        df = df.withColumn(
            "age_years",
            F.floor(
                F.datediff(F.current_date(), F.col("date_of_birth")) / F.lit(365.25)
            )
        )

        df = df.withColumn(
            "age_band",
            F.when(F.col("age_years") < 18, F.lit("UNDER_18"))
             .when((F.col("age_years") >= 18) & (F.col("age_years") <= 25), F.lit("18_25"))
             .when((F.col("age_years") >= 26) & (F.col("age_years") <= 35), F.lit("26_35"))
             .when((F.col("age_years") >= 36) & (F.col("age_years") <= 45), F.lit("36_45"))
             .when(F.col("age_years") > 45, F.lit("46_PLUS"))
             .otherwise(F.lit("UNKNOWN"))
        )

    # is_active_flag based on status_code
    if "status_code" in df.columns:
        df = df.withColumn(
            "is_active_flag",
            (F.col("status_code") == F.lit("ACTIVE")).cast("boolean")
        )

    return df


# ============================================================
# CUSTOMER ADDRESS ENRICHMENT
# ============================================================

def enrich_customer_address_df(df: DataFrame) -> DataFrame:
    """
    Enrich customer_address:
      - city_normalized, state_normalized
      - is_metro_city flag (simple list-based)
    """
    if "city" in df.columns:
        df = df.withColumn("city_normalized", F.upper(F.trim(F.col("city"))))

    if "state" in df.columns:
        df = df.withColumn("state_normalized", F.upper(F.trim(F.col("state"))))

    # simple metro city classification for demo purposes
    metro_cities = ["MUMBAI", "DELHI", "BENGALURU", "BANGALORE", "KOLKATA", "CHENNAI", "HYDERABAD", "PUNE"]

    if "city_normalized" in df.columns:
        df = df.withColumn(
            "is_metro_city",
            F.col("city_normalized").isin(metro_cities)
        )

    return df


# ============================================================
# PRODUCT & PRODUCT VARIANT ENRICHMENT
# ============================================================

def enrich_product_df(df: DataFrame) -> DataFrame:
    """
    Enrich product:
      - product_name_search (lower-case, trimmed for search engines)
      - is_active_flag (if not already boolean)
    """
    if "product_name" in df.columns:
        df = df.withColumn(
            "product_name_search",
            F.lower(F.trim(F.col("product_name")))
        )

    if "is_active" in df.columns:
        df = df.withColumn("is_active_flag", F.col("is_active").cast("boolean"))

    return df


def enrich_product_variant_df(df: DataFrame) -> DataFrame:
    """
    Enrich product_variant:
      - sku_normalized (upper-case)
      - variant_key (combination of product_id + sku)
    """
    if "sku" in df.columns:
        df = df.withColumn("sku_normalized", F.upper(F.trim(F.col("sku"))))

    if all(c in df.columns for c in ["product_id", "sku"]):
        df = df.withColumn(
            "variant_key",
            F.concat_ws("|", F.col("product_id").cast("string"), F.col("sku"))
        )

    return df


# ============================================================
# INVENTORY ENRICHMENT
# ============================================================

def enrich_inventory_level_df(df: DataFrame) -> DataFrame:
    """
    Enrich inventory_level:
      - available_qty = on_hand_qty - reserved_qty
      - is_out_of_stock flag
      - is_low_stock flag (threshold = 10 units)
    """
    if all(c in df.columns for c in ["on_hand_qty", "reserved_qty"]):
        df = df.withColumn(
            "available_qty",
            (F.col("on_hand_qty") - F.col("reserved_qty")).cast("int")
        )

        df = df.withColumn(
            "is_out_of_stock",
            (F.col("available_qty") <= 0).cast("boolean")
        )

        df = df.withColumn(
            "is_low_stock",
            (F.col("available_qty") > 0) & (F.col("available_qty") <= 10)
        )

    return df


# ============================================================
# ORDERS ENRICHMENT
# ============================================================

def enrich_orders_df(df: DataFrame) -> DataFrame:
    """
    Enrich orders:
      - order_date_date, order_year, order_month, order_day
      - order_value_band (LOW/MEDIUM/HIGH based on grand_total_amount)
      - is_cod_order (based on future join with payment or a simple placeholder flag)
      - customer_order_sequence (order number per customer by date)
    Note: Here we only use data within orders table; payment-link logic can be added later.
    """
    if "order_date" in df.columns:
        df = df.withColumn("order_date_date", F.to_date("order_date"))
        df = df.withColumn("order_year", F.year("order_date"))
        df = df.withColumn("order_month", F.month("order_date"))
        df = df.withColumn("order_day", F.dayofmonth("order_date"))
        df = df.withColumn("order_weekofyear", F.weekofyear("order_date"))

    if "grand_total_amount" in df.columns:
        df = df.withColumn(
            "order_value_band",
            F.when(F.col("grand_total_amount") < 2000, F.lit("LOW"))
             .when((F.col("grand_total_amount") >= 2000) & (F.col("grand_total_amount") <= 10000), F.lit("MEDIUM"))
             .when(F.col("grand_total_amount") > 10000, F.lit("HIGH"))
             .otherwise(F.lit("UNKNOWN"))
        )

    # customer_order_sequence: 1st order, 2nd order, etc.
    if all(c in df.columns for c in ["customer_id", "order_date"]):
        w = Window.partitionBy("customer_id").orderBy(F.col("order_date").asc())
        df = df.withColumn("customer_order_sequence", F.row_number().over(w))

        # first_order_flag
        df = df.withColumn(
            "is_first_order",
            (F.col("customer_order_sequence") == 1).cast("boolean")
        )

    return df


# ============================================================
# ORDER ITEM ENRICHMENT
# ============================================================

def enrich_order_item_df(df: DataFrame) -> DataFrame:
    """
    Enrich order_item:
      - gross_amount = quantity * base_unit_price
      - effective_discount_pct = discount_amount / gross_amount
      - net_line_amount = gross_amount - discount_amount + tax_amount + shipping_amount
    """
    required = ["quantity", "base_unit_price"]
    if all(c in df.columns for c in required):
        df = df.withColumn(
            "gross_amount",
            (F.col("quantity") * F.col("base_unit_price")).cast("double")
        )

    if all(c in df.columns for c in ["discount_amount", "gross_amount"]):
        df = df.withColumn(
            "effective_discount_pct",
            F.when(F.col("gross_amount") > 0,
                   (F.col("discount_amount") / F.col("gross_amount")) * 100.0)
             .otherwise(F.lit(0.0))
        )

    needed_for_net = ["gross_amount", "discount_amount", "tax_amount", "shipping_amount"]
    if all(c in df.columns for c in needed_for_net):
        df = df.withColumn(
            "net_line_amount",
            F.col("gross_amount")
            - F.col("discount_amount")
            + F.col("tax_amount")
            + F.col("shipping_amount")
        )

    return df


# ============================================================
# PAYMENT ENRICHMENT
# ============================================================

def enrich_payment_df(df: DataFrame) -> DataFrame:
    """
    Enrich payment:
      - is_successful flag
      - is_failed flag
      - payment_attempt_number per order
    """
    if "payment_status_code" in df.columns:
        df = df.withColumn(
            "is_successful",
            (F.col("payment_status_code") == F.lit("SUCCESS")).cast("boolean")
        )
        df = df.withColumn(
            "is_failed",
            (F.col("payment_status_code") == F.lit("FAILED")).cast("boolean")
        )

    if all(c in df.columns for c in ["order_id", "payment_date"]):
        w = Window.partitionBy("order_id").orderBy(F.col("payment_date").asc())
        df = df.withColumn(
            "payment_attempt_number",
            F.row_number().over(w)
        )

    return df


# ============================================================
# PRODUCT REVIEWS ENRICHMENT (FILE SOURCE)
# ============================================================

def enrich_product_reviews_df(df: DataFrame) -> DataFrame:
    """
    Enrich product_reviews:
      - review_length_chars
      - review_sentiment_label (GOOD/NEUTRAL/BAD based on rating)
      - is_helpful_review (helpful_votes >= threshold)
    """
    if "review_text" in df.columns:
        df = df.withColumn(
            "review_length_chars",
            F.length(F.col("review_text"))
        )

    # simple sentiment based on rating
    if "rating" in df.columns:
        df = df.withColumn(
            "review_sentiment_label",
            F.when(F.col("rating") >= 4, F.lit("GOOD"))
             .when(F.col("rating") == 3, F.lit("NEUTRAL"))
             .when(F.col("rating") <= 2, F.lit("BAD"))
             .otherwise(F.lit("UNKNOWN"))
        )

    if "helpful_votes" in df.columns:
        df = df.withColumn(
            "is_helpful_review",
            (F.col("helpful_votes") >= 5).cast("boolean")
        )

    return df


# ============================================================
# PROMOTION ENRICHMENT
# ============================================================

def enrich_promotion_df(df: DataFrame) -> DataFrame:
    """
    Enrich promotion:
      - is_percentage_discount flag
      - is_flat_discount flag
      - is_currently_active flag (based on start_date/end_date and is_active)
    """
    if "discount_type" in df.columns:
        df = df.withColumn(
            "is_percentage_discount",
            (F.col("discount_type") == F.lit("PERCENT")).cast("boolean")
        )
        df = df.withColumn(
            "is_flat_discount",
            (F.col("discount_type") == F.lit("AMOUNT")).cast("boolean")
        )

    if all(c in df.columns for c in ["start_date", "end_date", "is_active"]):
        df = df.withColumn("start_date_date", F.to_date("start_date"))
        df = df.withColumn("end_date_date", F.to_date("end_date"))

        df = df.withColumn(
            "is_currently_active",
            (
                (F.col("is_active") == True) &
                (F.col("start_date_date") <= F.current_date()) &
                (
                    F.col("end_date_date").isNull()
                    | (F.col("end_date_date") >= F.current_date())
                )
            ).cast("boolean")
        )

    return df


# ============================================================
# Orchestrator
# ============================================================

TABLE_ENRICHERS = {
    "customer": enrich_customer_df,
    "customer_address": enrich_customer_address_df,
    "product": enrich_product_df,
    "product_variant": enrich_product_variant_df,
    "inventory_level": enrich_inventory_level_df,
    "orders": enrich_orders_df,
    "order_item": enrich_order_item_df,
    "payment": enrich_payment_df,
    "product_reviews": enrich_product_reviews_df,
    "promotion": enrich_promotion_df,
    # Add more table-specific enrichers here if needed
}


def enrich_all_dataframes(cleaned_dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Apply enrichment functions to cleaned DataFrames.
    If a table doesn't have a specific enricher, it is passed through unchanged.
    """
    enriched: Dict[str, DataFrame] = {}

    for name, df in cleaned_dfs.items():
        print(f"[ENRICH] Processing DataFrame '{name}'")
        if name in TABLE_ENRICHERS:
            enriched_df = TABLE_ENRICHERS[name](df)
        else:
            # pass-through for lookup / small reference tables etc.
            enriched_df = df

        enriched[name] = enriched_df

    print(f"[ENRICH] Finished enriching {len(enriched)} DataFrame(s).")
    return enriched
