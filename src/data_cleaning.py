# src/data_cleaning.py

from typing import Dict, List

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T


# ============================================================
# Generic utility cleaning functions (reusable across tables)
# ============================================================

def standardize_column_names(df: DataFrame) -> DataFrame:
    """
    Standardize column names to snake_case-like style:
    - strip spaces
    - lower case
    - replace spaces with underscores
    (Only useful if you might get inconsistent names. Kept for completeness.)
    """
    renamed_cols = {
        col: col.strip().lower().replace(" ", "_")
        for col in df.columns
    }
    for old, new in renamed_cols.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


def trim_all_string_columns(df: DataFrame) -> DataFrame:
    """
    Trim leading/trailing spaces for ALL string columns.
    Uses df[col_name] instead of F.col(col_name) so that
    columns with dots/spaces in their names still work.
    """
    for col_name, dtype in df.dtypes:
        if dtype == "string":
            df = df.withColumn(col_name, F.trim(df[col_name]))
    return df



def replace_empty_strings_with_nulls(df: DataFrame, cols: List[str] = None) -> DataFrame:
    if cols is None:
        cols = [c for c, t in df.dtypes if t == "string"]

    for col_name in cols:
        df = df.withColumn(
            col_name,
            F.when(df[col_name] == "", F.lit(None)).otherwise(df[col_name])
        )
    return df



def drop_rows_with_nulls_in_keys(df: DataFrame, key_cols: List[str]) -> DataFrame:
    """
    Drop rows where any of the key columns is null.
    """
    return df.dropna(subset=key_cols)


def drop_duplicate_rows_by_keys(df: DataFrame, key_cols: List[str]) -> DataFrame:
    """
    Drop duplicate rows based on key columns.
    Keeps the first occurrence.
    """
    return df.dropDuplicates(subset=key_cols)


def enforce_non_negative_columns(df: DataFrame, cols: List[str]) -> DataFrame:
    """
    For numeric columns where negative values are invalid, clamp them to 0 and add a warning flag.
    Adds a boolean column per input column: <col>_was_negative.
    """
    for col_name in cols:
        if col_name in df.columns:
            flag_col = f"{col_name}_was_negative"
            df = df.withColumn(
                flag_col,
                F.when(F.col(col_name) < 0, F.lit(True)).otherwise(F.lit(False))
            )
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name) < 0, F.lit(0)).otherwise(F.col(col_name))
            )
    return df


def clip_column_to_range(df: DataFrame, col_name: str, min_val, max_val) -> DataFrame:
    """
    Clip numeric column to [min_val, max_val] range.
    """
    if col_name not in df.columns:
        return df

    return df.withColumn(
        col_name,
        F.when(F.col(col_name) < min_val, F.lit(min_val))
         .when(F.col(col_name) > max_val, F.lit(max_val))
         .otherwise(F.col(col_name))
    )


def cast_column_to_timestamp(df: DataFrame, col_name: str) -> DataFrame:
    """
    Cast a column to TimestampType, leaving null for invalid values.
    """
    if col_name not in df.columns:
        return df
    return df.withColumn(col_name, F.to_timestamp(F.col(col_name)))


def cast_column_to_integer(df: DataFrame, col_name: str) -> DataFrame:
    """
    Cast a column to IntegerType, leaving null for invalid parse.
    """
    if col_name not in df.columns:
        return df
    return df.withColumn(col_name, F.col(col_name).cast(T.IntegerType()))


def add_ingestion_ts_column(df: DataFrame, col_name: str = "ingestion_ts") -> DataFrame:
    """
    Add a column with current timestamp to track when data was ingested/cleaned.
    """
    if col_name in df.columns:
        return df
    return df.withColumn(col_name, F.current_timestamp())


# ============================================================
# Customer-specific cleaning functions
# ============================================================

def normalize_customer_name_case(df: DataFrame) -> DataFrame:
    """
    Title-case first_name and last_name.
    """
    for col_name in ["first_name", "last_name"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.initcap(F.col(col_name)))
    return df


def clean_customer_email_format(df: DataFrame) -> DataFrame:
    """
    Lower-case and trim email, and add a basic validity flag.
    Adds: email_is_valid (bool)
    """
    if "email" not in df.columns:
        return df

    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))

    # Very basic email regex (not perfect but good enough for cleaning layer)
    email_regex = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

    df = df.withColumn(
        "email_is_valid",
        F.col("email").rlike(email_regex)
    )
    return df


def clean_customer_phone_format(df: DataFrame) -> DataFrame:
    """
    Trim phone_number, remove spaces, and add a basic validity flag.
    Adds: phone_is_valid (bool)
    """
    if "phone_number" not in df.columns:
        return df

    df = df.withColumn("phone_number", F.regexp_replace(F.col("phone_number"), r"\s+", ""))
    # Very rough check: at least 6 digits somewhere
    df = df.withColumn(
        "phone_is_valid",
        F.regexp_replace(F.col("phone_number"), r"\D", "").rlike(r"^\d{6,}$")
    )
    return df


def validate_customer_status_values(df: DataFrame) -> DataFrame:
    """
    Add a flag indicating if status_code is unexpected.
    Adds: status_code_is_valid (bool)
    """
    if "status_code" not in df.columns:
        return df

    allowed = ["ACTIVE", "INACTIVE", "BLOCKED"]
    df = df.withColumn(
        "status_code_is_valid",
        F.col("status_code").isin(allowed)
    )
    return df


# ============================================================
# Product & Inventory cleaning functions
# ============================================================

def normalize_product_name(df: DataFrame) -> DataFrame:
    """
    Trim product_name and ensure it's not empty/null.
    """
    if "product_name" not in df.columns:
        return df
    df = df.withColumn("product_name", F.trim(F.col("product_name")))
    df = df.withColumn(
        "product_name",
        F.when(F.col("product_name") == "", F.lit(None)).otherwise(F.col("product_name"))
    )
    return df


def trim_sku_and_variant_name(df: DataFrame) -> DataFrame:
    """
    Trim sku and variant_name columns.
    """
    for col_name in ["sku", "variant_name"]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.trim(F.col(col_name)))
    return df


def validate_inventory_quantities(df: DataFrame) -> DataFrame:
    """
    Ensure on_hand_qty and reserved_qty are non-negative.
    Adds *_was_negative flags via enforce_non_negative_columns.
    """
    cols = [c for c in ["on_hand_qty", "reserved_qty"] if c in df.columns]
    if not cols:
        return df
    return enforce_non_negative_columns(df, cols)


# ============================================================
# Order & Payment cleaning functions
# ============================================================

def validate_order_monetary_non_negative(df: DataFrame) -> DataFrame:
    """
    Ensure order monetary columns are non-negative.
    """
    cols = [
        "total_item_amount",
        "total_discount_amount",
        "total_tax_amount",
        "total_shipping_amount",
        "grand_total_amount",
    ]
    cols = [c for c in cols if c in df.columns]
    if not cols:
        return df
    return enforce_non_negative_columns(df, cols)


def add_order_total_mismatch_flag(df: DataFrame) -> DataFrame:
    """
    Check if grand_total_amount ≈ computed sum of components.
    Adds: amount_mismatch_flag (bool)
    """
    required_cols = [
        "total_item_amount",
        "total_discount_amount",
        "total_tax_amount",
        "total_shipping_amount",
        "grand_total_amount",
    ]
    if any(c not in df.columns for c in required_cols):
        return df

    computed_total = (
        F.col("total_item_amount")
        - F.col("total_discount_amount")
        + F.col("total_tax_amount")
        + F.col("total_shipping_amount")
    )

    df = df.withColumn(
        "amount_mismatch_flag",
        F.abs(F.col("grand_total_amount") - computed_total) > F.lit(0.01)
    )
    return df


def validate_order_status_values(df: DataFrame) -> DataFrame:
    """
    Simple validity flag for order_status_code.
    Adds: order_status_is_valid (bool)
    """
    if "order_status_code" not in df.columns:
        return df

    allowed = ["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED"]
    df = df.withColumn(
        "order_status_is_valid",
        F.col("order_status_code").isin(allowed)
    )
    return df


def validate_payment_amounts(df: DataFrame) -> DataFrame:
    """
    Ensure payment.amount is non-negative and flag invalid statuses if needed.
    """
    if "amount" in df.columns:
        df = enforce_non_negative_columns(df, ["amount"])

    if "payment_status_code" in df.columns:
        allowed = ["PENDING", "SUCCESS", "FAILED", "REFUNDED"]
        df = df.withColumn(
            "payment_status_is_valid",
            F.col("payment_status_code").isin(allowed)
        )
    return df


def validate_refund_amounts(df: DataFrame) -> DataFrame:
    """
    Ensure refund_amount is non-negative.
    """
    if "refund_amount" in df.columns:
        df = enforce_non_negative_columns(df, ["refund_amount"])
    return df


# ============================================================
# Review (file source) cleaning functions
# ============================================================

def clean_review_ratings(df: DataFrame) -> DataFrame:
    """
    Cast rating to int and clip to [1,5].
    Adds: rating_was_out_of_range (bool)
    """
    if "rating" not in df.columns:
        return df

    df = cast_column_to_integer(df, "rating")
    df = df.withColumn(
        "rating_was_out_of_range",
        (F.col("rating") < 1) | (F.col("rating") > 5)
    )
    df = clip_column_to_range(df, "rating", 1, 5)
    return df


def sanitize_review_text(df: DataFrame, max_length: int = 5000) -> DataFrame:
    """
    Trim review_text and cap its length to avoid extreme values.
    Adds: review_text_was_truncated (bool)
    """
    if "review_text" not in df.columns:
        return df

    df = df.withColumn("review_text", F.trim(F.col("review_text")))

    df = df.withColumn(
        "review_text_was_truncated",
        F.length(F.col("review_text")) > max_length
    ).withColumn(
        "review_text",
        F.when(
            F.length(F.col("review_text")) > max_length,
            F.substring(F.col("review_text"), 1, max_length)
        ).otherwise(F.col("review_text"))
    )
    return df


def validate_review_status_values(df: DataFrame) -> DataFrame:
    """
    Validity flag for review status.
    Adds: review_status_is_valid (bool)
    """
    if "status" not in df.columns:
        return df

    allowed = ["PUBLISHED", "PENDING", "REJECTED"]
    df = df.withColumn(
        "review_status_is_valid",
        F.col("status").isin(allowed)
    )
    return df


def cast_review_date(df: DataFrame) -> DataFrame:
    """
    Cast review_date to timestamp.
    """
    if "review_date" not in df.columns:
        return df
    return cast_column_to_timestamp(df, "review_date")


# ============================================================
# Orchestration: apply cleaning per table/DataFrame
# ============================================================

def clean_df_generic(df: DataFrame) -> DataFrame:
    """
    Minimal generic cleaning applied to ALL tables:
    - trim strings
    - replace "" with null
    - add ingestion_ts
    """
    df = trim_all_string_columns(df)
    df = replace_empty_strings_with_nulls(df)
    df = add_ingestion_ts_column(df)
    return df


def clean_customer_df(df: DataFrame) -> DataFrame:
    df = clean_df_generic(df)
    df = normalize_customer_name_case(df)
    df = clean_customer_email_format(df)
    df = clean_customer_phone_format(df)
    df = validate_customer_status_values(df)
    df = drop_rows_with_nulls_in_keys(df, ["customer_id"])
    df = drop_duplicate_rows_by_keys(df, ["customer_id"])
    return df


def clean_orders_df(df: DataFrame) -> DataFrame:
    df = clean_df_generic(df)
    df = validate_order_monetary_non_negative(df)
    df = add_order_total_mismatch_flag(df)
    df = validate_order_status_values(df)
    df = cast_column_to_timestamp(df, "order_date")
    df = drop_rows_with_nulls_in_keys(df, ["order_id"])
    df = drop_duplicate_rows_by_keys(df, ["order_id"])
    return df


def clean_order_item_df(df: DataFrame) -> DataFrame:
    df = clean_df_generic(df)
    df = enforce_non_negative_columns(
        df,
        ["quantity", "base_unit_price", "discount_amount", "tax_amount", "shipping_amount", "line_total_amount"]
    )
    df = drop_rows_with_nulls_in_keys(df, ["order_item_id"])
    df = drop_duplicate_rows_by_keys(df, ["order_item_id"])
    return df


def clean_product_df(df: DataFrame) -> DataFrame:
    df = clean_df_generic(df)
    df = normalize_product_name(df)
    df = drop_rows_with_nulls_in_keys(df, ["product_id"])
    df = drop_duplicate_rows_by_keys(df, ["product_id"])
    return df


def clean_product_variant_df(df: DataFrame) -> DataFrame:
    df = clean_df_generic(df)
    df = trim_sku_and_variant_name(df)
    df = drop_rows_with_nulls_in_keys(df, ["product_variant_id"])
    df = drop_duplicate_rows_by_keys(df, ["product_variant_id"])
    return df


def clean_inventory_level_df(df: DataFrame) -> DataFrame:
    df = clean_df_generic(df)
    df = validate_inventory_quantities(df)
    df = drop_rows_with_nulls_in_keys(df, ["warehouse_id", "product_variant_id"])
    df = drop_duplicate_rows_by_keys(df, ["warehouse_id", "product_variant_id"])
    return df


def clean_payment_df(df: DataFrame) -> DataFrame:
    df = clean_df_generic(df)
    df = validate_payment_amounts(df)
    df = cast_column_to_timestamp(df, "payment_date")
    df = drop_rows_with_nulls_in_keys(df, ["payment_id"])
    df = drop_duplicate_rows_by_keys(df, ["payment_id"])
    return df


def clean_refund_df(df: DataFrame) -> DataFrame:
    df = clean_df_generic(df)
    df = validate_refund_amounts(df)
    df = cast_column_to_timestamp(df, "refund_date")
    df = drop_rows_with_nulls_in_keys(df, ["refund_id"])
    df = drop_duplicate_rows_by_keys(df, ["refund_id"])
    return df


def clean_product_attributes_df(df: DataFrame) -> DataFrame:
    """
    Cleaning for file-source product_attributes:
    - generic trimming/nulls
    - cast effective_from / effective_to
    - basic key null/drop
    """
    df = clean_df_generic(df)

    for col_name in ["effective_from", "effective_to"]:
        df = cast_column_to_timestamp(df, col_name)

    df = drop_rows_with_nulls_in_keys(df, ["product_id"])
    df = drop_duplicate_rows_by_keys(df, ["product_id", "effective_from"])
    return df


def clean_product_reviews_df(df: DataFrame) -> DataFrame:
    """
    Cleaning for file-source product_reviews:
    - generic trimming/nulls
    - rating range enforcement
    - review text sanitization
    - review status validity
    - cast review_date
    """
    df = clean_df_generic(df)
    df = clean_review_ratings(df)
    df = sanitize_review_text(df)
    df = validate_review_status_values(df)
    df = cast_review_date(df)
    df = drop_rows_with_nulls_in_keys(df, ["review_id"])
    df = drop_duplicate_rows_by_keys(df, ["review_id"])
    return df


# Mapping from logical table name to its cleaner
TABLE_CLEANERS = {
    "customer": clean_customer_df,
    "orders": clean_orders_df,
    "order_item": clean_order_item_df,
    "product": clean_product_df,
    "product_variant": clean_product_variant_df,
    "inventory_level": clean_inventory_level_df,
    "payment": clean_payment_df,
    "refund": clean_refund_df,
    "product_attributes": clean_product_attributes_df,
    "product_reviews": clean_product_reviews_df,
    # You can add more table-specific cleaners here as needed
}


def clean_all_dataframes(all_dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
    """
    Entry point used by main.py.
    Applies:
      - generic cleaning to all DataFrames
      - table-specific cleaning where a cleaner is defined
    Returns a new dict with cleaned DataFrames.
    """
    cleaned: Dict[str, DataFrame] = {}

    for name, df in all_dfs.items():
        print(f"[CLEAN] Cleaning DataFrame '{name}'")

        if name in TABLE_CLEANERS:
            cleaned_df = TABLE_CLEANERS[name](df)
        else:
            # For lookup tables and others: just generic cleaning
            cleaned_df = clean_df_generic(df)

        cleaned[name] = cleaned_df

    print(f"[CLEAN] Finished cleaning {len(cleaned)} DataFrame(s).")
    return cleaned
