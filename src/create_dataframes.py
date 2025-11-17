# src/create_dataframes.py

import os
import glob
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
import yaml


def load_config(config_path: str, env: str) -> dict:
    """
    Load YAML configuration and return the section for the given environment.

    :param config_path: Path to config.yaml
    :param env: Environment name, e.g. 'dev' or 'prod'
    """
    with open(config_path, "r") as f:
        full_cfg = yaml.safe_load(f)

    if "environments" not in full_cfg:
        raise ValueError("config.yaml must have top-level key 'environments'")

    env_cfg = full_cfg["environments"].get(env)
    if env_cfg is None:
        raise ValueError(f"Environment '{env}' not found in config.yaml")

    return env_cfg


def create_rdbms_dataframes(
    spark: SparkSession,
    rdbms_cfg: dict
) -> Dict[str, DataFrame]:
    """
    Load all RDBMS Parquet datasets from the configured base_dir.

    It expects a folder structure like:
        base_dir/
          customer_parquet/
          orders_parquet/
          payment_parquet/
          ...

    Table name is derived from folder name by stripping the suffix,
    e.g. 'customer_parquet' -> table name 'customer'.
    """
    base_dir = rdbms_cfg["base_dir"]
    suffix = rdbms_cfg.get("parquet_suffix", "_parquet")

    full_base = os.path.abspath(base_dir)
    if not os.path.isdir(full_base):
        raise FileNotFoundError(f"RDBMS base_dir does not exist: {full_base}")

    dataframes: Dict[str, DataFrame] = {}

    pattern = os.path.join(full_base, f"*{suffix}")
    parquet_dirs = glob.glob(pattern)

    if not parquet_dirs:
        print(f"[WARN] No parquet folders found under {full_base} with suffix '{suffix}'")

    for path in parquet_dirs:
        folder_name = os.path.basename(path.rstrip("/"))
        if not folder_name.endswith(suffix):
            # Should not happen due to pattern, but safe-guard
            continue

        table_name = folder_name[: -len(suffix)]  # remove suffix
        print(f"[RDBMS] Loading table '{table_name}' from '{path}'")

        df = spark.read.parquet(path)
        dataframes[table_name] = df

    return dataframes


def create_file_source_dataframes(
    spark: SparkSession,
    file_cfg: dict
) -> Dict[str, DataFrame]:
    """
    Load file-source dataframes (JSON) as defined in config.

    Expects:
        base_dir: "staging/file_source_test_data"
        product_attributes: "product_attributes.json"
        product_reviews: "product_reviews.json"

    Returns:
        {
          "product_attributes": DataFrame,
          "product_reviews": DataFrame
        }
    """
    base_dir = file_cfg["base_dir"]
    base_dir_abs = os.path.abspath(base_dir)

    dfs: Dict[str, DataFrame] = {}

    # Product attributes JSON (array-style JSON, so multiLine = True)
    if "product_attributes" in file_cfg:
        pa_rel = file_cfg["product_attributes"]
        pa_path = os.path.join(base_dir_abs, pa_rel)
        print(f"[FILE] Loading 'product_attributes' from '{pa_path}'")

        pa_df = (
            spark.read
            .option("multiLine", True)   # because JSON is an array, not line-delimited
            .json(pa_path)
        )
        dfs["product_attributes"] = pa_df

    # Product reviews JSON
    if "product_reviews" in file_cfg:
        pr_rel = file_cfg["product_reviews"]
        pr_path = os.path.join(base_dir_abs, pr_rel)
        print(f"[FILE] Loading 'product_reviews' from '{pr_path}'")

        pr_df = (
            spark.read
            .option("multiLine", True)
            .json(pr_path)
        )
        dfs["product_reviews"] = pr_df

    return dfs


def create_all_dataframes(
    spark: SparkSession,
    config_path: str,
    env: str = "dev"
) -> Dict[str, DataFrame]:
    """
    High-level helper to:
      - Load env-specific config
      - Create RDBMS DataFrames (from Parquet)
      - Create file-source DataFrames (from JSON)

    Returns a single dict mapping logical names to DataFrames:
        {
          "customer": df,
          "orders": df,
          ...
          "product_attributes": df,
          "product_reviews": df
        }
    """
    env_cfg = load_config(config_path, env)

    rdbms_cfg = env_cfg["rdbms_source"]
    file_cfg = env_cfg["file_source"]

    rdbms_dfs = create_rdbms_dataframes(spark, rdbms_cfg)
    file_dfs = create_file_source_dataframes(spark, file_cfg)

    # Merge dictionaries; RDBMS and file sources have distinct keys by design
    all_dfs = {**rdbms_dfs, **file_dfs}

    print(f"[INFO] Created {len(all_dfs)} DataFrame(s) for env='{env}'")
    return all_dfs
