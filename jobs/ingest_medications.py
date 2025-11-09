"""Ingestion job for medications into Iceberg with bucket partitioning."""
from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

JOB_KEY = "ingest_medications"


def load_job_args(job_args_path: str, env: str) -> Dict[str, Any]:
    with open(job_args_path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    try:
        job_conf = payload[JOB_KEY][env]
    except KeyError as exc:  # pragma: no cover - defensive for misconfiguration
        raise KeyError(
            f"Configuration for job '{JOB_KEY}' and environment '{env}' was not found."
        ) from exc
    return job_conf


def normalize_warehouse_path(path: str) -> str:
    if path.startswith("s3://"):
        return path.replace("s3://", "s3a://", 1)
    resolved = Path(path).expanduser().resolve()
    return resolved.as_uri()


def normalize_source_path(path: str) -> str:
    if path.startswith("s3://"):
        return path.replace("s3://", "s3a://", 1)
    return path


def build_spark_session(app_name: str, catalog_name: str, warehouse_path: str) -> SparkSession:
    base_conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{catalog_name}.type": "hadoop",
        f"spark.sql.catalog.{catalog_name}.warehouse": warehouse_path,
    }

    builder = SparkSession.builder.appName(app_name)
    for key, value in base_conf.items():
        builder = builder.config(key, value)

    if importlib.util.find_spec("awsglue"):
        from awsglue.context import GlueContext  # type: ignore

        spark_session = builder.getOrCreate()
        glue_context = GlueContext(spark_session.sparkContext)
        return glue_context.spark_session

    return builder.getOrCreate()


def transform_medications(df: DataFrame) -> DataFrame:
    normalized_source = F.when(
        F.col("med_id").isNotNull() & (F.length(F.col("med_id")) > 0),
        F.col("med_id"),
    ).otherwise(F.col("med_name"))

    cleaned = (
        df.withColumn("medication_order_id", F.col("medication_order_id").cast("string"))
        .withColumn("patient_id", F.col("patient_id").cast("string"))
        .withColumn("med_id", F.col("med_id").cast("string"))
        .withColumn("ordered_at", F.to_timestamp("ordered_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn(
            "normalized_med_id",
            F.regexp_replace(F.lower(normalized_source), "[^a-z0-9]", ""),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )
    return cleaned


def ensure_namespace(spark: SparkSession, catalog_name: str, database: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog_name}.{database}")


def table_exists(spark: SparkSession, full_table_name: str) -> bool:
    try:
        spark.table(full_table_name)
    except Exception:  # pragma: no cover - spark throws AnalysisException
        return False
    return True


def write_initial(df: DataFrame, full_table_name: str) -> None:
    (
        df.writeTo(full_table_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("bucket(16, normalized_med_id)")
        .createOrReplace()
    )


def merge_updates(spark: SparkSession, df: DataFrame, full_table_name: str) -> None:
    df.createOrReplaceTempView("incoming_medications")
    spark.sql(
        f"""
        MERGE INTO {full_table_name} AS target
        USING incoming_medications AS source
        ON target.medication_order_id = source.medication_order_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def run(job_args: Dict[str, Any]) -> None:
    catalog_name = job_args.get("catalog", "health")
    database = job_args["database"]
    table = job_args.get("table", "medications")
    warehouse_path = normalize_warehouse_path(job_args["warehouse_path"])

    spark = build_spark_session("ingest-medications", catalog_name, warehouse_path)

    ensure_namespace(spark, catalog_name, database)

    source_path = normalize_source_path(job_args["source_path"])
    df = spark.read.option("header", True).csv(source_path)
    transformed = transform_medications(df)

    full_table_name = f"{catalog_name}.{database}.{table}"

    if table_exists(spark, full_table_name):
        merge_updates(spark, transformed, full_table_name)
    else:
        write_initial(transformed, full_table_name)


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest medication data into Iceberg")
    parser.add_argument("--job-args", required=True, help="Path to the shared JSON job arguments")
    parser.add_argument(
        "--env",
        default="local",
        choices=["local", "cloud"],
        help="Execution environment that selects a configuration block",
    )
    args = parser.parse_args()

    job_args = load_job_args(args.job_args, args.env)
    run(job_args)


if __name__ == "__main__":
    main()
