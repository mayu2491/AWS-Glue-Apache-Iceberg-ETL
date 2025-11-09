"""Utility job to compact small Iceberg data files."""
from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession

JOB_KEY = "compact"


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


def compact_table(
    spark: SparkSession, catalog_name: str, database: str, table: str, options: Dict[str, str]
) -> None:
    table_identifier = f"{database}.{table}"
    statement = f"CALL {catalog_name}.system.rewrite_data_files(table => '{table_identifier}'"
    if options:
        options_sql = ", ".join([f"'{key}', '{value}'" for key, value in options.items()])
        statement += f", options => map({options_sql})"
    statement += ")"
    spark.sql(statement)


def clean_metadata(
    spark: SparkSession, catalog_name: str, database: str, table: str, retain_hours: int
) -> None:
    table_identifier = f"{database}.{table}"
    spark.sql(
        f"CALL {catalog_name}.system.expire_snapshots(table => '{table_identifier}', retain_last => 1)"
    )
    spark.sql(
        f"CALL {catalog_name}.system.remove_orphan_files(table => '{table_identifier}', older_than => (current_timestamp() - INTERVAL {retain_hours} HOURS))"
    )


def run(job_args: Dict[str, Any]) -> None:
    catalog_name = job_args.get("catalog", "health")
    database = job_args["database"]
    table = job_args["table"]
    warehouse_path = normalize_warehouse_path(job_args["warehouse_path"])
    retain_hours = int(job_args.get("retain_hours", 24))
    rewrite_options = job_args.get(
        "rewrite_options",
        {
            "min-input-files": "2",
            "min-input-file-size-bytes": str(128 * 1024 * 1024),
        },
    )

    spark = build_spark_session("compact-iceberg-table", catalog_name, warehouse_path)

    compact_table(spark, catalog_name, database, table, rewrite_options)
    clean_metadata(spark, catalog_name, database, table, retain_hours)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compact small files in an Iceberg table")
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
