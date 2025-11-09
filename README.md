# health-data-pipeline-iceberg

This repository contains a PySpark + AWS Glue compatible reference implementation for
building an Iceberg-backed health analytics lakehouse. The project loads clinical
encounter and medication CSV files into Apache Iceberg tables, supports idempotent
upserts, and includes a maintenance job for compacting small files.

## Repository layout

```
├── config/               # Shared JSON configuration for Glue & local jobs
├── docker/               # Docker Compose runtime for Spark + Hadoop services
├── infra/                # Athena DDL helpers
├── input/                # Sample CSV data for local development
├── jobs/                 # PySpark / Glue ETL and maintenance jobs
├── warehouse/            # Local Iceberg warehouse output (ignored in Git)
└── Makefile              # Local automation helpers
```

## Prerequisites

* Docker and Docker Compose plugin
* GNU Make
* (For AWS) An S3 bucket (e.g., `s3://demo-bucket`) and an AWS Glue Data Catalog
* AWS credentials with rights to access S3, Glue, and Athena when running in the cloud

## Configuration

All jobs read from a shared configuration file: [`config/job_args.json`](config/job_args.json).
Each job has `local` and `cloud` sections. The `local` block targets the included
`./warehouse` directory, while the `cloud` block points to the demo `s3://demo-bucket/warehouse` path.
Adjust the paths, database names, or rewrite tuning parameters as needed.

## Running locally with Docker

1. Start the Spark and Hadoop services and run both ingestion jobs:
   ```bash
   make run-local
   ```
   The command boots the containers defined in [`docker/docker-compose.yml`](docker/docker-compose.yml)
   and submits each job via `spark-submit` with the Iceberg runtime dependency.

2. Inspect the generated Iceberg tables using Spark SQL:
   ```bash
   docker compose -f docker/docker-compose.yml exec spark \
     /opt/spark/bin/spark-sql \
     --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \
     -e "SELECT * FROM health.healthcare.encounters"
   ```

3. Tear down the local environment when finished:
   ```bash
   make down
   ```

The output tables land under `./warehouse/healthcare/encounters` and
`./warehouse/healthcare/medications`.

## AWS Glue execution

1. Upload the contents of this repository (or package with `zip`) to S3.
2. Create an AWS Glue job for each script in the [`jobs/`](jobs) directory.
   * Job type: Spark
   * Glue version: 4.0 or later
   * Python file: e.g., `jobs/ingest_encounters.py`
   * Default arguments:
     ```
     --job-args s3://demo-bucket/config/job_args.json
     --env cloud
     ```
3. Provide the Iceberg runtime as an additional Python file or wheel or leverage
   the Glue 4.0 built-in Iceberg runtime. No code changes are required; the scripts
   automatically detect AWS Glue and reuse the provided `GlueContext`.
4. Schedule the `jobs/compact.py` job to run after large ingestion batches to merge
   small files and clean up Iceberg metadata.

## Athena Iceberg catalog

The [`infra/athena-ddl.sql`](infra/athena-ddl.sql) file contains example statements to
register the Iceberg tables in Athena and sample queries once data is available.
You can print the SQL locally via:

```bash
make athena-ddl
```

## Sample data

Two CSV files in [`input/`](input) provide deterministic fixtures for local testing.
Feel free to replace these with extracts from your own source systems:

* `input/encounters.csv` — patient encounters, partitioned by `days(admit_at)`
* `input/medications.csv` — medication orders, partitioned by `bucket(16, normalized_med_id)`

## Maintenance job

Run the compaction job on demand to merge small files and remove orphan data:

```bash
docker compose -f docker/docker-compose.yml exec spark \
  /opt/spark/bin/spark-submit \
  --master local[*] \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3 \
  jobs/compact.py --env local --job-args config/job_args.json
```

Adjust the `compact.cloud` block in `config/job_args.json` when running the job in AWS Glue.

## Troubleshooting

* Ensure the Iceberg runtime package version in the Makefile matches your Spark minor version.
* When running in AWS Glue, grant IAM permissions for S3, Glue Data Catalog, and CloudWatch Logs.
* For large historical backfills, tune the `rewrite_options` in the compaction configuration to
  increase target file sizes or adjust the minimum number of input files per rewrite task.

## Cleaning up

Use `make down` to stop containers and free local resources. Remove the `warehouse/` directory
if you want to reset the Iceberg tables.
