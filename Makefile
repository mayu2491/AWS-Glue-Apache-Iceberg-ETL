PROJECT_NAME ?= health-data-pipeline-iceberg
COMPOSE ?= docker compose -f docker/docker-compose.yml
SPARK_SUBMIT ?= /opt/spark/bin/spark-submit
ICEBERG_PACKAGE ?= org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3

.PHONY: run-local athena-ddl down

run-local:
	$(COMPOSE) up -d
	$(COMPOSE) exec spark $(SPARK_SUBMIT) \
		--master local[*] \
		--packages $(ICEBERG_PACKAGE) \
		jobs/ingest_encounters.py --env local --job-args config/job_args.json
	$(COMPOSE) exec spark $(SPARK_SUBMIT) \
		--master local[*] \
		--packages $(ICEBERG_PACKAGE) \
		jobs/ingest_medications.py --env local --job-args config/job_args.json

athena-ddl:
	@cat infra/athena-ddl.sql

down:
	$(COMPOSE) down
