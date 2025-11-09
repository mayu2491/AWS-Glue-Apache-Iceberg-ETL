-- Athena Iceberg database and table definitions for the health data warehouse.
CREATE DATABASE IF NOT EXISTS health_iceberg
LOCATION 's3://demo-bucket/warehouse/healthcare';

-- Encounters Iceberg table pointing to the Glue managed metadata.
CREATE TABLE IF NOT EXISTS health_iceberg.encounters (
    encounter_id string,
    patient_id string,
    admit_at timestamp,
    discharge_at timestamp,
    provider string,
    diagnosis string,
    updated_at timestamp,
    ingested_at timestamp
)
PARTITIONED BY (days(admit_at))
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format-version'='2'
);

-- Medications Iceberg table with bucket partitioning.
CREATE TABLE IF NOT EXISTS health_iceberg.medications (
    medication_order_id string,
    patient_id string,
    med_id string,
    med_name string,
    dose string,
    route string,
    ordered_at timestamp,
    updated_at timestamp,
    normalized_med_id string,
    ingested_at timestamp
)
PARTITIONED BY (bucket(16, normalized_med_id))
TBLPROPERTIES (
    'table_type'='ICEBERG',
    'format-version'='2'
);

-- Sample queries
SELECT *
FROM health_iceberg.encounters
WHERE admit_at >= date('2023-01-01')
ORDER BY admit_at
LIMIT 100;

SELECT patient_id,
       med_name,
       count(*) AS orders
FROM health_iceberg.medications
GROUP BY patient_id, med_name
ORDER BY orders DESC
LIMIT 100;
