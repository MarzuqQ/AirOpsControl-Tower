## Aviation-Data-Pipeline 

This repository contains a POC for an AWS Lakehouse pipeline that processes and analyzes airline flight events. The solution demonstrates how to build a Bronze → Silver → Gold data architecture using AWS Glue, Amazon S3, Apache Iceberg, and Amazon Athena without a dedicated infrastructure-as-code template.

### Overview

The pipeline ingests raw JSON flight events into S3 (Bronze layer), processes and deduplicates them with Apache Spark on AWS Glue (Silver layer), and aggregates them into curated datasets for analytics (Gold layer). Data is stored in Apache Iceberg tables for ACID transactions and incremental updates. Athena is used for ad-hoc SQL analysis.

### Architecture

| Layer  | Storage                 | Processing        | Purpose                                 |
|------- |-------------------------|-------------------|-----------------------------------------|
| Bronze | Amazon S3               | —                 | Raw immutable flight events in JSON     |
| Silver | Apache Iceberg on S3    | AWS Glue (Spark)  | Cleaned & deduplicated flight events    |
| Gold   | Apache Iceberg on S3    | AWS Glue (Spark)  | Aggregated analytics-ready KPIs         |
| Query  | Athena                  | —                 | Serverless SQL analytics over Iceberg   |

### AWS Services Used

- **Amazon S3**: Stores raw, curated, and aggregated datasets.
- **AWS Glue**: Runs Spark ETL jobs to transform data between layers.
- **Apache Iceberg**: Manages versioned, transactional tables stored in S3.
- **AWS Glue Data Catalog**: Stores table metadata for Athena and Glue.
- **Amazon Athena**: Runs SQL queries directly on Iceberg tables in S3.

### 🛠 Manual Setup Steps (No Terraform/SAM)

Since this project does not use an infrastructure-as-code template, here’s the manual setup process:

1. **Create S3 Buckets**
   - Raw bucket: `air-raw-test` → stores Bronze JSON events.
   - Curated bucket: `air-curated-test` → stores Silver & Gold Iceberg tables.
   - Example structure in raw bucket:
     - `s3://air-raw-test/events/event_date=2025-08-08/airport=SEA/flights.json`
     - `s3://air-raw-test/events/event_date=2025-08-08/airport=SEA/flights_updates.json`

2. **Create Glue Database**

```sql
CREATE DATABASE air_db_demo;
```

3. **Create Silver & Gold Tables (Athena)**

From `athena/sql/` run:

```sql
-- Silver
CREATE TABLE air_db_demo.silver_fact_flight_event (...) 
USING iceberg
LOCATION 's3://air-curated-test/silver_fact_flight_event';

-- Gold
CREATE TABLE air_db_demo.gold_kpi_stat_overview (...) 
USING iceberg
LOCATION 's3://air-curated-test/gold_kpi_stat_overview';
```

4. **Configure Glue Job Scripts**

Upload `glue/bronze_to_silver.py` and `glue/silver_to_gold.py` to S3 or paste directly into Glue Studio scripts.

- Set IAM role with S3 (read/write) + Glue + Athena permissions.
- Set Spark parameters for Iceberg support:

```bash
--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
--conf spark.sql.catalog.glue_catalog.warehouse=s3://air-curated-test/
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
--conf spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive=true
```

5. **Load Sample Data**

Copy from `data/sample/events/` into your raw bucket, preserving folder structure.

6. **Run ETL Jobs**

- Bronze → Silver: Deduplicates latest flight events per flight ID.
- Silver → Gold: Aggregates delay metrics and KPIs.

7. **Query with Athena**

From `athena/sql/` run:

```sql
-- View all flights in Silver
SELECT * FROM air_db_demo.silver_all_flights;

-- View KPIs in Gold
SELECT * FROM air_db_demo.gold_kpi_stat_overview;
```

### 📂 Repo Structure

```
aviation-data-pipeline/
├─ glue/
│  ├─ bronze_to_silver.py      # Deduplication & merge to Silver
│  └─ silver_to_gold.py        # Aggregation & merge to Gold
├─ athena/
│  └─ sql/
│     ├─ create_gold.sql
│     ├─ create_silver.sql
│     ├─ gold_kpi_stat_overview.sql
│     └─ silver_all_flights.sql
├─ data/
│  └─ sample/
│     └─ events/event_date=2025-08-08/airport=SEA/
│        ├─ flights.json
│        └─ flights_updates.json
├─ diagram/
│  └─ architecture.mmd
├─ .gitignore
├─ LICENSE
└─ README.md
```

### Validation Checklist

- **Buckets exist** and contain data.
- **Glue database and tables** are created.
- **Bronze → Silver job** runs without errors.
- **Silver → Gold job** runs without errors.
- **Athena queries** return expected data.


