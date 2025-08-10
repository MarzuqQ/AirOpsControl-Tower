from pyspark.sql import SparkSession, functions as F

# --- CONFIG ---
CURATED_DB = "air_db_demo"
EVENT_DATE = "2025-08-08"   # compute KPIs for this date only for now

# --- SPARK SESSION WITH ICEBERG CATALOG ---
spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://air-curated-test/")
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .getOrCreate()
)

# --- READ FROM SILVER (Iceberg) ---
df = spark.table(f"glue_catalog.{CURATED_DB}.silver_fact_flight_event") \
         .filter(F.col("event_date") == F.to_date(F.lit(EVENT_DATE)))

# --- AGGREGATE INTO DAILY ROUTE KPI ---
agg = (df.groupBy("origin", "dest", "event_date")
       .agg(
            F.count("*").alias("flights"),
            (100.0 * F.avg(F.when(F.col("delay_min") <= 15, 1).otherwise(0))).alias("otp_pct"),
            F.avg("delay_min").alias("avg_delay_min")
        ))

agg.createOrReplaceTempView("daily")

# --- UPSERT (MERGE) INTO GOLD (Iceberg) ---
spark.sql(f"""
MERGE INTO glue_catalog.{CURATED_DB}.gold_agg_daily_route t
USING daily s
ON  t.origin = s.origin
AND t.dest = s.dest
AND t.event_date = s.event_date
WHEN MATCHED THEN UPDATE SET
    t.flights       = s.flights,
    t.otp_pct       = s.otp_pct,
    t.avg_delay_min = s.avg_delay_min
WHEN NOT MATCHED THEN INSERT (
    origin, dest, event_date, flights, otp_pct, avg_delay_min
) VALUES (
    s.origin, s.dest, s.event_date, s.flights, s.otp_pct, s.avg_delay_min
)
""")

print("Silver to Gold aggregation complete.")