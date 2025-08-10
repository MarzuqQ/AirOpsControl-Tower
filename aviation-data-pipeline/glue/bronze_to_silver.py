from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

# --- CONFIG ---
RAW_BUCKET = "air-raw-test"             # raw bucket name (CHANGE THIS ACCORDINGLY)
CURATED_DB = "air_db_demo"              # Glue database name (CHANGE THIS ACCORDINGLY)
EVENT_DATE = "2025-08-08"               # processing date (CHANGE THIS ACCORDINGLY)
AIRPORT = "SEA"                         # specific airport (CHANGE THIS ACCORDINGLY)

# --- SPARK SESSION WITH ICEBERG CATALOG ---
spark = (SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://air-curated-test/")  # curated bucket
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive","true")
    .getOrCreate()
)

# --- READ RAW JSON (all files in the specific date+airport folder) ---
src_path = f"s3://{RAW_BUCKET}/events/event_date={EVENT_DATE}/airport={AIRPORT}/*.json"

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("flight_id", T.StringType()),
    T.StructField("carrier", T.StringType()),
    T.StructField("origin", T.StringType()),
    T.StructField("dest", T.StringType()),
    T.StructField("sched_dep_ts", T.StringType()),
    T.StructField("sched_arr_ts", T.StringType()),
    T.StructField("actual_dep_ts", T.StringType()),
    T.StructField("actual_arr_ts", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("delay_reason", T.StringType()),
    T.StructField("event_ts", T.StringType())
])

df = spark.read.schema(schema).json(src_path)

# --- CAST TIMESTAMPS & DERIVE EVENT DATE ---
to_ts = lambda c: F.to_timestamp(F.col(c))
df = (df
    .withColumn("sched_dep_ts", to_ts("sched_dep_ts"))
    .withColumn("sched_arr_ts", to_ts("sched_arr_ts"))
    .withColumn("actual_dep_ts", to_ts("actual_dep_ts"))
    .withColumn("actual_arr_ts", to_ts("actual_arr_ts"))
    .withColumn("event_ts", to_ts("event_ts"))
    .withColumn("event_date", F.to_date("event_ts"))
)

# --- CALCULATE DELAY MINUTES ---
df = df.withColumn(
    "delay_min",
    F.when(F.col("actual_arr_ts").isNotNull(),
           (F.col("actual_arr_ts").cast("long") - F.col("sched_arr_ts").cast("long")) / 60
    ).cast("int")
)

# --- DEDUPLICATE: KEEP LATEST EVENT FOR EACH FLIGHT ---
w = Window.partitionBy("flight_id").orderBy(F.col("event_ts").desc())
df_latest = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

# --- CREATE TEMP VIEW FOR MERGE ---
df_latest.createOrReplaceTempView("staging_updates")

# --- MERGE INTO ICEBERG SILVER TABLE ---
spark.sql(f"""
MERGE INTO glue_catalog.{CURATED_DB}.silver_fact_flight_event t
USING staging_updates s
ON t.flight_id = s.flight_id
WHEN MATCHED AND t.event_ts < s.event_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("Bronze to Silver merge complete.")