CREATE TABLE IF NOT EXISTS gold_agg_daily_route (
  origin         string,
  dest           string,
  event_date     date,
  flights        bigint,
  otp_pct        double,
  avg_delay_min  double
)
PARTITIONED BY (event_date)
LOCATION 's3://air-curated-test/gold/agg_daily_route/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet'
);
