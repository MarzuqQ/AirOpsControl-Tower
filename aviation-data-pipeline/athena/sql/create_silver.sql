CREATE TABLE IF NOT EXISTS silver_fact_flight_event (
  flight_id        string,
  origin           string,
  dest             string,
  carrier          string,
  sched_dep_ts     timestamp,
  actual_dep_ts    timestamp,
  sched_arr_ts     timestamp,
  actual_arr_ts    timestamp,
  status           string,
  delay_min        int,
  delay_reason     string,
  event_ts         timestamp,
  event_date       date
)
PARTITIONED BY (event_date)
LOCATION 's3://air-curated-test/silver/fact_flight_event/'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet'
);
