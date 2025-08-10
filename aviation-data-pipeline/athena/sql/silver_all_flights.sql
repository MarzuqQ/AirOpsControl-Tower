SELECT flight_id, origin, dest, status, delay_reason, sched_arr_ts, actual_arr_ts, delay_min, event_ts
FROM silver_fact_flight_event
WHERE event_date = DATE '2025-08-08'
ORDER BY event_ts DESC
LIMIT 50;
