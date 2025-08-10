SELECT origin, dest, event_date, flights, otp_pct, avg_delay_min
FROM gold_agg_daily_route
WHERE event_date = DATE '2025-08-08'
ORDER BY origin, dest;
