-- Settlement days in the last 30 with fewer half-hours loaded than the day has
-- (46, 48 or 50), before the latest day loaded (which is still filling, as is yesterday
-- until the morning run). Empty when everything is complete. Same rule as energy_checks.
SELECT date, expected_settlement_periods, periods_loaded
FROM dq_settlement_period_completeness
WHERE date >= date_sub(current_date(), 30)
  AND date < (SELECT max(settlement_date) FROM fact_gb_half_hourly)
ORDER BY date
