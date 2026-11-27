-- Settlement days in the last 30 with fewer half-hours loaded than the day has
-- (46, 48 or 50). Empty when everything is complete.
SELECT date, expected_settlement_periods, periods_loaded
FROM dq_settlement_period_completeness
WHERE date >= date_sub(current_date(), 30)
  AND date < current_date()
ORDER BY date
