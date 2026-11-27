-- needs: backtest
-- The day-ahead estimate against what happened, for the last 120 days.
SELECT
  date_add(issue_date, days_ahead)   AS day,
  round(actual, 1)                   AS actual_price,
  round(estimate, 1)                 AS day_ahead_estimate
FROM scored
WHERE days_ahead = 1
  AND date_add(issue_date, days_ahead) >= date_sub(current_date(), 120)
ORDER BY day
