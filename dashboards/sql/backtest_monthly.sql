-- needs: backtest
-- Day-ahead error by month of the forecast day, against the naive benchmark and the
-- price level.
SELECT
  date_trunc('MONTH', date_add(issue_date, days_ahead))   AS month,
  round(avg(abs(estimate - actual)), 1)                    AS model_error,
  round(avg(abs(last_7d_price - actual)), 1)               AS last_7_days_error,
  round(avg(actual), 0)                                    AS avg_price
FROM scored
WHERE days_ahead = 1
GROUP BY 1
ORDER BY 1
