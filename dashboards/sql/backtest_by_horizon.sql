-- needs: backtest
-- Average error by days ahead over every past forecast: the model against the naive
-- "same as the last 7 days".
SELECT
  days_ahead,
  round(avg(abs(estimate - actual)), 1)        AS model_error,
  round(avg(abs(last_7d_price - actual)), 1)   AS last_7_days_error,
  count(*)                                     AS forecasts
FROM scored
GROUP BY days_ahead
ORDER BY days_ahead
