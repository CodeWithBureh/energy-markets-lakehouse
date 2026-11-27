-- needs: backtest
-- One row of headline numbers for the counters.
SELECT
  round(avg(CASE WHEN days_ahead = 1 THEN abs(estimate - actual) END), 1)       AS day1_error,
  round(avg(CASE WHEN days_ahead = 1 THEN abs(last_7d_price - actual) END), 1)  AS day1_naive_error,
  round(1 - avg(CASE WHEN days_ahead = 1 THEN abs(estimate - actual) END)
          / avg(CASE WHEN days_ahead = 1 THEN abs(last_7d_price - actual) END), 3)
                                                                                AS day1_improvement,
  round(avg(CASE WHEN days_ahead = 7 THEN abs(estimate - actual) END), 1)       AS day7_error,
  count(DISTINCT issue_date)                                                    AS issues_scored
FROM scored
