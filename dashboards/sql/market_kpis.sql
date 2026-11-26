-- One row of headline numbers for the counters.
WITH days AS (
  SELECT date, gb_market_index_price_gbp_mwh AS price, avg_wind_share
  FROM fact_market_daily
  WHERE is_complete_day AND gb_market_index_price_gbp_mwh IS NOT NULL
)
SELECT
  max(date)                                                                     AS latest_day,
  round(max_by(price, date), 0)                                                 AS latest_price,
  round(avg(CASE WHEN date > date_sub(current_date(), 31) THEN price END), 0)   AS avg_price_30d,
  round(avg(CASE WHEN date > date_sub(current_date(), 396)
                  AND date <= date_sub(current_date(), 365) THEN price END), 0) AS avg_price_30d_last_year,
  round(avg(CASE WHEN date > date_sub(current_date(), 31) THEN avg_wind_share END), 3)
                                                                                AS wind_share_30d
FROM days
