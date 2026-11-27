-- GB against its interconnected neighbours, last 180 days, one row per market and day.
SELECT date, market, round(price, 1) AS price
FROM fact_market_daily
LATERAL VIEW stack(4,
  'GB',           gb_market_index_price_gbp_mwh,
  'France',       fr_avg_price_gbp_mwh,
  'Netherlands',  nl_avg_price_gbp_mwh,
  'Norway (NO2)', no2_avg_price_gbp_mwh) AS market, price
WHERE date >= date_sub(current_date(), 180)
  AND is_complete_day
  AND price IS NOT NULL
ORDER BY date, market
