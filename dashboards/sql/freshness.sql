-- Latest day loaded per source, against how far behind each is allowed to be.
WITH latest AS (
  SELECT 'Elexon: GB prices' AS source, max(settlement_date) AS latest_day, 1 AS allowed_days_behind
  FROM fact_gb_half_hourly WHERE market_index_price_gbp_mwh IS NOT NULL
  UNION ALL
  SELECT 'Elexon: demand forecast', max(to_date(demand_forecast_published_utc)), 1
  FROM fact_gb_week_ahead_outlook
  UNION ALL
  SELECT 'Open-Meteo: weather forecast', max(to_date(issued_utc)), 1
  FROM fact_gb_week_ahead_outlook WHERE NOT is_archived_forecast
  UNION ALL
  SELECT 'ENTSO-E: EU day-ahead prices', max(to_date(hour_utc)), 1
  FROM fact_eu_day_ahead_hourly
  UNION ALL
  SELECT 'National Gas: SAP', max(date), 2
  FROM fact_market_daily WHERE gb_gas_sap_gbp_mwh IS NOT NULL
  UNION ALL
  SELECT 'yfinance: TTF gas', max(date), 4
  FROM fact_market_daily WHERE ttf_eur_mwh IS NOT NULL
  UNION ALL
  SELECT 'FRED: FX rates', max(date), 10
  FROM fact_market_daily WHERE usd_per_gbp IS NOT NULL
  UNION ALL
  SELECT 'yfinance: share prices', max(trade_date), 4
  FROM fact_stock_returns_daily
  UNION ALL
  SELECT 'Copernicus: ERA5 weather', max(to_date(valid_time_utc)), 14
  FROM gb_weather_hourly
)
SELECT
  source,
  latest_day,
  datediff(current_date(), latest_day)   AS days_behind,
  allowed_days_behind,
  CASE WHEN latest_day IS NULL THEN 'No data'
       WHEN datediff(current_date(), latest_day) <= allowed_days_behind THEN 'OK'
       ELSE 'Late' END                   AS status
FROM latest
ORDER BY source
