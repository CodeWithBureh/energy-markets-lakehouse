-- Starter queries for the AI/BI dashboard. Replace `energy` with your catalog.

-- 1. Do price spikes show up in share prices? Biggest GB price days and same-day returns
SELECT m.date, m.gb_avg_price_gbp_mwh, m.gb_max_price_gbp_mwh, m.avg_wind_share,
       r.ticker, r.company, ROUND(r.daily_return * 100, 2) AS return_pct
FROM energy.gold.fact_market_daily m
JOIN energy.gold.fact_stock_returns_daily r ON r.trade_date = m.date
WHERE m.date >= date_sub(current_date(), 365)
ORDER BY m.gb_max_price_gbp_mwh DESC
LIMIT 50;

-- 2. Rolling 60-day correlation of each stock with GB power and gas moves
SELECT date, ticker, corr60_return_vs_gb_power, corr60_return_vs_henry_hub
FROM energy.gold.mart_stocks_vs_market
WHERE obs_in_window >= 40
ORDER BY date, ticker;

-- 3. Wind vs price: classic merit-order effect
SELECT date_trunc('hour', start_time_utc) AS hour, AVG(wind_share) AS wind_share,
       AVG(system_price_gbp_mwh) AS price, AVG(avg_wind_speed_100m_ms) AS wind_speed_ms
FROM energy.gold.fact_gb_half_hourly
GROUP BY 1;

-- 4. GB vs neighbours (GBP/MWh)
SELECT date, gb_avg_price_gbp_mwh, fr_avg_price_gbp_mwh, nl_avg_price_gbp_mwh,
       no2_avg_price_gbp_mwh, gb_fr_spread_gbp_mwh
FROM energy.gold.fact_market_daily ORDER BY date;

-- 5. FinOps: what this project costs (needs access to system tables; run in prod)
SELECT usage_date,
       billing_origin_product,
       SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= date_sub(current_date(), 30)
  AND (custom_tags['project'] = 'energy-markets-lakehouse'
       OR usage_metadata.dlt_pipeline_id IS NOT NULL)
GROUP BY ALL
ORDER BY usage_date;

-- 5b. Turn DBUs into money with list prices
SELECT u.usage_date, u.billing_origin_product,
       SUM(u.usage_quantity * p.pricing.default) AS list_cost
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
WHERE u.usage_date >= date_sub(current_date(), 30)
GROUP BY ALL
ORDER BY u.usage_date;
