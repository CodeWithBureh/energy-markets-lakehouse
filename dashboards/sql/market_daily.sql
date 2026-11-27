-- Complete days over the last two years: price and its two main drivers.
SELECT
  date,
  CAST(year(date) AS STRING)                    AS year,
  round(gb_market_index_price_gbp_mwh, 1)       AS price,
  round(gb_avg_residual_demand_mw / 1000, 2)    AS residual_demand_gw,
  round(avg_wind_share * 100, 1)                AS wind_share_pct,
  round(gb_gas_sap_gbp_mwh, 1)                  AS gas_sap,
  round(ttf_gbp_mwh, 1)                         AS gas_ttf
FROM fact_market_daily
WHERE is_complete_day
  AND date >= add_months(current_date(), -24)
ORDER BY date
