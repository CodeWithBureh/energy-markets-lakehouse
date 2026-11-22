-- Week-ahead GB power price report: one row for each of the next 7 days.
--
-- Run it daily after the energy_daily job (about 08:30 UK), e.g. as a scheduled Databricks
-- SQL query, a dashboard dataset or an alert. Replace `energy` with your catalog.
--
-- How the estimate works
--   price = a + b1 * residual demand (GW) + b2 * gas (SAP, GBP/MWh, previous gas day)
--   fitted on the 60 complete days before the forecast was issued, then applied to the
--   forecast residual demand and the latest SAP. Tested walk-forward over 2026 (refit
--   daily), this missed the daily market index price by about 13.6 GBP/MWh on average,
--   against 16.4 without gas.
--   Residual demand = demand minus metered (transmission) wind. Demand comes from Elexon's
--   2-14 day peak forecast (NDFD) scaled to a daily average; wind from Elexon's WINDFOR
--   (corrected) where it covers the day, otherwise from the Open-Meteo wind forecast through
--   the lakehouse's own power curve.
--   The low/high range is +/- one standard deviation of the fit's errors. Days 4-7 carry
--   extra wind-forecast uncertainty that the range does not include.
--   Gas is held at the latest SAP for the whole week.

WITH latest_issue AS (
  SELECT max(issue_date) AS issue_date FROM energy.gold.fact_gb_week_ahead_outlook
),

history AS (
  SELECT
    m.date,
    m.gb_market_index_price_gbp_mwh                       AS y,
    m.gb_avg_residual_demand_mw / 1000                    AS x1,   -- GW
    lag(m.gb_gas_sap_gbp_mwh) OVER (ORDER BY m.date)      AS x2    -- previous gas day
  FROM energy.gold.fact_market_daily m
  WHERE m.is_complete_day
),

train AS (
  SELECT h.*
  FROM history h CROSS JOIN latest_issue i
  WHERE h.date <  i.issue_date
    AND h.date >= date_sub(i.issue_date, 60)
    AND h.y IS NOT NULL AND h.x1 IS NOT NULL AND h.x2 IS NOT NULL
),

-- Least squares with two regressors, from covariances (closed form).
moments AS (
  SELECT
    count(*) AS n,
    avg(y) AS my, avg(x1) AS m1, avg(x2) AS m2,
    var_pop(x1) AS v1, var_pop(x2) AS v2,
    covar_pop(x1, x2) AS c12, covar_pop(x1, y) AS c1y, covar_pop(x2, y) AS c2y
  FROM train
),

model AS (
  SELECT
    n,
    (c1y * v2 - c2y * c12) / (v1 * v2 - c12 * c12) AS b1,
    (c2y * v1 - c1y * c12) / (v1 * v2 - c12 * c12) AS b2,
    my, m1, m2
  FROM moments
),

coefs AS (
  SELECT n, b1, b2, my - b1 * m1 - b2 * m2 AS a FROM model
),

fit_error AS (
  SELECT stddev_pop(t.y - (c.a + c.b1 * t.x1 + c.b2 * t.x2)) AS sd
  FROM train t CROSS JOIN coefs c
),

recent AS (
  SELECT
    avg(gb_market_index_price_gbp_mwh)   AS last_7d_price,
    avg(gb_avg_residual_demand_mw)/1000  AS last_7d_residual_gw
  FROM energy.gold.fact_market_daily m CROSS JOIN latest_issue i
  WHERE m.date >= date_sub(i.issue_date, 7) AND m.date < i.issue_date
)

SELECT
  o.target_date                                         AS day,
  date_format(o.target_date, 'EEE')                     AS weekday,
  o.days_ahead,
  round(o.demand_avg_forecast_mw / 1000, 1)             AS demand_gw,
  round(o.wind_forecast_mw / 1000, 1)                   AS wind_gw,
  o.wind_source,
  round(o.residual_demand_forecast_mw / 1000, 1)        AS residual_demand_gw,
  round(o.avg_wind_speed_100m_ms, 1)                    AS wind_speed_ms,
  round(o.avg_temp_c, 1)                                AS temp_c,
  round(o.latest_sap_gbp_mwh, 1)                        AS gas_sap_gbp_mwh,
  round(o.latest_ttf_eur_mwh, 1)                        AS gas_ttf_eur_mwh,
  round(c.a + c.b1 * o.residual_demand_forecast_mw / 1000 + c.b2 * o.latest_sap_gbp_mwh, 0)
                                                        AS price_estimate_gbp_mwh,
  round(c.a + c.b1 * o.residual_demand_forecast_mw / 1000 + c.b2 * o.latest_sap_gbp_mwh - e.sd, 0)
                                                        AS price_low_gbp_mwh,
  round(c.a + c.b1 * o.residual_demand_forecast_mw / 1000 + c.b2 * o.latest_sap_gbp_mwh + e.sd, 0)
                                                        AS price_high_gbp_mwh,
  round(r.last_7d_price, 0)                             AS last_7_days_avg_price_gbp_mwh,
  -- Model context, the same on every row
  round(c.b1, 2)                                        AS gbp_per_gw_residual,
  round(c.b2, 2)                                        AS gbp_per_gbp_gas,
  c.n                                                   AS days_in_fit,
  o.issue_date,
  o.issued_utc                                          AS weather_issued_utc,
  o.demand_forecast_published_utc
FROM energy.gold.fact_gb_week_ahead_outlook o
JOIN latest_issue i ON o.issue_date = i.issue_date
CROSS JOIN coefs c
CROSS JOIN fit_error e
CROSS JOIN recent r
ORDER BY o.target_date;
