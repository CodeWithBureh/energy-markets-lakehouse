-- Week-ahead GB power price report: one row for each of the next 7 days.
--
-- Run it daily after the energy_daily job (about 08:30 UK), e.g. as a scheduled Databricks
-- SQL query, a dashboard dataset or an alert. Replace `energy` with your catalog.
--
-- How the estimate works
--   model = a + b1 * residual demand (GW) + b2 * gas (SAP, GBP/MWh, previous gas day)
--   fitted on the 60 complete days before the forecast was issued, then applied to the
--   forecast residual demand and the latest SAP.
--   estimate = 75% model + 25% the last 7 days' average price. The blend damps the model
--   when price stops tracking residual demand; it beat the model alone in every year of the
--   backtest (sql/week_ahead_backtest.sql), most on days 5-7, and 20-25% was the best weight
--   in both halves of the history.
--   Residual demand = demand minus metered (transmission) wind. Demand comes from Elexon's
--   2-14 day peak forecast (NDFD) scaled to a daily average; wind from Elexon's WINDFOR
--   (corrected) for the hours it covers, otherwise from the Open-Meteo wind forecast
--   through the lakehouse's own power curve.
--   The low/high range is +/- one standard deviation of the estimate's actual errors at
--   the same days ahead, over the issues from 67 to 8 days before this one (every one of
--   their days has a price by now), so it widens from day 1 to day 7. It held about 72% of
--   outcomes in the backtest. With fewer than 30 such errors (a fresh copy without the
--   forecast archive), it falls back to the model's own fit error, which is too narrow.
--   Gas is held at the latest SAP for the whole week.

WITH params AS (
  SELECT 60 AS fit_days, 0.25 AS last_7_days_weight, 60 AS error_issues
),

latest_issue AS (
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

-- This issue, and the past issues whose errors set the range.
issues AS (
  SELECT DISTINCT o.issue_date, o.issue_date = i.issue_date AS is_latest
  FROM energy.gold.fact_gb_week_ahead_outlook o
  CROSS JOIN latest_issue i CROSS JOIN params p
  WHERE o.issue_date = i.issue_date
     OR (o.issue_date <  date_sub(i.issue_date, 7)
         AND o.issue_date >= date_sub(i.issue_date, 7 + p.error_issues))
),

-- Least squares with two regressors per issue, from covariances (closed form).
moments AS (
  SELECT
    s.issue_date,
    count(*) AS n,
    avg(h.y) AS my, avg(h.x1) AS m1, avg(h.x2) AS m2,
    var_pop(h.x1) AS v1, var_pop(h.x2) AS v2,
    covar_pop(h.x1, h.x2) AS c12, covar_pop(h.x1, h.y) AS c1y, covar_pop(h.x2, h.y) AS c2y
  FROM issues s
  CROSS JOIN params p
  JOIN history h
    ON h.date < s.issue_date AND h.date >= date_sub(s.issue_date, p.fit_days)
  WHERE h.y IS NOT NULL AND h.x1 IS NOT NULL AND h.x2 IS NOT NULL
  GROUP BY s.issue_date
),

model AS (
  SELECT
    issue_date, n, my, m1, m2,
    (c1y * v2 - c2y * c12) / (v1 * v2 - c12 * c12) AS b1,
    (c2y * v1 - c1y * c12) / (v1 * v2 - c12 * c12) AS b2
  FROM moments
),

coefs AS (
  SELECT issue_date, n, b1, b2, my - b1 * m1 - b2 * m2 AS a FROM model
),

recent AS (
  SELECT
    s.issue_date,
    avg(m.gb_market_index_price_gbp_mwh)   AS last_7d_price,
    avg(m.gb_avg_residual_demand_mw)/1000  AS last_7d_residual_gw
  FROM issues s
  JOIN energy.gold.fact_market_daily m
    ON m.date >= date_sub(s.issue_date, 7) AND m.date < s.issue_date
  GROUP BY s.issue_date
),

estimates AS (
  SELECT
    o.*,
    s.is_latest,
    c.a + c.b1 * o.residual_demand_forecast_mw / 1000 + c.b2 * o.latest_sap_gbp_mwh
                                                                            AS model_estimate,
    (1 - p.last_7_days_weight)
      * (c.a + c.b1 * o.residual_demand_forecast_mw / 1000 + c.b2 * o.latest_sap_gbp_mwh)
      + p.last_7_days_weight * r.last_7d_price                              AS estimate
  FROM energy.gold.fact_gb_week_ahead_outlook o
  JOIN issues s ON s.issue_date = o.issue_date
  JOIN coefs c ON c.issue_date = o.issue_date
  JOIN recent r ON r.issue_date = o.issue_date
  CROSS JOIN params p
),

-- How far off the estimate was at each days ahead, over the past issues.
past_error AS (
  SELECT
    e.days_ahead,
    count(*)                                              AS n,
    stddev_pop(e.estimate - a.gb_market_index_price_gbp_mwh) AS sd
  FROM estimates e
  JOIN energy.gold.fact_market_daily a
    ON a.date = e.target_date AND a.is_complete_day
  WHERE NOT e.is_latest AND a.gb_market_index_price_gbp_mwh IS NOT NULL
  GROUP BY e.days_ahead
),

-- Fallback: the model's own fit error on its training days.
fit_error AS (
  SELECT stddev_pop(h.y - (c.a + c.b1 * h.x1 + c.b2 * h.x2)) AS sd
  FROM coefs c
  CROSS JOIN latest_issue i
  CROSS JOIN params p
  JOIN history h
    ON h.date < i.issue_date AND h.date >= date_sub(i.issue_date, p.fit_days)
  WHERE c.issue_date = i.issue_date
    AND h.y IS NOT NULL AND h.x1 IS NOT NULL AND h.x2 IS NOT NULL
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
  round(o.estimate, 0)                                  AS price_estimate_gbp_mwh,
  round(o.estimate - rng.sd, 0)                         AS price_low_gbp_mwh,
  round(o.estimate + rng.sd, 0)                         AS price_high_gbp_mwh,
  round(o.model_estimate, 0)                            AS model_only_estimate_gbp_mwh,
  round(r.last_7d_price, 0)                             AS last_7_days_avg_price_gbp_mwh,
  -- Model context, the same on every row
  round(c.b1, 2)                                        AS gbp_per_gw_residual,
  round(c.b2, 2)                                        AS gbp_per_gbp_gas,
  c.n                                                   AS days_in_fit,
  rng.source                                            AS range_source,
  o.issue_date,
  o.issued_utc                                          AS weather_issued_utc,
  o.demand_forecast_published_utc
FROM estimates o
JOIN coefs c ON c.issue_date = o.issue_date
JOIN recent r ON r.issue_date = o.issue_date
CROSS JOIN fit_error f
LEFT JOIN past_error pe ON pe.days_ahead = o.days_ahead
CROSS JOIN LATERAL (
  SELECT
    CASE WHEN pe.n >= 30 THEN pe.sd ELSE f.sd END AS sd,
    CASE WHEN pe.n >= 30 THEN format_string('past errors (%d)', pe.n)
         ELSE 'model fit error (too few past errors)' END AS source
) rng
WHERE o.is_latest
ORDER BY o.target_date;
