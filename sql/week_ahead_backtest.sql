-- Backtest of the week-ahead GB power price report: how far off it was, by days ahead.
--
-- Replays sql/week_ahead_report.sql for every past issue in fact_gb_week_ahead_outlook
-- (rebuilt from Open-Meteo's archive back to Mar 2024, then the live daily fetches): the
-- same model refitted on the 60 complete days before each issue, applied to that issue's
-- forecasts, blended 75/25 with the last 7 days' average price, and compared with the day's
-- market index price once it is known.
-- Replace `energy` with your catalog.
--
-- Columns, per days_ahead (1 = tomorrow):
--   mae_report            the report's estimate (model blended with the last 7 days)
--   mae_model_only        the model alone, without the blend
--   mae_last_7_days       "same as the last 7 days" (the baseline to beat)
--   mae_actual_residual   the model alone fed the residual demand that actually happened:
--                         the gap to mae_model_only is what forecasting demand and wind costs
--   bias_report           mean of estimate minus actual (positive = too high)
--   mae_residual_gw       how far off the residual demand forecast was
--   share_late_hours      share of days using a weather forecast made after the issue time
--                         (archived issues only, late on day 7: the archive keeps 7 days
--                         of lead), so day 7 is slightly flattered

WITH params AS (
  SELECT 0.25 AS last_7_days_weight                  -- as in sql/week_ahead_report.sql
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

issues AS (
  SELECT DISTINCT issue_date FROM energy.gold.fact_gb_week_ahead_outlook
),

-- Two-regressor least squares per issue, from covariances (as in the report).
moments AS (
  SELECT
    i.issue_date,
    count(*) AS n,
    avg(h.y) AS my, avg(h.x1) AS m1, avg(h.x2) AS m2,
    var_pop(h.x1) AS v1, var_pop(h.x2) AS v2,
    covar_pop(h.x1, h.x2) AS c12, covar_pop(h.x1, h.y) AS c1y, covar_pop(h.x2, h.y) AS c2y
  FROM issues i
  JOIN history h
    ON h.date < i.issue_date AND h.date >= date_sub(i.issue_date, 60)
  WHERE h.y IS NOT NULL AND h.x1 IS NOT NULL AND h.x2 IS NOT NULL
  GROUP BY i.issue_date
),

coefs AS (
  SELECT
    issue_date, n, b1, b2, my - b1 * m1 - b2 * m2 AS a
  FROM (
    SELECT
      issue_date, n, my, m1, m2,
      (c1y * v2 - c2y * c12) / (v1 * v2 - c12 * c12) AS b1,
      (c2y * v1 - c1y * c12) / (v1 * v2 - c12 * c12) AS b2
    FROM moments
  )
  WHERE n >= 30                                     -- skip issues with a thin fit window
),

recent AS (
  SELECT i.issue_date, avg(m.gb_market_index_price_gbp_mwh) AS last_7d_price
  FROM issues i
  JOIN energy.gold.fact_market_daily m
    ON m.date >= date_sub(i.issue_date, 7) AND m.date < i.issue_date
  GROUP BY i.issue_date
),

scored AS (
  SELECT
    o.issue_date,
    o.days_ahead,
    o.is_archived_forecast,
    o.forecast_lead_shortfall_hours,
    a.gb_market_index_price_gbp_mwh                                        AS actual,
    c.a + c.b1 * o.residual_demand_forecast_mw / 1000 + c.b2 * o.latest_sap_gbp_mwh
                                                                            AS model_estimate,
    (1 - p.last_7_days_weight)
      * (c.a + c.b1 * o.residual_demand_forecast_mw / 1000 + c.b2 * o.latest_sap_gbp_mwh)
      + p.last_7_days_weight * r.last_7d_price                              AS estimate,
    c.a + c.b1 * a.gb_avg_residual_demand_mw / 1000 + c.b2 * o.latest_sap_gbp_mwh
                                                                            AS estimate_actual_residual,
    r.last_7d_price,
    (o.residual_demand_forecast_mw - a.gb_avg_residual_demand_mw) / 1000   AS residual_error_gw
  FROM energy.gold.fact_gb_week_ahead_outlook o
  JOIN coefs c ON c.issue_date = o.issue_date
  JOIN recent r ON r.issue_date = o.issue_date
  CROSS JOIN params p
  JOIN energy.gold.fact_market_daily a
    ON a.date = o.target_date AND a.is_complete_day
  WHERE a.gb_market_index_price_gbp_mwh IS NOT NULL
    AND o.residual_demand_forecast_mw IS NOT NULL
    AND o.latest_sap_gbp_mwh IS NOT NULL
)

SELECT
  days_ahead,
  count(*)                                                    AS days_scored,
  min(issue_date)                                             AS first_issue,
  max(issue_date)                                             AS last_issue,
  round(avg(abs(estimate - actual)), 1)                       AS mae_report,
  round(avg(abs(model_estimate - actual)), 1)                 AS mae_model_only,
  round(avg(abs(last_7d_price - actual)), 1)                  AS mae_last_7_days,
  round(avg(abs(estimate_actual_residual - actual)), 1)       AS mae_actual_residual,
  round(avg(estimate - actual), 1)                            AS bias_report,
  round(avg(abs(residual_error_gw)), 2)                       AS mae_residual_gw,
  round(avg(CASE WHEN forecast_lead_shortfall_hours > 0 THEN 1 ELSE 0 END), 2)
                                                              AS share_late_hours
FROM scored
GROUP BY days_ahead
ORDER BY days_ahead;
