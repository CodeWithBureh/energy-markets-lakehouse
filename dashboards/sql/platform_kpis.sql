-- One row of headline numbers for the counters.
WITH jobs AS (
  SELECT job_id, name
  FROM system.lakeflow.jobs
  QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
),
daily AS (
  SELECT r.result_state
  FROM system.lakeflow.job_run_timeline r
  JOIN jobs j ON j.job_id = r.job_id
  WHERE j.name = 'energy-daily'
    AND r.result_state IS NOT NULL
    AND r.period_start_time >= date_sub(current_date(), 30)
)
SELECT
  (SELECT count(*) FROM daily WHERE result_state = 'SUCCEEDED')   AS daily_runs_ok,
  (SELECT count(*) FROM daily)                                    AS daily_runs,
  (SELECT count(*) FROM dq_settlement_period_completeness
    WHERE date >= date_sub(current_date(), 30) AND date < current_date())
                                                                  AS incomplete_days_30d
