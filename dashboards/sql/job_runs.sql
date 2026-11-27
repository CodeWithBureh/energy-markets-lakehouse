-- Finished runs of the prod jobs over the last 30 days, by day and outcome.
WITH jobs AS (
  SELECT job_id, name
  FROM system.lakeflow.jobs
  QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
)
SELECT
  to_date(r.period_start_time)   AS run_date,
  j.name                         AS job,
  r.result_state,
  count(*)                       AS runs
FROM system.lakeflow.job_run_timeline r
JOIN jobs j ON j.job_id = r.job_id
WHERE j.name IN ('energy-daily', 'energy-checks', 'energy-weekly-weather', 'energy-backfill')
  AND r.result_state IS NOT NULL
  AND r.period_start_time >= date_sub(current_date(), 30)
GROUP BY ALL
ORDER BY run_date, job
