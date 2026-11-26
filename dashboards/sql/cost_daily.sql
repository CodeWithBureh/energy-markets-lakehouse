-- Databricks list-price cost of this project per day, product and environment, last 30
-- days. Jobs and the warehouse carry the project tag; the pipeline is matched by name.
-- The environment comes from the job or pipeline name: "[ci] " for the CI copy, "[dev "
-- for a developer's copy, no prefix for prod. The warehouse is shared by all of them.
-- Excludes Azure storage, and predictive optimization, which isn't attributed to a project.
WITH jobs AS (
  SELECT job_id, name
  FROM system.lakeflow.jobs
  QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
),
pipelines AS (
  SELECT pipeline_id, name
  FROM system.lakeflow.pipelines
  WHERE name LIKE '%energy-medallion'
  QUALIFY row_number() OVER (PARTITION BY pipeline_id ORDER BY change_time DESC) = 1
)
SELECT
  u.usage_date,
  CASE u.billing_origin_product
    WHEN 'DLT'  THEN 'Pipeline'
    WHEN 'JOBS' THEN 'Jobs'
    WHEN 'SQL'  THEN 'SQL warehouse'
    ELSE initcap(u.billing_origin_product) END                 AS product,
  CASE
    WHEN u.billing_origin_product = 'SQL'                THEN 'Shared (warehouse)'
    WHEN coalesce(pl.name, j.name) LIKE '[ci] %'         THEN 'CI'
    WHEN coalesce(pl.name, j.name) LIKE '[dev %'         THEN 'Dev'
    WHEN coalesce(pl.name, j.name) IS NULL               THEN 'Unknown'
    ELSE 'Prod' END                                            AS environment,
  round(sum(u.usage_quantity * p.pricing.default), 2)          AS usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
LEFT JOIN pipelines pl ON pl.pipeline_id = u.usage_metadata.dlt_pipeline_id
LEFT JOIN jobs j ON j.job_id = u.usage_metadata.job_id
WHERE u.usage_date >= date_sub(current_date(), 30)
  AND (u.custom_tags['project'] = 'energy-markets-lakehouse' OR pl.pipeline_id IS NOT NULL)
GROUP BY ALL
ORDER BY u.usage_date, product, environment
