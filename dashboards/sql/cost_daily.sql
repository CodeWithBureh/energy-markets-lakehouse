-- Databricks list-price cost of this project per day and product, last 30 days.
-- Jobs and the warehouse carry the project tag; the pipeline is matched by name.
-- Excludes Azure storage, and predictive optimization, which isn't attributed to a project.
WITH pipelines AS (
  SELECT DISTINCT pipeline_id
  FROM system.lakeflow.pipelines
  WHERE name LIKE '%energy-medallion'
)
SELECT
  u.usage_date,
  CASE u.billing_origin_product
    WHEN 'DLT'  THEN 'Pipeline'
    WHEN 'JOBS' THEN 'Jobs'
    WHEN 'SQL'  THEN 'SQL warehouse'
    ELSE initcap(u.billing_origin_product) END             AS product,
  round(sum(u.usage_quantity * p.pricing.default), 2)      AS usd
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_start_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_start_time < p.price_end_time)
LEFT JOIN pipelines pl ON pl.pipeline_id = u.usage_metadata.dlt_pipeline_id
WHERE u.usage_date >= date_sub(current_date(), 30)
  AND (u.custom_tags['project'] = 'energy-markets-lakehouse' OR pl.pipeline_id IS NOT NULL)
GROUP BY ALL
ORDER BY u.usage_date, product
