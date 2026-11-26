output "warehouse_id" {
  value = databricks_sql_endpoint.analytics.id
}

output "catalogs" {
  value = { for k, c in databricks_catalog.this : k => c.name }
}
