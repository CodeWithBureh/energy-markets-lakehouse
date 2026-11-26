# Everything here was created by hand before Terraform. These blocks adopt it into state
# on the first apply, without changing it. They can stay: once a resource is in state, its
# import block does nothing.

import {
  for_each = local.catalogs
  to       = databricks_catalog.this[each.key]
  id       = each.value
}

import {
  to = databricks_grants.prod_catalog
  id = "catalog/${var.prod_catalog}"
}

import {
  to = databricks_grants.dev_catalog
  id = "catalog/${var.dev_catalog}"
}

import {
  to = databricks_service_principal.prod
  id = var.prod_service_principal_scim_id
}

import {
  to = databricks_service_principal.ci
  id = var.ci_service_principal_scim_id
}

import {
  to = databricks_secret_scope.energy
  id = var.secret_scope
}

import {
  to = databricks_secret_acl.energy_users
  id = "${var.secret_scope}|||users"
}

import {
  to = databricks_sql_endpoint.analytics
  id = var.warehouse_id
}
