# Unity Catalog, identities, secrets and the SQL warehouse for this project.
#
# The workspace, metastore and storage are shared with other projects, so they are read,
# never managed, here. Everything below already existed when this was written and was
# brought under Terraform with the import blocks in imports.tf; `terraform plan` should
# show no changes. Jobs, the pipeline, schemas and the Volume stay in the Databricks Asset
# Bundle (databricks.yml): Terraform owns what the bundle runs on.

terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.70"
    }
  }
  # Local state until infra/azure creates the state container; then switch to:
  # backend "azurerm" { key = "databricks.tfstate" } with the rest in backend.hcl.
}

provider "databricks" {
  host    = var.workspace_host
  profile = var.databricks_profile # null in CI, which uses DATABRICKS_* env vars
}

locals {
  catalogs = {
    prod = var.prod_catalog
    dev  = var.dev_catalog
  }
  # What each deploying principal may do in its own catalog, and nothing in the other.
  deploy_privileges = [
    "USE_CATALOG", "USE_SCHEMA", "CREATE_SCHEMA", "CREATE_TABLE", "CREATE_MATERIALIZED_VIEW",
    "SELECT", "MODIFY", "READ_VOLUME", "WRITE_VOLUME",
  ]
}

# --- Shared, read only -------------------------------------------------------------------

data "databricks_group" "readers" {
  display_name = var.readers_group # account-level; schema grants come from the bundle
}

# --- Catalogs ----------------------------------------------------------------------------

resource "databricks_catalog" "this" {
  for_each       = local.catalogs
  name           = each.value
  isolation_mode = "OPEN"
  properties     = { collation = "UTF8_BINARY" }

  lifecycle {
    prevent_destroy = true # holds every table; never let a plan drop it
  }
}

# Authoritative: any grant on these catalogs not listed here is removed on apply.
resource "databricks_grants" "prod_catalog" {
  catalog = databricks_catalog.this["prod"].name
  grant {
    principal  = databricks_service_principal.prod.application_id
    privileges = local.deploy_privileges
  }
  grant {
    principal  = data.databricks_group.readers.display_name
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "dev_catalog" {
  catalog = databricks_catalog.this["dev"].name
  grant {
    principal  = databricks_service_principal.ci.application_id
    privileges = concat(local.deploy_privileges, ["CREATE_VOLUME"]) # ci creates its own Volume
  }
  grant {
    principal  = data.databricks_group.readers.display_name
    privileges = ["USE_CATALOG"]
  }
}

# --- Service principals ------------------------------------------------------------------

# Prod jobs run as this; GitHub deploys prod with its OAuth secret (made by hand, shown once).
resource "databricks_service_principal" "prod" {
  display_name          = "energy-prod-sp"
  application_id        = var.prod_service_principal_id
  workspace_access      = true
  databricks_sql_access = true

  lifecycle {
    prevent_destroy = true
    # It also has the consumer entitlement, which workspace_access covers; the provider
    # can't set both, so leave that one as it is.
    ignore_changes = [workspace_consume]
  }
}

# Deploys and runs the ci copy from the dev branch; it can't touch prod.
resource "databricks_service_principal" "ci" {
  display_name          = "energy-ci-sp"
  application_id        = var.ci_service_principal_id
  workspace_access      = true
  databricks_sql_access = true

  lifecycle {
    prevent_destroy = true
  }
}

# --- Secrets -----------------------------------------------------------------------------

# A pointer to Key Vault (infra/azure). Values are set in the vault, never here.
resource "databricks_secret_scope" "energy" {
  name = var.secret_scope
  keyvault_metadata {
    resource_id = var.key_vault_id
    dns_name    = var.key_vault_uri
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "databricks_secret_acl" "energy_users" {
  scope      = databricks_secret_scope.energy.name
  principal  = "users"
  permission = "MANAGE"
}

# --- SQL warehouse -----------------------------------------------------------------------

# Serverless: starts in seconds and bills only while running. 2X-Small with a 5-minute
# auto-stop; the project tag lets the system.billing queries find its cost.
resource "databricks_sql_endpoint" "analytics" {
  name                      = var.warehouse_name
  cluster_size              = "2X-Small"
  auto_stop_mins            = 5
  min_num_clusters          = 1
  max_num_clusters          = 1
  enable_serverless_compute = true
  warehouse_type            = "PRO"
  enable_photon             = true
  spot_instance_policy      = "COST_OPTIMIZED"

  tags {
    custom_tags {
      key   = "project"
      value = "energy-markets-lakehouse"
    }
  }
}
