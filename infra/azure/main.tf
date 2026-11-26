# Azure resources for this project: the Key Vault behind the `energy` secret scope, its
# access, and a storage account for Terraform state.
#
# The resource group, the Databricks workspace and its access connector are shared with
# other projects, so they are looked up, never managed. The vault and its role assignments
# already existed and are adopted with the import blocks in imports.tf.

terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
  }
  # Bootstrap: the first apply uses local state and creates the state storage below. Then
  # uncomment, `terraform init -migrate-state -backend-config=backend.hcl`, and do the same
  # in infra/databricks with key = "databricks.tfstate".
  # backend "azurerm" { key = "azure.tfstate" }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
  # The subscription is shared and already has KeyVault, Storage and Authorization
  # registered; the default ("core") also registers unrelated services and can hang.
  resource_provider_registrations = "none"
}

data "azurerm_client_config" "current" {}

data "azurerm_resource_group" "shared" {
  name = var.resource_group_name
}

# --- Key Vault ---------------------------------------------------------------------------

# Holds the API keys (entsoe-token, fred-api-key, cds-api-key). Values are set with
# `az keyvault secret set`, never in Terraform, so they never reach state.
resource "azurerm_key_vault" "energy" {
  name                       = var.key_vault_name
  location                   = data.azurerm_resource_group.shared.location
  resource_group_name        = data.azurerm_resource_group.shared.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  purge_protection_enabled   = false
  soft_delete_retention_days = 90

  lifecycle {
    prevent_destroy = true
  }
}

# Databricks reads every secret-scope value through its first-party app, whoever runs the job.
resource "azurerm_role_assignment" "databricks_reads_secrets" {
  scope                = azurerm_key_vault.energy.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = var.azure_databricks_app_object_id
}

# The person who rotates the keys.
resource "azurerm_role_assignment" "owner_manages_secrets" {
  scope                = azurerm_key_vault.energy.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = var.secrets_officer_object_id
}

# --- Terraform state ---------------------------------------------------------------------

resource "azurerm_storage_account" "tfstate" {
  name                            = var.state_storage_account_name
  resource_group_name             = data.azurerm_resource_group.shared.name
  location                        = data.azurerm_resource_group.shared.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = false # Entra ID only: `use_azuread_auth` in the backend

  blob_properties {
    versioning_enabled = true # every state version can be recovered
    delete_retention_policy {
      days = 30
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "azurerm_storage_container" "tfstate" {
  name                  = "tfstate"
  storage_account_id    = azurerm_storage_account.tfstate.id
  container_access_type = "private"
}

# Whoever runs Terraform needs to read and write state blobs.
resource "azurerm_role_assignment" "state_writer" {
  scope                = azurerm_storage_account.tfstate.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}
