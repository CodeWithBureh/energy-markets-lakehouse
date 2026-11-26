variable "workspace_host" {
  description = "Workspace URL, e.g. https://adb-1234567890123456.7.azuredatabricks.net"
  type        = string
}

variable "databricks_profile" {
  description = "Databricks CLI profile to authenticate with locally; null in CI"
  type        = string
  default     = null
}

variable "prod_catalog" {
  type    = string
  default = "energy"
}

variable "dev_catalog" {
  type    = string
  default = "energy_dev"
}

variable "readers_group" {
  description = "Account-level group with read access (same as the bundle's readers_group)"
  type        = string
  default     = "energy-readers"
}

variable "prod_service_principal_id" {
  description = "Application ID of energy-prod-sp (also prod_service_principal in databricks.yml)"
  type        = string
}

variable "ci_service_principal_id" {
  description = "Application ID of energy-ci-sp"
  type        = string
}

variable "prod_service_principal_scim_id" {
  description = "Workspace SCIM id of energy-prod-sp, for the import only"
  type        = string
}

variable "ci_service_principal_scim_id" {
  description = "Workspace SCIM id of energy-ci-sp, for the import only"
  type        = string
}

variable "secret_scope" {
  type    = string
  default = "energy"
}

variable "key_vault_id" {
  description = "Resource ID of the Key Vault behind the secret scope (output of infra/azure)"
  type        = string
}

variable "key_vault_uri" {
  description = "Vault URI, e.g. https://kv-energy-lakehouse.vault.azure.net/"
  type        = string
}

variable "warehouse_name" {
  type    = string
  default = "Serverless Starter Warehouse"
}

variable "warehouse_id" {
  description = "Existing SQL warehouse id, for the import only"
  type        = string
}
