variable "subscription_id" {
  type = string
}

variable "resource_group_name" {
  description = "Existing, shared resource group holding the workspace and the vault"
  type        = string
}

variable "key_vault_name" {
  type    = string
  default = "kv-energy-lakehouse"
}

variable "azure_databricks_app_object_id" {
  description = <<-EOT
    Object id, in your tenant, of the AzureDatabricks first-party app (application id
    2ff814a6-3304-4ab8-85cb-cd0e6f879c1d):
    az ad sp show --id 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d --query id -o tsv
  EOT
  type        = string
}

variable "secrets_officer_object_id" {
  description = "Object id of the user who manages the secrets: az ad signed-in-user show --query id -o tsv"
  type        = string
}

variable "state_storage_account_name" {
  description = "Globally unique, 3-24 lowercase letters and digits"
  type        = string
}

# Role assignment ids, for the import only:
#   az role assignment list --scope <vault id> --query "[].{role:roleDefinitionName, id:id}" -o table
variable "databricks_reads_secrets_assignment_id" {
  type = string
}

variable "owner_manages_secrets_assignment_id" {
  type = string
}
