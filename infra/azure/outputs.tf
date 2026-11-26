output "key_vault_id" {
  description = "key_vault_id for infra/databricks"
  value       = azurerm_key_vault.energy.id
}

output "key_vault_uri" {
  description = "key_vault_uri for infra/databricks"
  value       = azurerm_key_vault.energy.vault_uri
}

output "backend_config" {
  description = "Contents for backend.hcl in both stacks"
  value       = <<-EOT
    resource_group_name  = "${data.azurerm_resource_group.shared.name}"
    storage_account_name = "${azurerm_storage_account.tfstate.name}"
    container_name       = "${azurerm_storage_container.tfstate.name}"
    use_azuread_auth     = true
  EOT
}
