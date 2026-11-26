# The vault and its two role assignments were created by hand (docs/setup.md, 3a). These
# adopt them into state on the first apply without changing them. The state storage is new.

import {
  to = azurerm_key_vault.energy
  id = "/subscriptions/${var.subscription_id}/resourceGroups/${var.resource_group_name}/providers/Microsoft.KeyVault/vaults/${var.key_vault_name}"
}

import {
  to = azurerm_role_assignment.databricks_reads_secrets
  id = var.databricks_reads_secrets_assignment_id
}

import {
  to = azurerm_role_assignment.owner_manages_secrets
  id = var.owner_manages_secrets_assignment_id
}
