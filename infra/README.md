# Infrastructure as code

Terraform for what the Databricks Asset Bundle runs on. The bundle (`databricks.yml`) keeps
the jobs, the pipeline, the schemas and the landing Volume; Terraform owns the platform
under them.

| Stack | Manages | Authenticates with |
|---|---|---|
| [`azure/`](azure/) | Key Vault for the API keys and its role assignments; a storage account for Terraform state | `az login` |
| [`databricks/`](databricks/) | Catalogs `energy` and `energy_dev` and their grants; the prod and ci service principals; the Key Vault-backed secret scope and its ACL; the SQL warehouse | a Databricks CLI profile |

**Not managed, because other projects share them:** the resource group, the Databricks
workspace and its access connector, the Unity Catalog metastore and its storage. The stacks
look these up and never change them.

Everything here existed before Terraform and is adopted with `import` blocks, so the first
plan of each stack should only import. Catalogs, the vault, the scope, the principals and
the state storage have `prevent_destroy`, so no plan can delete them.

## First run

Needs Terraform 1.6 or later and the Azure CLI.

1. **Azure stack** (creates the state storage):
   ```bash
   az login
   cd infra/azure
   cp terraform.tfvars.example terraform.tfvars   # fill in; commands for each id in variables.tf
   terraform init
   terraform plan    # expect: 3 to import (vault, 2 role assignments), 3 to add (state storage)
   terraform apply
   ```
   If the plan wants to *change* the vault, the config doesn't match what's there yet: fix
   the config, not the vault.
2. **Move its state into the new storage:** write `backend.hcl` from
   `terraform output -raw backend_config`, uncomment the `backend "azurerm"` block in
   `main.tf`, then `terraform init -migrate-state -backend-config=backend.hcl`.
3. **Databricks stack:**
   ```bash
   cd ../databricks
   cp terraform.tfvars.example terraform.tfvars   # key_vault_* from the azure outputs
   # uncomment its backend block (key = "databricks.tfstate"), copy backend.hcl
   terraform init -backend-config=backend.hcl
   terraform plan    # expect: 9 to import, 0 to add, 0 to change, 0 to destroy
   terraform apply
   ```

After that, change these resources here, not in the portal: a manual change shows up as a
diff in the next plan.

## Notes

- `databricks_grants` is authoritative: a catalog grant made by hand is removed on the next
  apply unless it's added to `main.tf`. Schema-level grants for the readers group come from
  the bundle (`resources/storage.yml`) and aren't touched.
- Secret values never go through Terraform, so they never reach state. Set them with
  `az keyvault secret set`.
- The prod principal also has the consumer entitlement, which `workspace_access` covers;
  the provider can't set both, so Terraform ignores it.
- CI runs `terraform fmt -check` and `terraform validate` on both stacks. Planning in CI
  needs an Entra app with a federated credential for GitHub (OIDC) and a Databricks
  principal allowed to read grants and principals; that's the next step.
