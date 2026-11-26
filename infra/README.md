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

## State

Both stacks keep state in the `tfstate` container of `stenergylakehousetf` (created by
`azure/`), one blob each: `azure.tfstate` and `databricks.tfstate`. Access is with your
Entra login (`use_azuread_auth`); the account has shared keys off and blob versioning on,
so any earlier state version can be recovered.

## Working on a stack

Needs Terraform 1.6 or later, the Azure CLI and, for `databricks/`, a Databricks CLI profile.

```bash
az login --tenant <tenant id>
cd infra/<stack>
cp terraform.tfvars.example terraform.tfvars   # fill in; commands for each id in variables.tf
# backend.hcl (gitignored), the same in both stacks:
#   resource_group_name  = "rg-unitycatalog-uksouth"
#   storage_account_name = "stenergylakehousetf"
#   container_name       = "tfstate"
#   use_azuread_auth     = true
terraform init -backend-config=backend.hcl     # PowerShell: quote it, "-backend-config=backend.hcl"
terraform plan
terraform apply
```

Change these resources here, not in the portal: a manual change shows up as a diff in the
next plan. If a plan wants to change something you didn't touch, fix the config, not the
resource.

## How it was bootstrapped

The state storage can't hold the state of the stack that creates it until it exists, so
`azure/` was first applied with local state (3 imports, 3 new resources), then moved with
`terraform init -migrate-state -backend-config=backend.hcl`. `databricks/` was planned
against the remote backend from the start: 9 imports, 0 to add, 0 to change.

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
