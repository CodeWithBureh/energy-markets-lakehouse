# Setup and operations

How to build this lakehouse in your own Azure Databricks workspace, deploy it, backfill it and run it day to day. The [README](../README.md) has the overview.

## Getting started

### 1. Accounts and keys (all free)
| Source | What to do | Secret key name |
|---|---|---|
| Elexon | Nothing. The Insights API needs no key | – |
| ENTSO-E | Register at transparency.entsoe.eu, then email transparency@entsoe.eu for "Restful API access" | `entsoe-token` |
| FRED | Request a key at fred.stlouisfed.org | `fred-api-key` |
| Copernicus CDS | Create an account, copy the API key, **accept the ERA5 licence** on the dataset page | `cds-api-key` |
| yfinance | Nothing (unofficial; swap for Alpaca/Twelve Data if it breaks). Also supplies TTF gas (`TTF=F`) | – |
| National Gas data portal | Nothing. SAP is data item `PUBOB603` | – |
| Open-Meteo | Nothing (free for non-commercial use, CC BY 4.0) | – |

### 2. Local development
```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,spark]"   # spark: local PySpark for tests/pipeline (needs Java 17)
pytest -q
export ENTSOE_TOKEN=... FRED_API_KEY=... CDS_API_KEY=...
ingest --source elexon --landing-root ./landing --start 2026-10-24 --end 2026-10-26
```

### 3. Azure workspace (shared by dev and prod)
One workspace hosts both targets. They're kept apart by catalog: dev writes to
`energy_dev` (with schemas prefixed `dev_<you>_`), prod writes to `energy`.

1. **Workspace**: Premium tier, UK South, serverless enabled. Check the managed resource
   group for a **NAT gateway** (~£25–30/month idle). This project only uses serverless, so
   deploy without secure cluster connectivity, or budget for it.
2. **Catalogs**: create `energy_dev` and `energy` (managed storage on ADLS Gen2 via the
   Access Connector).
3. **Secrets**: a Key Vault-backed secret scope named `energy`, read by both targets
   (see [3a](#3a-key-vault-backed-secret-scope)).
4. **Service principal** for prod (see [3b](#3b-prod-service-principal)).
5. **Budgets**: in Azure Cost Management, set a budget on the subscription/resource group
   with alerts at £40 and £65.
6. **Readers group**: Settings → Identity and access → Groups → Add new, named
   `energy-readers` (an account-level group; Unity Catalog can't grant to workspace-local
   groups). Add yourself, then run `GRANT USE CATALOG ON CATALOG energy TO \`energy-readers\``.
   The bundle grants the group `USE SCHEMA` + `SELECT` on bronze/silver/gold and view
   access to the prod jobs and pipeline.
7. Set `workspace.host`, `alert_email` and `prod_service_principal` in `databricks.yml`.

#### 3a. Key Vault-backed secret scope

The API keys live in Azure Key Vault; the Databricks scope is only a pointer to it. Secret
names must match `SECRET_KEYS` in `config.py`.

1. **Create the vault** (Azure portal → Key vaults → Create): same resource group as the
   workspace, a globally unique name (e.g. `kv-energy-lakehouse`), UK South, Standard tier,
   **Azure role-based access control**, public network access on. If you restrict networking
   later, tick "Allow trusted Microsoft services".
2. **Let yourself write secrets**: vault → Access control (IAM) → Add role assignment →
   **Key Vault Secrets Officer** → your user.
3. **Let Databricks read them**: add **Key Vault Secrets User** for the **AzureDatabricks**
   application (`2ff814a6-3304-4ab8-85cb-cd0e6f879c1d`). Every read from the scope goes
   through this app, whoever runs the job.
4. **Add the secrets** (vault → Secrets → Generate/Import, or the CLI):
   ```bash
   az keyvault secret set --vault-name kv-energy-lakehouse --name entsoe-token --value "<token>"
   az keyvault secret set --vault-name kv-energy-lakehouse --name fred-api-key --value "<key>"
   az keyvault secret set --vault-name kv-energy-lakehouse --name cds-api-key  --value "<key>"
   ```
5. From the vault's **Properties** page, copy the **Vault URI** and the **Resource ID**.
6. **Create the scope** at `https://<workspace-url>#secrets/createScope` (case-sensitive;
   it isn't in the menus): scope name `energy`, manage principal **All workspace users**,
   DNS name = Vault URI, Resource ID = Resource ID.
7. **Check it**:
   ```bash
   databricks secrets list-scopes          # energy, backend AZURE_KEYVAULT
   databricks secrets list-secrets energy  # the three names
   ```
   The scope is read-only from Databricks (`put-secret` fails). Change keys in Key Vault;
   Databricks sees new values immediately.

#### 3b. Prod service principal

1. **Create it**: workspace → your name → Settings → Identity and access → Service principals
   → Add new. Name `energy-prod-sp`, **Databricks managed**, keep **Workspace access** (no
   cluster creation needed; everything is serverless).
2. Copy its **Application ID** into `prod_service_principal` in `databricks.yml`.
3. **OAuth secret for CI**: service principal → Secrets → Generate secret, scope
   **all-apis** (bundle deploys touch files, jobs, pipelines, Unity Catalog and secrets),
   lifetime 730 days. It's shown once; set a reminder to rotate it.
4. **Grant it the prod catalog only** (SQL editor):
   ```sql
   GRANT USE CATALOG, USE SCHEMA, CREATE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW,
         SELECT, MODIFY, READ VOLUME, WRITE VOLUME
   ON CATALOG energy TO `<application-id>`;
   ```
   Nothing on `energy_dev`, which keeps prod out of dev.
5. **Secret scope**: nothing to do. "All workspace users" already covers it, and it needs no
   Key Vault role because the AzureDatabricks app does the reading.
6. **Let yourself deploy as it**: service principal → Permissions → Grant access → your user
   → **Service principal: User** (workspace admins may already have this).
7. **GitHub**: repo → Settings → Environments → New environment `prod` (optionally limit it
   to the `main` branch and add yourself as a required reviewer). Add environment secrets
   `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID` (the Application ID) and
   `DATABRICKS_CLIENT_SECRET`. The deploy job in `ci.yml` uses `environment: prod`, so PR
   runs can't read them.

#### 3c. CI service principal (dev branch)

The `dev` branch deploys a shared test copy, the `ci` target: catalog `energy_dev`, schemas
`ci_raw`/`ci_bronze`/`ci_silver`/`ci_gold`, jobs named `[ci] …`, schedules paused. It runs as
its own service principal so it can't touch prod, and prod's can't touch it.

1. **Create it** as in 3b.1, named `energy-ci-sp`, and generate an OAuth secret (3b.3).
2. **Grant it the dev catalog only**:
   ```sql
   GRANT USE CATALOG, USE SCHEMA, CREATE SCHEMA, CREATE TABLE, CREATE MATERIALIZED VIEW,
         CREATE VOLUME, SELECT, MODIFY, READ VOLUME, WRITE VOLUME
   ON CATALOG energy_dev TO `<ci-application-id>`;
   ```
3. **GitHub**: add repository secrets `DEV_DATABRICKS_CLIENT_ID` and
   `DEV_DATABRICKS_CLIENT_SECRET`. Pull-request validation also reads them, so they're
   repository secrets rather than environment secrets.
4. **Seed it once**: Actions → ci → Run workflow on `dev`, with `seed_start_month` about
   three months back (e.g. `2026-08`), so monthly FRED series have rows. Later runs only need
   the daily job's rolling windows.

Only CI should deploy `-t ci`: its deployment state lives in the CI principal's home folder,
so a deploy from your laptop would create a second copy of every job.

### CI/CD flow

```
feature branch ──PR──▶ dev ──auto PR──▶ main
   tests,              deploy -t ci,        deploy -t prod,
   validate ci+prod    run it, check        validate-only, check
```

Promotion is automatic: when a push to `dev` passes in the ci copy, the `promote` job opens a
`dev` → `main` pull request (or reuses the open one) and turns on auto-merge. GitHub merges
it once `main`'s required checks pass, and that merge deploys prod. A failure anywhere on
the way stops the promotion; fix it on `dev` and the next push tries again. To hold a
release back, turn auto-merge off on the open promotion pull request.

Setup (once):
1. Create a fine-grained personal access token (GitHub → Settings → Developer settings):
   this repository only, permissions **Contents: read and write** and **Pull requests: read
   and write**. Save it as the repo secret `PROMOTE_TOKEN`. `GITHUB_TOKEN` can't be used:
   pull requests and merges made with it don't trigger workflows, so `main`'s checks and the
   prod deploy would never run.
2. Repo settings: allow auto-merge, and leave "Automatically delete head branches" off (it
   would delete `dev` after each promotion). `main`'s protection doesn't require branches to
   be up to date, because each promotion's merge commit is on `main` only.

| Event | What runs |
| --- | --- |
| Pull request into `dev` or `main` | ruff, pytest with coverage, build the wheel and start each entry point, `bundle validate` for `ci` and `prod` |
| Pull request into `main` | also: the source branch must be `dev` (or `hotfix/*`) |
| Push to `dev` | `bundle deploy -t ci`, pipeline validate-only update, `energy_forecast_archive` (a warning if it fails), the full `energy_daily` job (fails on `SUCCESS_WITH_FAILURES` too), then `energy_checks`; if all pass, `promote` opens the `dev` → `main` pull request with auto-merge |
| Push to `main` | `bundle deploy -t prod`, pipeline validate-only update, `energy_checks` against prod data (read-only; the schedule owns the runs) |

`energy_checks` (`src/energy_lakehouse/checks.py`) fails the run when a required check
fails: every silver and gold table has rows, current silver rows are unique on their keys,
GB and EU prices reach within `max_age_days` (default 3) of today, no incomplete settlement
days in the last 30, and GB prices are present and within range. ERA5 checks only warn,
because only the weekly job loads weather. Run it any time:
```bash
databricks bundle run -t prod energy_checks
databricks bundle run -t dev energy_checks --params max_age_days=7   # dev is rarely run
```

Recommended repo settings: protect `dev` and `main` (pull requests only, require the `test`
and `validate` checks, plus `promotion-guard` on `main`), and add a required reviewer to the
`prod` environment. Dependabot opens weekly update PRs against `dev`.

### 4. Dev
```bash
databricks auth login --host https://adb-<id>.<n>.azuredatabricks.net
databricks bundle deploy -t dev
databricks bundle run -t dev energy_daily    # ingest -> pipeline, rolling windows
# Backfill a specific window (max 31 days per run, the --max-days guard):
databricks bundle run -t dev energy_daily --params start=2026-10-01,end=2026-10-31
```
Schedules stay paused in dev, so it only costs money when you run something.

**Backfilling many months** uses the on-demand `energy_backfill` job. Each source loops over
the months inside Databricks (one month per iteration) and the pipeline refreshes once at the
end. Set the target and months in `backfill.toml`, then:
```bash
python scripts/backfill.py --dry-run   # show the plan
python scripts/backfill.py             # start the job and wait for it
```
or call the job directly:
```bash
databricks bundle run -t prod energy_backfill --params start_month=2024-01,end_month=current
databricks bundle run -t prod energy_backfill --params "months=2024-03 2024-10,include_weather=true"
databricks bundle run -t prod energy_backfill --params start_month=2024-01,end_month=current,weather_only=true
```
`weather_only=true` backfills ERA5 alone and skips the market sources (Elexon, ENTSO-E, FRED,
stocks, gas, the forecast archive). Expect it to be slow: the Copernicus queue can take up to an hour per month.
If some months fail, open the run and click **Repair run**: only the failed months rerun.

**Reruns only fetch what's missing or can still change.** Before each request the ingest looks
in the landing zone. A partition landed after its source stopped revising it is skipped
without downloading. A download byte-identical to the last landed file is not written. So a
backfill rerun fills gaps, and the daily job's overlapping windows don't land duplicates.
Add `refetch=true` to force a full re-download (e.g. after fixing a parser).

| Data | Treated as final after |
|---|---|
| Elexon forecasts (dated by publish day) | the publish day has ended |
| Elexon demand outturn, market index | 2 days |
| Elexon generation by fuel | 3 days |
| Elexon system prices | 30 days (settlement runs) |
| ENTSO-E day-ahead | 2 days |
| Gas (SAP, TTF) | 3 days |
| Stocks | 7 days |
| FRED | 45 days (monthly EU gas lands late) |
| ERA5 | 120 days from the month's first day (ERA5T is replaced by final ERA5) |
| Weather forecast archive | as soon as it lands (only issues at least 2 days old) |

The settle periods live in `SETTLE_DAYS` in `src/energy_lakehouse/landing.py`. The weather
forecast is new every run, so it is never skipped.

**The weather forecast archive** (`weather_forecast_archive`, its own `energy_forecast_archive` job at 06:40
and part of every market backfill) rebuilds past outlook issues from Open-Meteo's
[Previous Runs API](https://open-meteo.com/en/docs/previous-runs-api): for each day since
14 Mar 2024, the forecast for D+1 to D+7 as it stood at 07:40 UK time, using for each hour the
newest forecast made by then. The archive keeps forecasts up to 7 days before valid time, so
late on D+7 it uses one made up to 16 hours after the issue (`lead_shortfall_hours`). An issue
is rebuilt 2 days on, once every forecast it uses has been made (the API also returns values
for runs not made yet, so a younger issue isn't trusted); on days with a live fetch, the
outlook uses that instead. The whole history is about 11,000 calls by
Open-Meteo's count against a free limit of 10,000 a day, so the first full backfill can stop
partway with HTTP 429; rerun it the next day and the months already landed are skipped.
After that, `energy_forecast_archive` adds each issue 2 days on, refetching the issues from 14 to 2 days
ago so a missed run catches up.

### 5. Prod
Pushes to `main` run the tests, deploy prod as the service principal (step 3b.7), then run a
validate-only pipeline update and `energy_checks` (see CI/CD flow).

> **Tip:** let CI run the first prod deploy rather than running `bundle deploy -t prod` from
> your laptop. CI signs in as the service principal, so the prod schemas and Volume end up
> owned by it rather than by you.

Once deployed, run or backfill it:
```bash
databricks bundle run -t prod energy_daily
databricks bundle run -t prod energy_daily --params start=2026-09-01,end=2026-09-30
```


## Troubleshooting

* **Wheel not found on serverless**: some CLI versions don't resolve `../dist/*.whl` in
  `environments`. Replace it with the absolute path
  `/Workspace${workspace.root_path}/artifacts/.internal/<wheel-name>.whl`.
* **ENTSO-E returns no rows**: tomorrow's prices are published around 13:00 CET. Before
  that you get an Acknowledgement document, which is logged, not treated as an error.
* **ERA5 request refused**: accept the dataset licence on the CDS website (one-off).

Data: Contains BMRS data © Elexon Limited copyright and database right. ENTSO-E
Transparency Platform. FRED®, Federal Reserve Bank of St. Louis. Gas SAP: National Gas Transmission. Weather forecasts: Open-Meteo.com (CC BY 4.0). Copernicus Climate Change
Service (ERA5).
