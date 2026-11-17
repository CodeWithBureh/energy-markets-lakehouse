#!/usr/bin/env bash
# Free Edition can't call most external APIs (outbound internet is allow-listed), so in
# dev we ingest on your laptop and copy the files into the Free Edition Volume.
#
#   export ENTSOE_TOKEN=... FRED_API_KEY=... CDS_API_KEY=...
#   ./scripts/dev_ingest_and_upload.sh 2026-10-01 2026-10-07
#
# Find your dev Volume path with:  databricks bundle summary -t dev
# (development mode prefixes schemas, e.g. /Volumes/workspace/dev_yourname_raw/landing)
set -euo pipefail

START=${1:?start date YYYY-MM-DD}
END=${2:?end date YYYY-MM-DD}
VOLUME=${DEV_VOLUME:?set DEV_VOLUME, e.g. /Volumes/workspace/dev_yourname_raw/landing}
LOCAL=./landing

for src in elexon entsoe fred stocks; do
  ingest --source "$src" --start "$START" --end "$END" --landing-root "$LOCAL" --allow-partial
done
# ERA5 is slow (CDS queue); uncomment when you need it
# ingest --source era5 --start "$START" --end "$END" --landing-root "$LOCAL" --allow-partial

# Uses your default CLI profile; add --profile <name> if your Free Edition profile differs.
databricks fs cp -r "$LOCAL" "dbfs:${VOLUME}" --overwrite
echo "Uploaded to ${VOLUME}. Now: databricks bundle run -t dev energy_pipeline"
