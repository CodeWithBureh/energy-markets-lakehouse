"""Static configuration: which zones, series, tickers and weather variables we collect."""

from __future__ import annotations

# --- ENTSO-E bidding zones -------------------------------------------------------------
# GB left ENTSO-E's transparency platform after Brexit, so we collect GB's
# interconnected neighbours instead and compare them with Elexon's GB prices.
ENTSOE_ZONES: dict[str, str] = {
    "FR": "10YFR-RTE------C",       # IFA / IFA2 / ElecLink
    "NL": "10YNL----------L",       # BritNed
    "BE": "10YBE----------2",       # Nemo Link
    "NO2": "10YNO-2--------T",      # North Sea Link
    "DK1": "10YDK-1--------W",      # Viking Link
    "IE_SEM": "10Y1001A1001A59C",   # EWIC / Moyle
}

# --- FRED series -----------------------------------------------------------------------
FRED_SERIES: dict[str, str] = {
    "DHHNGSP": "Henry Hub natural gas spot price, USD/MMBtu, daily",
    "PNGASEUUSDM": "Global price of natural gas, EU, USD/MMBtu, monthly",
    "DEXUSUK": "USD per 1 GBP, daily",
    "DEXUSEU": "USD per 1 EUR, daily",
}

# --- Equities --------------------------------------------------------------------------
# London listings are quoted in pence (GBp) on Yahoo; silver converts to GBP.
STOCKS: dict[str, dict[str, str]] = {
    "SSE.L": {"name": "SSE", "segment": "Generation & networks", "currency": "GBp"},
    "CNA.L": {"name": "Centrica", "segment": "Supply & trading", "currency": "GBp"},
    "NG.L": {"name": "National Grid", "segment": "Networks", "currency": "GBp"},
    "DRX.L": {"name": "Drax", "segment": "Generation (biomass)", "currency": "GBp"},
    "BP.L": {"name": "BP", "segment": "Oil & gas", "currency": "GBp"},
    "SHEL.L": {"name": "Shell", "segment": "Oil & gas", "currency": "GBp"},
}

# --- ERA5 weather ----------------------------------------------------------------------
ERA5_DATASET = "reanalysis-era5-single-levels"
ERA5_VARIABLES: list[str] = [
    "2m_temperature",                       # t2m  -> demand driver
    "10m_u_component_of_wind",              # u10
    "10m_v_component_of_wind",              # v10
    "100m_u_component_of_wind",             # u100 -> wind-turbine hub height
    "100m_v_component_of_wind",             # v100
    "surface_solar_radiation_downwards",    # ssrd -> solar output
]
# Bounding box around Great Britain, CDS order: [North, West, South, East]
ERA5_GB_AREA: list[float] = [61.0, -8.0, 49.5, 2.0]
# ERA5T (preliminary) data lands roughly 5 days behind real time.
ERA5_LATENCY_DAYS = 6

# --- Secrets ---------------------------------------------------------------------------
# Key names in the Databricks secret scope (backed by Azure Key Vault in prod).
# Locally, set the matching environment variables instead.
SECRET_KEYS: dict[str, str] = {
    "entsoe": "entsoe-token",      # env: ENTSOE_TOKEN
    "fred": "fred-api-key",        # env: FRED_API_KEY
    "cds": "cds-api-key",          # env: CDS_API_KEY
}

# Cost guard: refuse to pull more than this many days in one run unless overridden.
DEFAULT_MAX_DAYS_PER_RUN = 31
