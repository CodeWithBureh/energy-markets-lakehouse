"""Secret lookup: environment variable first, then Databricks secret scope.

In prod the scope is backed by Azure Key Vault, so keys never appear in code or notebooks.
"""

from __future__ import annotations

import os

from energy_lakehouse.config import SECRET_KEYS


def _env_name(source: str) -> str:
    return {"entsoe": "ENTSOE_TOKEN", "fred": "FRED_API_KEY", "cds": "CDS_API_KEY"}[source]


def get_secret(source: str, scope: str | None = None) -> str:
    env_value = os.environ.get(_env_name(source))
    if env_value:
        return env_value

    if scope:
        try:
            from databricks.sdk.runtime import dbutils  # available on Databricks compute

            return dbutils.secrets.get(scope=scope, key=SECRET_KEYS[source])
        except Exception as exc:
            raise RuntimeError(
                f"Could not read secret '{SECRET_KEYS[source]}' from scope '{scope}': {exc}"
            ) from exc

    raise RuntimeError(
        f"No credential for {source}: set ${_env_name(source)} or pass --secret-scope"
    )
