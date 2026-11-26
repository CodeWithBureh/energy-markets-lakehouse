"""Run the Lakeflow pipeline code (src/pipeline) against a local Spark session.

The pipeline modules expect Databricks' `dlt` module and a global `spark`. Here `dlt` is a
stand-in whose decorators return the function unchanged and record each table's settings,
and `spark` is the real local session, except that reading a table returns a DataFrame the
test registered. So each test calls the real transformation on small inputs and checks the
result. AUTO CDC itself (the SCD type 2 merge) is Databricks' and isn't run; the tests check
each table is wired to it with the right keys.
"""
from __future__ import annotations

import builtins
import importlib
import os
import re
import sys
import types

import pytest

pyspark = pytest.importorskip("pyspark", reason="pip install -e '.[spark]' to run these")
from pyspark.sql import SparkSession

SCHEMAS = {"energy.bronze_schema": "bronze", "energy.silver_schema": "silver",
           "energy.gold_schema": "gold"}


class FakeDlt(types.ModuleType):
    """Decorators pass functions through; streaming tables and CDC flows are recorded."""

    def __init__(self):
        super().__init__("dlt")
        self.tables, self.views, self.expectations = {}, {}, {}
        self.streaming_tables, self.cdc_flows = {}, {}

    def table(self, name=None, **kwargs):
        def wrap(f):
            self.tables[name or f.__name__] = kwargs | {"fn": f}
            return f
        return wrap

    def view(self, name=None, **kwargs):
        def wrap(f):
            self.views[name or f.__name__] = kwargs | {"fn": f}
            return f
        return wrap

    def _expect(self, kind):
        def deco(name, condition):
            def wrap(f):
                self.expectations.setdefault(f.__name__, []).append((kind, name, condition))
                return f
            return wrap
        return deco

    def __getattr__(self, item):
        if item.startswith("expect"):
            return self._expect(item)
        raise AttributeError(item)

    def create_streaming_table(self, name, **kwargs):
        self.streaming_tables[name] = kwargs

    def create_auto_cdc_flow(self, target, **kwargs):
        self.cdc_flows[target] = kwargs


class _Reader:
    def __init__(self, tables):
        self._tables = tables

    def table(self, name):
        if name not in self._tables:
            raise KeyError(f"test didn't register table {name}")
        return self._tables[name]


class FakeSpark:
    """The local session, with table reads served from `tables`."""

    def __init__(self, real):
        self._real = real
        self.tables: dict = {}
        self.read = _Reader(self.tables)
        self.readStream = _Reader(self.tables)

    def __getattr__(self, item):
        return getattr(self._real, item)


@pytest.fixture(scope="session")
def real_spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[1]").appName("pipeline-tests")
         .config("spark.sql.session.timeZone", "UTC")          # as on Databricks
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    for k, v in SCHEMAS.items():
        s.conf.set(k, v)
    yield s
    s.stop()


@pytest.fixture
def pipeline(real_spark, monkeypatch):
    """Load a pipeline module fresh: pipeline("silver") -> (module, fake dlt, fake spark)."""
    def load(name):
        fake_dlt, fake_spark = FakeDlt(), FakeSpark(real_spark)
        monkeypatch.setitem(sys.modules, "dlt", fake_dlt)
        monkeypatch.setattr(builtins, "spark", fake_spark, raising=False)
        sys.modules.pop(f"pipeline.{name}", None)
        return importlib.import_module(f"pipeline.{name}"), fake_dlt, fake_spark
    return load


@pytest.fixture
def df(real_spark):
    """df(rows, "a INT, t TIMESTAMP") -> DataFrame. Python datetimes for TIMESTAMP columns
    are read as UTC (plain createDataFrame would read them as the machine's local time)."""
    def make(rows, schema):
        ntz = re.sub(r"\bTIMESTAMP\b(?!_)", "TIMESTAMP_NTZ", schema)
        frame = real_spark.createDataFrame(rows, ntz)
        return frame.select(*[
            frame[f.name].cast("timestamp").alias(f.name) if f.dataType.typeName() == "timestamp_ntz"
            else frame[f.name] for f in frame.schema.fields])
    return make


def collect_utc(frame):
    """collect() with timestamps as naive UTC datetimes. Plain collect() converts them to
    the machine's local time zone, which differs between a laptop and CI."""
    from pyspark.sql.types import TimestampType
    cols = [frame[f.name].cast("timestamp_ntz").alias(f.name) if isinstance(f.dataType, TimestampType)
            else frame[f.name] for f in frame.schema.fields]
    return frame.select(*cols).collect()
