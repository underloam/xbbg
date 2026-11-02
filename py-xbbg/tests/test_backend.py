"""Tests for the public Backend enum."""

from __future__ import annotations

import re

import pytest

from xbbg.backend import (
    Backend,
    check_backend,
    effective_backend,
    resolve_backend,
    validate_backend_format,
)
from xbbg.services import Format


class TestBackendEnum:
    """Tests for Backend enum values and behavior."""

    def test_backend_is_string_enum(self):
        assert isinstance(Backend.NATIVE, str)
        assert isinstance(Backend.PANDAS, str)
        assert isinstance(Backend.NARWHALS, str)

    def test_backend_string_comparison(self):
        assert Backend.NATIVE == "native"
        assert Backend.PYARROW == "pyarrow"
        assert Backend.PANDAS == "pandas"
        assert Backend.NARWHALS == "narwhals"
        assert Backend.POLARS == "polars"

    def test_backend_native_value(self):
        assert Backend.NATIVE.value == "native"

    def test_backend_pyarrow_value(self):
        assert Backend.PYARROW.value == "pyarrow"

    def test_backend_narwhals_value(self):
        assert Backend.NARWHALS.value == "narwhals"

    def test_backend_narwhals_lazy_value(self):
        assert Backend.NARWHALS_LAZY.value == "narwhals_lazy"

    def test_backend_pandas_value(self):
        assert Backend.PANDAS.value == "pandas"

    def test_backend_polars_value(self):
        assert Backend.POLARS.value == "polars"

    def test_backend_polars_lazy_value(self):
        assert Backend.POLARS_LAZY.value == "polars_lazy"

    def test_backend_duckdb_value(self):
        assert Backend.DUCKDB.value == "duckdb"

    def test_total_backend_count(self):
        assert len(Backend) == 14

    def test_all_expected_backends_exist(self):
        expected = {
            "NATIVE",
            "PYARROW",
            "NARWHALS",
            "NARWHALS_LAZY",
            "PANDAS",
            "POLARS",
            "POLARS_LAZY",
            "DUCKDB",
            "CUDF",
            "MODIN",
            "DASK",
            "IBIS",
            "PYSPARK",
            "SQLFRAME",
        }
        assert {backend.name for backend in Backend} == expected

    def test_backend_lookup_by_value(self):
        assert Backend("native") == Backend.NATIVE
        assert Backend("pyarrow") == Backend.PYARROW
        assert Backend("pandas") == Backend.PANDAS
        assert Backend("narwhals") == Backend.NARWHALS
        assert Backend("duckdb") == Backend.DUCKDB

    def test_backend_invalid_value_raises(self):
        with pytest.raises(ValueError):
            Backend("arrow")
        with pytest.raises(ValueError):
            Backend("invalid_backend")

    def test_backend_name_attribute(self):
        assert Backend.NATIVE.name == "NATIVE"
        assert Backend.PYARROW.name == "PYARROW"
        assert Backend.PANDAS.name == "PANDAS"
        assert Backend.NARWHALS.name == "NARWHALS"
        assert Backend.DUCKDB.name == "DUCKDB"

    def test_backend_lazy_variants(self):
        lazy_backends = [Backend.NARWHALS_LAZY, Backend.POLARS_LAZY]
        for backend in lazy_backends:
            assert "_lazy" in backend.value

    def test_backend_eager_variants(self):
        eager_backends = [
            Backend.NATIVE,
            Backend.PYARROW,
            Backend.NARWHALS,
            Backend.PANDAS,
            Backend.POLARS,
            Backend.CUDF,
            Backend.MODIN,
        ]
        for backend in eager_backends:
            assert "_lazy" not in backend.value

    def test_default_backend_validation_resolves_narwhals(self):
        backend, fmt = validate_backend_format(None, Format.LONG)
        assert backend == Backend.NARWHALS
        assert fmt == Format.LONG

    def test_backend_resolution_honors_configured_default(self):
        assert resolve_backend(None, Backend.NATIVE) == Backend.NATIVE
        assert resolve_backend("pyarrow", Backend.NATIVE) == Backend.PYARROW
        assert effective_backend(None, Backend.NATIVE) == Backend.NATIVE


# Floors declared by the root pyproject.toml optional dependencies.
@pytest.mark.parametrize(
    ("backend", "module_name", "below", "floor"),
    [
        (Backend.PYARROW, "pyarrow", "21.0.0", "22.0.0"),
        (Backend.PANDAS, "pandas", "2.2.1", "2.2.2"),
        (Backend.POLARS, "polars", "0.20.3", "0.20.4"),
        (Backend.POLARS_LAZY, "polars", "0.20.3", "0.20.4"),
        (Backend.NARWHALS_LAZY, "polars", "0.20.3", "0.20.4"),
        (Backend.DUCKDB, "duckdb", "1.5.0", "1.5.1"),
    ],
)
def test_runtime_version_check_matches_declared_floor(backend, module_name, below, floor, monkeypatch):
    module = pytest.importorskip(module_name)

    monkeypatch.setattr(module, "__version__", below)
    with pytest.raises(ValueError, match=rf"but version {re.escape(below)} is installed"):
        check_backend(backend)
    assert check_backend(backend, raise_on_error=False) is False

    monkeypatch.setattr(module, "__version__", floor)
    assert check_backend(backend) is True
