from __future__ import annotations

import builtins
import importlib.util
from pathlib import Path
import sys
import types

import pytest


def test_imports():
    """Test that the xbbg package and its Rust extension can be imported."""
    # Import from installed package (not local source)
    import xbbg

    assert xbbg is not None
    # Access _core through the package to trigger __getattr__ which sets up DLL paths
    assert xbbg._core is not None
    assert hasattr(xbbg._core, "__version__")


def test_native_version_attribute_reports_the_compiled_crate():
    """Release identity must not depend on whether Git metadata was available."""
    from xbbg import _core

    assert _core.__version__ == _core.version()


def test_public_python_surface_remains_importable():
    """Public package and blp facade names stay importable across internal splits."""
    import xbbg
    from xbbg import blp
    from xbbg._exports import (
        BLP_MODULE_EXPORTS,
        PACKAGE_ASYNC_EXPORTS,
        PACKAGE_BLP_SCHEMA_EXPORTS,
        PACKAGE_CONFIG_EXPORTS,
        PACKAGE_LIFECYCLE_EXPORTS,
        PACKAGE_MIDDLEWARE_EXPORTS,
        PACKAGE_STREAMING_EXPORTS,
        PACKAGE_SYNC_EXPORTS,
        PACKAGE_TA_EXPORTS,
    )

    package_groups = (
        PACKAGE_SYNC_EXPORTS,
        PACKAGE_ASYNC_EXPORTS,
        PACKAGE_STREAMING_EXPORTS,
        PACKAGE_TA_EXPORTS,
        PACKAGE_CONFIG_EXPORTS,
        PACKAGE_MIDDLEWARE_EXPORTS,
        PACKAGE_LIFECYCLE_EXPORTS,
        PACKAGE_BLP_SCHEMA_EXPORTS,
    )
    for group in package_groups:
        for name in group:
            assert name in xbbg.__all__
            assert getattr(xbbg, name) is not None

    for name in BLP_MODULE_EXPORTS:
        assert name in blp.__all__
        assert getattr(blp, name) is not None

    assert blp.bfld is blp.bflds
    assert blp.abfld is blp.abflds
    assert callable(blp.fieldInfo)
    assert callable(blp.fieldSearch)
    assert callable(blp.bops)
    assert callable(blp.bschema)
    assert blp.RequestContext is xbbg.RequestContext
    assert blp.RequestEnvironment is xbbg.RequestEnvironment


def test_markets_modules_do_not_require_pandas(monkeypatch: pytest.MonkeyPatch):
    """Loading market metadata helpers is valid without optional pandas installed."""
    spec = importlib.util.find_spec("xbbg")
    assert spec is not None
    assert spec.submodule_search_locations is not None
    markets_dir = Path(next(iter(spec.submodule_search_locations))) / "markets"

    for module_name in list(sys.modules):
        if (
            module_name == "xbbg.markets"
            or module_name.startswith("xbbg.markets.")
            or module_name == "pandas"
            or module_name.startswith("pandas.")
        ):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    markets_pkg = types.ModuleType("xbbg.markets")
    markets_pkg.__path__ = [str(markets_dir)]
    monkeypatch.setitem(sys.modules, "xbbg.markets", markets_pkg)

    real_import = builtins.__import__

    def guarded_import(name: str, *args, **kwargs):
        if name == "pandas" or name.startswith("pandas."):
            raise ImportError(f"blocked optional dataframe backend: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)

    bloomberg_spec = importlib.util.spec_from_file_location("xbbg.markets.bloomberg", markets_dir / "bloomberg.py")
    assert bloomberg_spec is not None
    assert bloomberg_spec.loader is not None
    bloomberg_module = importlib.util.module_from_spec(bloomberg_spec)
    monkeypatch.setitem(sys.modules, "xbbg.markets.bloomberg", bloomberg_module)
    bloomberg_spec.loader.exec_module(bloomberg_module)

    real_import_module = bloomberg_module.importlib.import_module

    def guarded_import_module(name: str, package: str | None = None):
        if name == "pandas" or name.startswith("pandas."):
            raise ImportError(f"blocked optional dataframe backend: {name}")
        return real_import_module(name, package)

    monkeypatch.setattr(bloomberg_module.importlib, "import_module", guarded_import_module)

    info_spec = importlib.util.spec_from_file_location("xbbg.markets.info", markets_dir / "info.py")
    assert info_spec is not None
    assert info_spec.loader is not None
    info_module = importlib.util.module_from_spec(info_spec)
    monkeypatch.setitem(sys.modules, "xbbg.markets.info", info_module)
    info_spec.loader.exec_module(info_module)

    assert bloomberg_module.ExchangeInfo is not None
    with pytest.raises(ImportError, match=r"fetch_exchange_info\(\) requires optional backend 'pandas'"):
        bloomberg_module.fetch_exchange_info("AAPL US Equity")

    with pytest.raises(ImportError, match="exch_info\\(\\) requires optional backend 'pandas'"):
        info_module.exch_info("AAPL US Equity")
