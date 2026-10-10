"""Package-stub generation runs in the checkout-only release tooling CI job."""

from __future__ import annotations

import ast
from pathlib import Path
import runpy
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = ROOT / "py-xbbg" / "src" / "xbbg"


@pytest.fixture(autouse=True)
def _no_package_import(monkeypatch):
    """Require generation to work even when xbbg and its native module cannot load."""
    for name in list(sys.modules):
        if name.startswith("xbbg."):
            monkeypatch.setitem(sys.modules, name, None)
    monkeypatch.setitem(sys.modules, "xbbg", None)


@pytest.fixture
def generated_stub():
    generator = runpy.run_path(str(ROOT / "scripts" / "generate_package_stub.py"))
    return generator["generate_stub"](PACKAGE)


def test_package_stub_is_current(generated_stub):
    assert generated_stub == (PACKAGE / "__init__.pyi").read_text(encoding="utf-8")
    assert sys.modules["xbbg"] is None


def test_package_stub_reexports_real_types_and_modules(generated_stub):
    module = ast.parse(generated_stub)
    imports = {
        alias.asname: (node.module, alias.name)
        for node in module.body
        if isinstance(node, ast.ImportFrom) and node.level == 1
        for alias in node.names
    }

    assert imports["EngineConfig"] == ("_core", "PyEngineConfig")
    assert imports["Engine"] == ("_engine", "Engine")
    assert imports["configure"] == ("_engine", "configure")
    assert imports["Backend"] == ("backend", "Backend")
    assert imports["set_backend"] == ("backend", "set_backend")
    assert imports["RequestParams"] == ("services", "RequestParams")
    assert imports["Service"] == ("_services_gen", "Service")
    assert imports["add_middleware"] == ("request_middleware", "add_middleware")
    assert imports["ServiceSchema"] == ("schema", "ServiceSchema")
    assert imports["get_sdk_info"] == ("_sdk", "get_sdk_info")
    assert imports["BlpRequestError"] == ("exceptions", "BlpRequestError")
    for name in ("_core", "ext", "markets", "testing"):
        assert imports[name] == (None, name)
    registry = runpy.run_path(str(PACKAGE / "_exports.py"))
    assert set(imports) | {"__version__"} == set(registry["PACKAGE_EXPORTS"])
