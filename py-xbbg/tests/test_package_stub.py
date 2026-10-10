"""The generated package stub must describe the complete lazy public API."""

from __future__ import annotations

import ast
import importlib.abc
from pathlib import Path
import runpy
import sys

import pytest


@pytest.fixture(autouse=True)
def _no_real_engine(monkeypatch):
    """Replace the parent native-engine fixture with a native import prohibition."""

    class NoNativeExtension(importlib.abc.MetaPathFinder):
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "xbbg._core":
                pytest.fail("package stub generation must not import the native extension")

    monkeypatch.delitem(sys.modules, "xbbg._core", raising=False)
    package = sys.modules.get("xbbg")
    if package is not None:
        monkeypatch.delitem(package.__dict__, "_core", raising=False)
        monkeypatch.setitem(package.__dict__, "_core_module", None)
    monkeypatch.setattr(sys, "meta_path", [NoNativeExtension(), *sys.meta_path])


def _exports(stub: str) -> list[str]:
    module = ast.parse(stub)
    assignment = next(
        node
        for node in module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(name, ast.Name) and name.id == "__all__" for name in node.targets)
    )
    return ast.literal_eval(assignment.value)


def test_package_stub_matches_runtime_registry():
    import xbbg
    from xbbg._exports import PACKAGE_EXPORTS

    stub = Path(xbbg.__file__).with_name("__init__.pyi").read_text(encoding="utf-8")

    assert _exports(stub) == list(PACKAGE_EXPORTS) == xbbg.__all__
    assert "xbbg._core" not in sys.modules
    assert "_core" not in vars(xbbg)
    assert xbbg._core_module is None


def test_public_sync_wrappers_have_matching_static_signatures():
    import xbbg

    package = Path(xbbg.__file__).parent
    registry = runpy.run_path(str(package / "_exports.py"))
    trees = {
        name: ast.parse((package / f"{name}.py").read_text(encoding="utf-8"))
        for name in ("blp", "_streaming", "_technical")
    }
    type_checking = next(
        node
        for node in trees["blp"].body
        if isinstance(node, ast.If) and isinstance(node.test, ast.Name) and node.test.id == "TYPE_CHECKING"
    )
    declarations = {node.name: node for node in type_checking.body if isinstance(node, ast.FunctionDef)}
    async_functions = {
        node.name: node for tree in trees.values() for node in tree.body if isinstance(node, ast.AsyncFunctionDef)
    }
    installer = next(
        node
        for node in trees["blp"].body
        if isinstance(node, ast.FunctionDef) and node.name == "_install_manual_sync_wrappers"
    )
    registrations = next(node.iter for node in installer.body if isinstance(node, ast.For))
    for registration in registrations.elts:
        sync_name, async_name = registration.elts
        if sync_name.value not in registry["PACKAGE_EXPORTS"]:
            continue
        assert sync_name.value in declarations, f"{sync_name.value} has no static signature"
        declaration = declarations[sync_name.value]
        original = async_functions[async_name.id]
        assert ast.dump(declaration.args) == ast.dump(original.args), sync_name.value
        assert declaration.returns is not None, sync_name.value
        if original.returns is not None:
            assert ast.dump(declaration.returns) == ast.dump(original.returns), sync_name.value
