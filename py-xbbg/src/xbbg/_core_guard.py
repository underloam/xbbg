"""Refuse a native ``xbbg._core`` that was built for a different xbbg.

pip and conda install the Python files and the extension together, so an installed
extension whose stamped ``__version__`` differs from the installed distribution came
from another build: an interrupted upgrade, pip and conda installing over each other,
or a binary left behind by an earlier install. An editable install keeps whichever
extension was last compiled into the source tree while ``git pull`` updates the
Python sources, and its metadata is as old as that extension, so in a source checkout
the extension's build commit is compared with the checkout's ``HEAD`` instead.
"""

from __future__ import annotations

from importlib.abc import MetaPathFinder
from importlib.machinery import ExtensionFileLoader, PathFinder
from pathlib import Path
import sys
from typing import TYPE_CHECKING
import warnings

if TYPE_CHECKING:
    from collections.abc import Sequence
    from importlib.machinery import ModuleSpec
    from types import ModuleType

CORE_MODULE = "xbbg._core"
_PACKAGE_DIR = Path(__file__).resolve().parent


def install(distribution_version: str | None) -> None:
    """Verify ``xbbg._core`` on every import path, including ``from xbbg._core import ...``.

    Args:
        distribution_version: Installed xbbg version, or ``None`` without package metadata.
    """
    sys.meta_path[:] = [finder for finder in sys.meta_path if not getattr(finder, "xbbg_core_guard", False)]
    sys.meta_path.insert(0, _CoreFinder(distribution_version))


def verify_core(core: ModuleType, *, distribution_version: str | None, package_dir: Path = _PACKAGE_DIR) -> None:
    """Check that a freshly loaded ``xbbg._core`` belongs to the xbbg in ``package_dir``.

    Raises:
        ImportError: An installed extension was built for another xbbg version.
    """
    checkout = _source_checkout(package_dir)
    if checkout is not None:
        _warn_if_built_from_other_commit(core, checkout)
        return
    built_for = getattr(core, "__version__", None)
    if distribution_version is None or built_for == distribution_version:
        return
    raise ImportError(
        f"xbbg._core ({getattr(core, '__file__', None) or 'native extension'}) was built for xbbg {built_for}, "
        f"but the installed xbbg is {distribution_version}: the native extension and the Python package come "
        f"from different builds. Reinstall xbbg {distribution_version} with the tool that installed it, e.g. "
        f'`pip install --force-reinstall --no-deps "xbbg=={distribution_version}"` or '
        f'`conda install --force-reinstall "xbbg={distribution_version}"`.'
    )


def _source_checkout(package_dir: Path) -> Path | None:
    """Return the repository root when xbbg is imported from its own source tree."""
    src = package_dir.parent
    if src.name != "src" or src.parent.name != "py-xbbg":
        return None
    root = src.parent.parent
    return root if (root / ".git").exists() else None


def _checkout_head(root: Path) -> str | None:
    """Resolve the checkout's ``HEAD`` commit from ``.git`` files, without running git."""
    git_dir = root / ".git"
    try:
        if git_dir.is_file():  # linked worktree: "gitdir: <path>"
            git_dir = root / git_dir.read_text(encoding="utf-8").partition("gitdir:")[2].strip()
        head = (git_dir / "HEAD").read_text(encoding="utf-8").strip()
        if not head.startswith("ref: "):
            return head or None  # detached HEAD
        ref = head.removeprefix("ref: ")
        common_dir = git_dir
        if (git_dir / "commondir").is_file():
            common_dir = git_dir / (git_dir / "commondir").read_text(encoding="utf-8").strip()
        for base in (git_dir, common_dir):
            if (base / ref).is_file():
                return (base / ref).read_text(encoding="utf-8").strip()
        for line in (common_dir / "packed-refs").read_text(encoding="utf-8").splitlines():
            commit, _, name = line.partition(" ")
            if name == ref:
                return commit
    except OSError:
        return None
    return None


def _warn_if_built_from_other_commit(core: ModuleType, checkout: Path) -> None:
    head = _checkout_head(checkout)
    if head is None:
        return
    build_info = getattr(core, "__build_info__", None)
    built_from = build_info.get("gitCommit") if isinstance(build_info, dict) else None
    if built_from == head:
        return
    origin = f"commit {built_from[:10]}" if built_from and built_from != "unknown" else "an unrecorded commit"
    warnings.warn(
        f"xbbg._core in this source checkout was built from {origin}, but the checkout is at {head[:10]}, "
        "so the native extension may not match the Python sources. Rebuild it with `pixi run install` "
        "(or `pip install -e .`).",
        RuntimeWarning,
        stacklevel=2,
    )


class _CoreFinder(MetaPathFinder):
    """Find ``xbbg._core`` like the default path finder, verifying it once it has loaded."""

    xbbg_core_guard = True

    def __init__(self, distribution_version: str | None) -> None:
        self._distribution_version = distribution_version

    def find_spec(
        self, fullname: str, path: Sequence[str] | None, target: ModuleType | None = None
    ) -> ModuleSpec | None:
        if fullname != CORE_MODULE:
            return None
        spec = PathFinder.find_spec(fullname, path, target)
        if spec is not None and isinstance(spec.loader, ExtensionFileLoader):
            spec.loader = _VerifiedCoreLoader(fullname, spec.loader.path, self._distribution_version)
        return spec


class _VerifiedCoreLoader(ExtensionFileLoader):
    """Extension loader that rejects a binary built for another xbbg before it is used."""

    def __init__(self, name: str, path: str, distribution_version: str | None) -> None:
        super().__init__(name, path)
        self._distribution_version = distribution_version

    def exec_module(self, module: ModuleType) -> None:
        super().exec_module(module)
        verify_core(module, distribution_version=self._distribution_version)
