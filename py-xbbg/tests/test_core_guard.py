"""An ``xbbg._core`` from another build must not load silently."""

from __future__ import annotations

import contextlib
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import types
import warnings

import pytest

from xbbg import _core_guard

HEAD = "1" * 40
OLDER = "2" * 40


def _core(version: str, commit: str | None = None) -> types.ModuleType:
    core = types.ModuleType("xbbg._core")
    core.__dict__["__version__"] = version
    if commit is not None:
        core.__dict__["__build_info__"] = {"gitCommit": commit}
    return core


def _installed_package(tmp_path: Path) -> Path:
    package_dir = tmp_path / "site-packages" / "xbbg"
    package_dir.mkdir(parents=True)
    return package_dir


def _checkout_package(tmp_path: Path, layout: str) -> Path:
    """Create a source checkout whose ``HEAD`` resolves to ``HEAD`` through ``layout``."""
    root = tmp_path / "xbbg"
    package_dir = root / "py-xbbg" / "src" / "xbbg"
    package_dir.mkdir(parents=True)
    if layout == "worktree":
        common_dir = tmp_path / "main" / ".git"
        git_dir = common_dir / "worktrees" / "xbbg"
        git_dir.mkdir(parents=True)
        (root / ".git").write_text(f"gitdir: {git_dir}\n", encoding="utf-8")
        (git_dir / "commondir").write_text("../..\n", encoding="utf-8")
        (git_dir / "HEAD").write_text("ref: refs/heads/feature\n", encoding="utf-8")
        (common_dir / "packed-refs").write_text(f"{HEAD} refs/heads/feature\n", encoding="utf-8")
        return package_dir
    git_dir = root / ".git"
    git_dir.mkdir()
    if layout == "detached":
        (git_dir / "HEAD").write_text(f"{HEAD}\n", encoding="utf-8")
        return package_dir
    (git_dir / "HEAD").write_text("ref: refs/heads/main\n", encoding="utf-8")
    if layout == "loose":
        (git_dir / "refs" / "heads").mkdir(parents=True)
        (git_dir / "refs" / "heads" / "main").write_text(f"{HEAD}\n", encoding="utf-8")
    else:
        (git_dir / "packed-refs").write_text(
            f"# pack-refs with: peeled fully-peeled sorted\n{OLDER} refs/heads/other\n{HEAD} refs/heads/main\n",
            encoding="utf-8",
        )
    return package_dir


@pytest.mark.parametrize(
    ("distribution_version", "outcome"),
    [
        ("1.5.1", pytest.raises(ImportError, match=r"built for xbbg 1\.5\.0, but the installed xbbg is 1\.5\.1")),
        ("1.5.0", contextlib.nullcontext()),
        (None, contextlib.nullcontext()),  # no package metadata: nothing to compare against
    ],
)
def test_installed_extension_must_be_built_for_the_installed_distribution(tmp_path, distribution_version, outcome):
    package_dir = _installed_package(tmp_path)

    with outcome:
        _core_guard.verify_core(_core("1.5.0"), distribution_version=distribution_version, package_dir=package_dir)


@pytest.mark.parametrize("layout", ["loose", "packed", "detached", "worktree"])
def test_source_checkout_compares_the_build_commit_with_head(tmp_path, layout):
    """Editable metadata is as old as the in-place extension, so a checkout is checked against HEAD."""
    package_dir = _checkout_package(tmp_path, layout)

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        _core_guard.verify_core(_core("1.4.12", HEAD), distribution_version="1.5.0", package_dir=package_dir)

    with pytest.warns(RuntimeWarning, match=f"built from commit {OLDER[:10]}, but the checkout is at {HEAD[:10]}"):
        _core_guard.verify_core(_core("1.4.12", OLDER), distribution_version="1.5.0", package_dir=package_dir)


def test_source_checkout_extension_without_a_build_commit_is_reported(tmp_path):
    package_dir = _checkout_package(tmp_path, "loose")

    with pytest.warns(RuntimeWarning, match="built from an unrecorded commit"):
        _core_guard.verify_core(_core("1.4.6-dirty"), distribution_version="1.4.7.dev0", package_dir=package_dir)


def test_importing_names_from_a_mismatched_extension_is_refused():
    """``from xbbg._core import X`` bypasses ``xbbg.__getattr__`` but still goes through the guard."""
    import xbbg

    script = textwrap.dedent(
        """
        import importlib.metadata

        importlib.metadata.version = lambda name: "0.0.0"
        import xbbg._core_guard as guard

        guard._source_checkout = lambda package_dir: None  # judge it as an installed package
        try:
            from xbbg._core import ArrowTable
        except ImportError as exc:
            print(exc)
        else:
            raise SystemExit("xbbg._core loaded despite the version mismatch")
        """
    )
    env = {**os.environ, "PYTHONPATH": str(Path(xbbg.__file__).resolve().parents[1])}
    result = subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, env=env, timeout=120, check=False
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "but the installed xbbg is 0.0.0" in result.stdout
