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
@pytest.mark.thread_unsafe(reason="each parametrized pytest.raises context is one object shared by all threads")
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
        import sys
        import types

        version_file = types.ModuleType("xbbg._version")
        version_file.version = "0.0.0"
        sys.modules["xbbg._version"] = version_file
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


@pytest.mark.parametrize(
    ("message", "name", "unavailable"),
    [
        ("No module named 'xbbg._core'", "xbbg._core", True),
        ("DLL load failed while importing _core", None, True),
        ("xbbg: cannot open shared object file", None, True),
        ("xbbg._core: image not found", None, True),
        ("Library not loaded: xbbg._core", None, True),
        ("cannot import name 'ext_missing' from 'xbbg._core'", "xbbg._core", False),
        ("No module named 'pandas'", "pandas", False),
        ("DLL load failed while importing unrelated", "unrelated", False),
    ],
)
def test_native_import_classifier_distinguishes_missing_from_outdated(message, name, unavailable):
    assert _core_guard.is_native_import_error(ImportError(message, name=name)) is unavailable


_STAND_IN_CORE = textwrap.dedent(
    """
    import importlib.abc
    import importlib.util
    import sys

    import xbbg

    assert "xbbg._core" not in sys.modules, "xbbg._core was imported by `import xbbg`"


    def install_stand_in_core(exec_module):
        # Serve a pure-Python xbbg._core ahead of the build guard and the real extension.
        class StandInLoader(importlib.abc.Loader):
            def create_module(self, spec):
                return None

            def exec_module(self, module):
                exec_module(module)

        class StandInFinder(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname == "xbbg._core":
                    return importlib.util.spec_from_loader(fullname, StandInLoader())
                return None

        sys.meta_path.insert(0, StandInFinder())
    """
)


def _run_with_stand_in_core(script: str) -> subprocess.CompletedProcess[str]:
    import xbbg

    env = {**os.environ, "PYTHONPATH": str(Path(xbbg.__file__).resolve().parents[1])}
    return subprocess.run(
        [sys.executable, "-c", _STAND_IN_CORE + textwrap.dedent(script)],
        capture_output=True,
        text=True,
        env=env,
        timeout=180,
        check=False,
    )


def test_concurrent_first_access_waits_for_the_importing_thread():
    """Threads that reach ``_core`` while another thread imports it share that import."""
    result = _run_with_stand_in_core(
        """
        import threading
        import time

        entered = threading.Event()
        release = threading.Event()


        def blocking_exec(module):
            entered.set()
            if not release.wait(60):
                raise RuntimeError("stand-in xbbg._core was never released")
            module.STAND_IN = True


        install_stand_in_core(blocking_exec)
        results = {}


        def record(name, call):
            try:
                results[name] = call()
            except BaseException as exc:
                results[name] = exc


        def from_import():
            from xbbg import _core

            return _core


        def waiting_on_import_lock(thread):
            frame = sys._current_frames().get(thread.ident)
            while frame is not None:
                if frame.f_code.co_name == "acquire" and "_bootstrap" in frame.f_code.co_filename:
                    return True
                frame = frame.f_back
            return False


        importer = threading.Thread(target=record, args=("importer", xbbg._import_core))
        importer.start()
        if not entered.wait(60):
            raise SystemExit("stand-in xbbg._core never started executing")

        waiters = [
            threading.Thread(target=record, args=("import_core", xbbg._import_core)),
            threading.Thread(target=record, args=("from_import", from_import)),
        ]
        for waiter in waiters:
            waiter.start()
        deadline = time.monotonic() + 60
        while not all(not waiter.is_alive() or waiting_on_import_lock(waiter) for waiter in waiters):
            if time.monotonic() > deadline:
                raise SystemExit("waiting threads never reached importlib's module lock")
            time.sleep(0.01)

        release.set()
        for thread in (importer, *waiters):
            thread.join(60)

        failures = {name: repr(value) for name, value in results.items() if isinstance(value, BaseException)}
        if failures:
            raise SystemExit(f"concurrent _core access failed: {failures}")
        modules = {id(module) for module in results.values()}
        if len(results) != 3 or len(modules) != 1 or not getattr(results["importer"], "STAND_IN", False):
            raise SystemExit(f"threads saw different _core modules: {results}")
        print("shared")
        """
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Recursive import" not in result.stdout + result.stderr
    assert result.stdout.strip().endswith("shared")


def test_same_thread_reentry_is_still_reported():
    result = _run_with_stand_in_core(
        """
        seen = []


        def reentrant_exec(module):
            try:
                xbbg._import_core()
            except ImportError as exc:
                seen.append(str(exc))
            else:
                seen.append("re-entry returned without error")


        install_stand_in_core(reentrant_exec)
        core = xbbg._import_core()
        if seen != ["Recursive import of _core detected"]:
            raise SystemExit(f"unexpected re-entry outcome: {seen}")
        if xbbg._import_core() is not core:
            raise SystemExit("_core was not cached after the import finished")
        print("reported")
        """
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip().endswith("reported")
