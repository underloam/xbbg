"""Loading the Bloomberg runtime through the shared locator: what Python passes and how it reports.

The search order lives in Rust (``xbbg-sdk``) and is tested there. These tests cover what Python
adds: the request it builds, ``set_sdk_path`` (including the already-loaded case), the shape of
``get_sdk_info`` and the help text. Most use a stand-in for ``xbbg._core``; the last group runs the
real extension against synthetic libraries and, where ``BLPAPI_ROOT`` names a real runtime, loads it.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import threading
import types

import pytest

pytestmark = pytest.mark.thread_unsafe(reason="monkeypatches xbbg._sdk state, sys.modules and os.environ")

LIBRARY_NAME = "blpapi3_64.dll" if sys.platform == "win32" else "libblpapi3_64.so"
LIBRARY_BYTES = b"synthetic stand-in for a Bloomberg runtime"


class FakeCore:
    """The part of ``xbbg._core`` that ``xbbg._sdk`` uses, with canned answers."""

    def __init__(self, *, candidates=(), rejected=(), loaded=None, load_result=None, load_error=None):
        self.candidates = list(candidates)
        self.rejected = list(rejected)
        self.loaded = loaded
        self.load_result = load_result
        self.load_error = load_error
        self.located: list[dict] = []
        self.load_requests: list[dict] = []

    def locate_sdk(self, request=None):
        self.located.append(request)
        explicit = (request or {}).get("explicit")
        found = [c for c in self.candidates if c["source"] != "explicit" or c["detail"] == explicit]
        return {"candidates": found, "rejected": self.rejected}

    def loaded_sdk(self):
        return self.loaded

    def load_sdk(self, request=None):
        self.load_requests.append(request)
        if self.load_error is not None:
            raise ImportError(self.load_error)
        self.loaded = self.load_result
        return self.load_result


def _candidate(path, source, detail="", version=None):
    return {"path": str(path), "source": source, "detail": str(detail), "version": version}


def _loaded(path, source, detail="", version="3.26.5.1", rejected=()):
    return {"path": str(path), "source": source, "detail": str(detail), "version": version, "rejected": list(rejected)}


@pytest.fixture
def sdk(monkeypatch):
    """``xbbg._sdk`` with empty state and no ``blpapi`` wheel."""
    from xbbg import _sdk

    monkeypatch.setattr(_sdk, "_sdk_info", None)
    monkeypatch.setattr(_sdk, "_manual_sdk_path", None)
    monkeypatch.setattr(_sdk, "_blpapi_package_init", lambda: None)
    return _sdk


@pytest.fixture
def install_core(monkeypatch):
    """Make ``xbbg._sdk`` use ``core`` instead of importing the extension; returns the forgotten cores."""
    from xbbg import _core_guard

    forgotten: list[object] = []

    def install(core):
        monkeypatch.setattr(_core_guard, "import_without_runtime", lambda: core)
        monkeypatch.setattr(_core_guard, "forget_core", forgotten.append)
        return forgotten

    return install


def _sdk_dir(root: Path, name: str = "sdk") -> Path:
    directory = root / name
    directory.mkdir()
    (directory / LIBRARY_NAME).write_bytes(b"stand-in")
    return directory


# --- the request Python builds -----------------------------------------------------------------


def test_request_is_empty_when_python_knows_nothing(sdk):
    assert sdk._locate_request() == {}


def test_request_carries_the_manual_path_and_the_wheel_directory(sdk, monkeypatch, tmp_path):
    wheel = tmp_path / "site" / "blpapi"
    monkeypatch.setattr(sdk, "_blpapi_package_init", lambda: wheel / "__init__.py")
    monkeypatch.setattr(sdk, "_manual_sdk_path", tmp_path / "manual")

    request = sdk._locate_request()

    assert request == {
        "explicit": str(tmp_path / "manual"),
        "hints": [{"label": "blpapi_python", "dir": str(wheel)}],
    }
    # Only what Python alone knows: the environment, the cache and the repository are Rust's.
    assert set(request) <= {"explicit", "hints"}


def test_load_runtime_passes_the_request_and_returns_the_loaded_runtime(sdk, monkeypatch, tmp_path):
    monkeypatch.setattr(sdk, "_manual_sdk_path", tmp_path)
    loaded = _loaded(tmp_path / LIBRARY_NAME, "explicit", tmp_path)
    core = FakeCore(load_result=loaded)

    assert sdk.load_runtime(core) == loaded
    assert core.load_requests == [{"explicit": str(tmp_path)}]
    # A loaded runtime is never searched for again.
    assert sdk.load_runtime(core) == loaded
    assert len(core.load_requests) == 1


def test_load_runtime_failure_raises_the_help_with_every_rejection(sdk):
    reason = "/opt/sdk/libblpapi3_64.so: lacks a required function: blpapi_Foo"
    core = FakeCore(load_error=f"no Bloomberg runtime library could be found or loaded:\n  blpapi_root: {reason}")

    with pytest.raises(ImportError) as raised:
        sdk.load_runtime(core)

    message = str(raised.value)
    assert reason in message
    assert raised.value.name == "xbbg._core"
    assert isinstance(raised.value.__cause__, ImportError)
    # The single search order, in order.
    positions = [
        message.index(token)
        for token in (
            "xbbg.set_sdk_path",
            "BLPAPI_LIB_DIR",
            "BLPAPI_ROOT",
            "XBBG_DEV_SDK_ROOT",
            "blpapi Python package",
            "Bloomberg Terminal (DAPI)",
            "library search path",
            "own library search",
            "xbbg runtime cache",
        )
    ]
    assert positions == sorted(positions)


def test_help_error_counts_as_an_unavailable_core(sdk):
    from xbbg import _core_guard

    with pytest.raises(ImportError) as raised:
        sdk.load_runtime(FakeCore(load_error="nothing found"))

    assert _core_guard.is_native_import_error(raised.value)


# --- set_sdk_path --------------------------------------------------------------------------------


def test_set_sdk_path_rejects_a_missing_directory(sdk, install_core, tmp_path):
    install_core(FakeCore())

    with pytest.raises(ValueError, match="does not exist or is not a directory"):
        sdk.set_sdk_path(tmp_path / "missing")
    with pytest.raises(ValueError, match="does not exist or is not a directory"):
        sdk.set_sdk_path(_sdk_dir(tmp_path) / LIBRARY_NAME)


def test_set_sdk_path_rejects_a_directory_without_a_library(sdk, install_core, tmp_path):
    empty = tmp_path / "empty"
    empty.mkdir()
    core = FakeCore()
    forgotten = install_core(core)

    with pytest.raises(ValueError, match="Could not find Bloomberg SDK library in"):
        sdk.set_sdk_path(empty)

    assert sdk._manual_sdk_path is None
    assert forgotten == [core]  # nothing loaded: the next import loads (and explains) properly


def test_set_sdk_path_records_the_path_and_loads_it(sdk, install_core, tmp_path):
    sdk_dir = _sdk_dir(tmp_path)
    library = sdk_dir / LIBRARY_NAME
    core = FakeCore(
        candidates=[_candidate(library, "explicit", sdk_dir)],
        load_result=_loaded(library, "explicit", sdk_dir),
    )
    forgotten = install_core(core)

    sdk.set_sdk_path(sdk_dir)

    assert sdk._manual_sdk_path == sdk_dir
    assert core.load_requests == [{"explicit": str(sdk_dir)}]
    info = sdk.get_sdk_info()
    assert info["active"] == "manual"
    assert info["sources"][0]["path"] == library
    assert forgotten == []


def test_set_sdk_path_after_the_runtime_is_loaded_elsewhere_is_an_error(sdk, install_core, tmp_path):
    first = _sdk_dir(tmp_path, "first")
    second = _sdk_dir(tmp_path, "second")
    core = FakeCore(
        candidates=[_candidate(second / LIBRARY_NAME, "explicit", second)],
        loaded=_loaded(first / LIBRARY_NAME, "blpapi_root", "BLPAPI_ROOT"),
    )
    install_core(core)

    with pytest.raises(RuntimeError, match=r"already loaded from .*first.* cannot be replaced"):
        sdk.set_sdk_path(second)

    assert sdk._manual_sdk_path is None
    assert core.load_requests == []


def test_set_sdk_path_to_the_loaded_runtime_is_accepted(sdk, install_core, tmp_path):
    sdk_dir = _sdk_dir(tmp_path)
    library = sdk_dir / LIBRARY_NAME
    core = FakeCore(
        candidates=[_candidate(library, "explicit", sdk_dir)],
        loaded=_loaded(library, "blpapi_root", "BLPAPI_ROOT"),
    )
    install_core(core)

    sdk.set_sdk_path(sdk_dir)

    assert sdk._manual_sdk_path == sdk_dir


def test_set_sdk_path_failure_to_load_restores_the_previous_path(sdk, install_core, monkeypatch, tmp_path):
    previous = _sdk_dir(tmp_path, "previous")
    sdk_dir = _sdk_dir(tmp_path, "new")
    monkeypatch.setattr(sdk, "_manual_sdk_path", previous)
    core = FakeCore(
        candidates=[_candidate(sdk_dir / LIBRARY_NAME, "explicit", sdk_dir)],
        load_error="no Bloomberg runtime library could be found or loaded",
    )
    forgotten = install_core(core)

    with pytest.raises(ValueError, match="could not be loaded"):
        sdk.set_sdk_path(sdk_dir)

    assert sdk._manual_sdk_path == previous
    assert forgotten == [core]


def test_set_sdk_path_names_why_its_runtime_was_refused_and_tries_nothing_else(sdk, install_core, tmp_path):
    sdk_dir = _sdk_dir(tmp_path, "old")
    explicit = sdk_dir / LIBRARY_NAME
    refusal = f"explicit ({sdk_dir}): {explicit}: lacks blpapi_Old (1 of the 157 BLPAPI functions this build calls)"
    core = FakeCore(
        candidates=[_candidate(explicit, "explicit", sdk_dir)],
        load_error=f"no Bloomberg runtime library could be found or loaded:\n  {refusal}",
    )
    install_core(core)

    with pytest.raises(ValueError) as raised:
        sdk.set_sdk_path(sdk_dir)

    message = str(raised.value)
    assert message == f"The Bloomberg runtime in {sdk_dir} could not be loaded:\n  {refusal}"
    # The path is authoritative: the search order that was not walked is not advertised.
    assert "BLPAPI_LIB_DIR" not in message
    assert core.load_requests == [{"explicit": str(sdk_dir)}]
    assert sdk._manual_sdk_path is None


def test_set_sdk_path_reports_a_runtime_loaded_meanwhile(sdk, install_core, tmp_path):
    sdk_dir = _sdk_dir(tmp_path, "new")
    other = _sdk_dir(tmp_path, "other") / LIBRARY_NAME

    class RacingCore(FakeCore):
        def load_sdk(self, request=None):
            # Another thread's import loaded a different runtime first.
            self.loaded = _loaded(other, "blpapi_root", "BLPAPI_ROOT")
            raise ImportError("a Bloomberg runtime is already loaded")

    forgotten = install_core(RacingCore(candidates=[_candidate(sdk_dir / LIBRARY_NAME, "explicit", sdk_dir)]))

    with pytest.raises(RuntimeError, match=r"already loaded from .*other.* cannot be replaced"):
        sdk.set_sdk_path(sdk_dir)

    assert sdk._manual_sdk_path is None
    assert forgotten == []  # a runtime is loaded: the imported extension stays


def test_clear_sdk_path_forgets_the_path_and_the_cached_report(sdk, monkeypatch, tmp_path):
    monkeypatch.setattr(sdk, "_manual_sdk_path", tmp_path)
    monkeypatch.setattr(sdk, "_sdk_info", {"sources": [], "active": None, "runtime_version": None})

    sdk.clear_sdk_path()

    assert sdk._manual_sdk_path is None
    assert sdk._sdk_info is None
    assert sdk._locate_request() == {}


# --- get_sdk_info --------------------------------------------------------------------------------


def test_get_sdk_info_shape_and_source_names(sdk, install_core, tmp_path):
    dirs = {name: _sdk_dir(tmp_path, name) for name in ("lib", "root", "dev", "wheel", "dapi1", "dapi2", "path")}
    cache_install = tmp_path / "cache" / "sdk" / "3.26.1.1" / "linux-x86_64" / "pypi-abc"
    cache_library = cache_install / "lib" / LIBRARY_NAME
    broken_install = tmp_path / "cache" / "sdk" / "3.20.0.1" / "linux-x86_64" / "pypi-def"
    candidates = [
        _candidate(dirs["lib"] / LIBRARY_NAME, "blpapi_lib_dir", "BLPAPI_LIB_DIR"),
        _candidate(dirs["root"] / LIBRARY_NAME, "blpapi_root", "BLPAPI_ROOT"),
        _candidate(dirs["dev"] / LIBRARY_NAME, "dev_sdk_root", "XBBG_DEV_SDK_ROOT"),
        _candidate(dirs["wheel"] / LIBRARY_NAME, "hint", "blpapi_python"),
        _candidate(dirs["dapi1"] / LIBRARY_NAME, "terminal", dirs["dapi1"]),
        _candidate(dirs["dapi2"] / LIBRARY_NAME, "terminal", dirs["dapi2"]),
        _candidate(dirs["path"] / LIBRARY_NAME, "search_path", "PATH"),
        _candidate(cache_library, "cache", cache_install, version="3.26.1.1"),
    ]
    rejected = [
        {
            "path": str(broken_install),
            "source": "cache",
            "detail": str(broken_install),
            "reason": "the library does not match its recorded hash",
        }
    ]
    loaded = _loaded(dirs["root"] / LIBRARY_NAME, "blpapi_root", "BLPAPI_ROOT", version="3.26.5.1")
    install_core(FakeCore(candidates=candidates, rejected=rejected, loaded=loaded))

    info = sdk.get_sdk_info()

    assert set(info) == {"sources", "active", "runtime_version"}
    assert info["active"] == "sdk_env"
    assert info["runtime_version"] == "3.26.5.1"
    assert [source["name"] for source in info["sources"]] == [
        "sdk_lib_dir",
        "sdk_env",
        "dev_sdk_root",
        "blpapi_python",
        "dapi",  # one entry per source name: the first Terminal root
        "search_path",
        "xbbg_cache",
    ]
    by_name = {source["name"]: source for source in info["sources"]}
    assert by_name["dapi"]["path"] == dirs["dapi1"] / LIBRARY_NAME
    assert by_name["sdk_env"]["version"] == "3.26.5.1"  # the loaded runtime's version fills the active source
    assert by_name["sdk_lib_dir"]["version"] is None
    assert by_name["xbbg_cache"] == {
        "name": "xbbg_cache",
        "version": "3.26.1.1",
        "path": cache_library,
        "install_dir": cache_install,
        "rejected": [
            {"install_dir": broken_install, "reason": "the library does not match its recorded hash"},
        ],
    }
    assert all(set(source) >= {"name", "version", "path"} for source in info["sources"])


def test_get_sdk_info_reports_the_manual_path_first_and_active(sdk, install_core, tmp_path):
    sdk_dir = _sdk_dir(tmp_path)
    library = sdk_dir / LIBRARY_NAME
    other = _sdk_dir(tmp_path, "other")
    install_core(
        FakeCore(
            candidates=[
                _candidate(library, "explicit", sdk_dir),
                _candidate(other / LIBRARY_NAME, "blpapi_root", "BLPAPI_ROOT"),
            ],
            loaded=_loaded(library, "explicit", sdk_dir),
        )
    )
    sdk._manual_sdk_path = sdk_dir

    info = sdk.get_sdk_info()

    assert [source["name"] for source in info["sources"]] == ["manual", "sdk_env"]
    assert info["active"] == "manual"


def test_get_sdk_info_names_a_runtime_loaded_outside_the_locator_by_its_file(sdk, install_core, tmp_path):
    root = _sdk_dir(tmp_path)
    library = root / LIBRARY_NAME
    loaded = _loaded(library, None, "")
    install_core(
        FakeCore(candidates=[_candidate(library, "blpapi_root", "BLPAPI_ROOT")], loaded=loaded),
    )

    assert sdk.get_sdk_info()["active"] == "sdk_env"


@pytest.mark.parametrize(("kind", "name"), [("terminal", "dapi"), ("cache", "xbbg_cache")])
def test_get_sdk_info_active_source_names_the_loaded_file_not_its_first_candidate(
    sdk, install_core, tmp_path, kind, name
):
    refused = _sdk_dir(tmp_path, "old") / LIBRARY_NAME
    loaded = _sdk_dir(tmp_path, "current") / LIBRARY_NAME
    install_core(
        FakeCore(
            candidates=[
                _candidate(refused, kind, refused.parent, version="3.1.0.0"),
                _candidate(loaded, kind, loaded.parent, version="3.26.5.1"),
            ],
            load_result=_loaded(loaded, kind, loaded.parent),
        ),
    )

    info = sdk.get_sdk_info()

    assert info["active"] == name
    [active] = info["sources"]
    assert active["path"] == loaded
    assert active["version"] == info["runtime_version"] == "3.26.5.1"
    if kind == "cache":
        assert active["install_dir"] == loaded.parent


def test_get_sdk_info_lists_a_runtime_only_the_operating_system_found(sdk, install_core, tmp_path):
    # The operating system's own search has no candidate before it loads one: the search lists a
    # runtime that was refused and the cache, the load reports where the runtime came from.
    refused = _sdk_dir(tmp_path, "old") / LIBRARY_NAME
    cached = _sdk_dir(tmp_path, "install") / LIBRARY_NAME
    system = _sdk_dir(tmp_path, "system") / LIBRARY_NAME
    install_core(
        FakeCore(
            candidates=[
                _candidate(refused, "blpapi_root", "BLPAPI_ROOT"),
                _candidate(cached, "cache", cached.parent, "3.26.9.1"),
            ],
            load_result=_loaded(system, "system", "the operating system's library search"),
        ),
    )

    info = sdk.get_sdk_info()

    assert info["active"] == "system"
    assert [source["name"] for source in info["sources"]] == ["sdk_env", "system", "xbbg_cache"]
    assert info["sources"][1] == {"name": "system", "version": "3.26.5.1", "path": system}


def test_get_sdk_info_without_a_runtime_reports_the_sources_and_no_active_one(sdk, install_core, tmp_path):
    root = _sdk_dir(tmp_path)
    core = FakeCore(
        candidates=[_candidate(root / LIBRARY_NAME, "blpapi_root", "BLPAPI_ROOT")],
        load_error="no Bloomberg runtime library could be found or loaded",
    )
    forgotten = install_core(core)

    info = sdk.get_sdk_info()

    assert info["active"] is None
    assert info["runtime_version"] is None
    assert [source["name"] for source in info["sources"]] == ["sdk_env"]
    assert forgotten == [core]


def test_get_sdk_info_without_the_extension_is_empty(sdk, monkeypatch):
    from xbbg import _core_guard

    def missing():
        raise ImportError("No module named 'xbbg._core'", name="xbbg._core")

    monkeypatch.setattr(_core_guard, "import_without_runtime", missing)

    assert sdk.get_sdk_info() == {"sources": [], "active": None, "runtime_version": None}


def test_get_sdk_info_returns_caller_owned_copies(sdk, install_core, tmp_path):
    root = _sdk_dir(tmp_path)
    library = root / LIBRARY_NAME
    install_core(
        FakeCore(
            candidates=[_candidate(library, "blpapi_root", "BLPAPI_ROOT")],
            loaded=_loaded(library, "blpapi_root", "BLPAPI_ROOT"),
        )
    )

    first = sdk.get_sdk_info()
    expected = {**first, "sources": [dict(source) for source in first["sources"]]}
    first["sources"][0]["name"] = "mutated"
    first["sources"].append({"name": "extra"})
    first["active"] = "mutated"

    assert sdk.get_sdk_info() == expected


def test_sdk_info_computed_before_clear_sdk_path_is_not_published(sdk, install_core, tmp_path):
    """A detection that started before ``clear_sdk_path`` must not repopulate the cache."""
    sdk_dir = _sdk_dir(tmp_path)
    library = sdk_dir / LIBRARY_NAME
    probing = threading.Event()
    release = threading.Event()
    probes: list[str] = []

    class SlowCore(FakeCore):
        def locate_sdk(self, request=None):
            probes.append(threading.current_thread().name)
            if len(probes) == 1:
                probing.set()
                if not release.wait(30):
                    raise TimeoutError("detection was never released")
            return super().locate_sdk(request)

    core = SlowCore(
        candidates=[_candidate(library, "explicit", sdk_dir)],
        loaded=_loaded(library, "explicit", sdk_dir),
    )
    install_core(core)
    sdk._manual_sdk_path = sdk_dir

    stale: dict = {}
    detector = threading.Thread(target=lambda: stale.update(sdk.get_sdk_info()), name="detector")
    detector.start()
    try:
        assert probing.wait(30)
        sdk.clear_sdk_path()
    finally:
        release.set()
        detector.join(30)

    assert not detector.is_alive()
    # The in-flight caller gets the answer for the state it started from...
    assert stale["active"] == "manual"
    # ...but the next call re-detects instead of reading that stale answer.
    core.candidates = []
    core.loaded = None
    core.load_result = None
    core.load_error = "nothing"
    fresh = sdk.get_sdk_info()
    assert fresh["active"] is None
    assert fresh["sources"] == []
    assert probes == ["detector", threading.current_thread().name]


def test_blpapi_wheel_entry_takes_the_distribution_version_when_the_search_has_none(
    sdk, install_core, monkeypatch, tmp_path
):
    wheel = _sdk_dir(tmp_path, "wheel")
    install_core(
        FakeCore(
            candidates=[_candidate(wheel / LIBRARY_NAME, "hint", "blpapi_python")],
            loaded=_loaded(tmp_path / "elsewhere", "blpapi_root", "BLPAPI_ROOT"),
        )
    )
    monkeypatch.setattr(sdk, "_blpapi_distribution_version", lambda: "3.25.11.1")

    info = sdk.get_sdk_info()

    # The loaded runtime is always listed, so ``active`` names an entry of ``sources``.
    assert [source["name"] for source in info["sources"]] == ["blpapi_python", "sdk_env"]
    assert info["sources"][0]["version"] == "3.25.11.1"
    assert info["active"] == "sdk_env"


# --- the import guard ----------------------------------------------------------------------------


def _guard_loader(monkeypatch):
    from importlib.machinery import ExtensionFileLoader

    import xbbg
    from xbbg import _core_guard

    monkeypatch.setattr(ExtensionFileLoader, "exec_module", lambda self, module: None)
    monkeypatch.setattr(_core_guard, "verify_core", lambda *args, **kwargs: None)
    # The loader the package installs, which looks up `_sdk.load_runtime` on each call.
    return _core_guard._VerifiedCoreLoader("xbbg._core", "_core.so", None, xbbg._load_runtime)


def test_importing_the_extension_loads_the_runtime(monkeypatch):
    from xbbg import _sdk

    loader = _guard_loader(monkeypatch)
    module = types.ModuleType("xbbg._core")
    loaded = []
    monkeypatch.setattr(_sdk, "load_runtime", loaded.append)

    loader.exec_module(module)

    assert loaded == [module]


def test_a_missing_runtime_fails_the_import_with_the_help(monkeypatch):
    from xbbg import _sdk

    loader = _guard_loader(monkeypatch)
    module = types.ModuleType("xbbg._core")
    module.loaded_sdk = lambda: None
    module.load_sdk = lambda request=None: (_ for _ in ()).throw(ImportError("nothing found"))
    monkeypatch.setattr(_sdk, "_blpapi_package_init", lambda: None)

    with pytest.raises(ImportError, match="BLPAPI_ROOT"):
        loader.exec_module(module)


def test_a_deferred_import_does_not_load_the_runtime(monkeypatch):
    from xbbg import _core_guard, _sdk

    loader = _guard_loader(monkeypatch)
    monkeypatch.setattr(_sdk, "load_runtime", lambda module: pytest.fail("the runtime must not load"))
    monkeypatch.setattr(_core_guard, "_deferral", types.SimpleNamespace(active=True))

    loader.exec_module(types.ModuleType("xbbg._core"))


def test_forget_core_drops_the_module_and_the_package_attribute(monkeypatch):
    from xbbg import _core_guard

    package = types.ModuleType("xbbg")
    core = types.ModuleType("xbbg._core")
    package._core = core
    monkeypatch.setitem(sys.modules, "xbbg", package)
    monkeypatch.setitem(sys.modules, "xbbg._core", core)

    _core_guard.forget_core(core)

    assert "xbbg._core" not in sys.modules
    assert "_core" not in vars(package)
    # Another module object under the same name is not touched.
    other = types.ModuleType("xbbg._core")
    monkeypatch.setitem(sys.modules, "xbbg._core", other)
    _core_guard.forget_core(core)
    assert sys.modules["xbbg._core"] is other


# --- the real extension ----------------------------------------------------------------------------


@pytest.fixture
def core():
    return pytest.importorskip("xbbg._core")


def _host_platform() -> str:
    machine = platform.machine().lower()
    x86_64 = machine in {"x86_64", "amd64"}
    if sys.platform == "win32" and x86_64:
        return "windows-x86_64"
    if sys.platform.startswith("linux") and x86_64:
        return "linux-x86_64"
    if sys.platform == "darwin" and machine in {"arm64", "aarch64"}:
        return "macos-aarch64"
    pytest.skip("the xbbg cache has no platform for this host")


def _install(root: Path, version: str, *, name: str = "install", content: bytes = LIBRARY_BYTES) -> tuple[Path, Path]:
    """Write one cache install as the provisioner would; returns ``(install_dir, library)``."""
    install_dir = root / "sdk" / version / _host_platform() / name
    library = install_dir / "lib" / LIBRARY_NAME
    library.parent.mkdir(parents=True)
    library.write_bytes(content)
    provenance = {
        "version": version,
        "platform": _host_platform(),
        "source": "artifact:synthetic",
        "artifactUrl": "file:synthetic",
        "artifactSha256": hashlib.sha256(b"archive").hexdigest(),
        "lib": f"lib/{LIBRARY_NAME}",
        "libSha256": hashlib.sha256(LIBRARY_BYTES).hexdigest(),
        "licenses": [],
    }
    (install_dir / "provenance.json").write_text(json.dumps(provenance), encoding="utf-8")
    return install_dir, library


def _empty_env(tmp_path: Path) -> dict[str, str]:
    """An environment with nothing the search reads, so a request alone decides."""
    return {"XBBG_CACHE_DIR": str(tmp_path / "no-cache"), "HOME": str(tmp_path / "home")}


def test_locate_sdk_lists_an_explicit_directory(core, tmp_path):
    sdk_dir = _sdk_dir(tmp_path)

    located = core.locate_sdk({"explicit": sdk_dir, "env": _empty_env(tmp_path)})

    [first, *_] = located["candidates"]
    assert first["source"] == "explicit"
    assert Path(first["path"]).resolve() == (sdk_dir / LIBRARY_NAME).resolve()
    assert located["rejected"] == []


def test_locate_sdk_without_a_request_searches_with_the_process_environment(core):
    located = core.locate_sdk()

    assert set(located) == {"candidates", "rejected"}


def test_locate_sdk_reads_the_hosts_environment_view(core, tmp_path):
    root = _sdk_dir(tmp_path, "root")

    located = core.locate_sdk({"env": {**_empty_env(tmp_path), "BLPAPI_ROOT": str(root)}})

    assert [(c["source"], c["detail"]) for c in located["candidates"]] == [("blpapi_root", "BLPAPI_ROOT")]


def test_locate_sdk_orders_hints_after_the_environment(core, tmp_path):
    root = _sdk_dir(tmp_path, "root")
    wheel = _sdk_dir(tmp_path, "wheel")

    located = core.locate_sdk(
        {
            "hints": [{"label": "blpapi_python", "dir": str(wheel)}],
            "env": {**_empty_env(tmp_path), "BLPAPI_ROOT": str(root)},
        }
    )

    assert [c["source"] for c in located["candidates"]] == ["blpapi_root", "hint"]
    assert located["candidates"][1]["detail"] == "blpapi_python"


def test_locate_sdk_verifies_the_cache_and_reports_what_it_refused(core, tmp_path):
    cache = tmp_path / "cache"
    good_dir, good_library = _install(cache, "3.26.1.1", name="good")
    bad_dir, bad_library = _install(cache, "3.20.0.1", name="bad")
    bad_library.write_bytes(b"tampered")

    located = core.locate_sdk({"cacheRoot": str(cache), "env": {"HOME": str(tmp_path / "home")}})

    [candidate] = [c for c in located["candidates"] if c["source"] == "cache"]
    assert Path(candidate["path"]) == good_library
    assert Path(candidate["detail"]) == good_dir
    assert candidate["version"] == "3.26.1.1"
    [refused] = [r for r in located["rejected"] if r["source"] == "cache"]
    assert Path(refused["path"]) == bad_dir
    assert refused["reason"]


def test_locate_sdk_rejects_a_malformed_request(core):
    with pytest.raises(ValueError, match="invalid Bloomberg runtime request"):
        core.locate_sdk({"explicitly": "x"})


def _run_core(script: str, tmp_path: Path, **env: str) -> subprocess.CompletedProcess[str]:
    base = {
        key: value for key, value in os.environ.items() if not key.startswith(("BLPAPI_", "XBBG_DEV_", "DYLD_", "LD_"))
    }
    xbbg_src = str(Path(sys.modules["xbbg"].__file__).resolve().parents[1])
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**base, "PYTHONPATH": xbbg_src, **env},
        timeout=300,
        check=False,
    )


def test_load_sdk_with_blpapi_root_loads_the_runtime_and_reports_it(tmp_path):
    root = os.environ.get("BLPAPI_ROOT")
    if not root or not Path(root).is_dir():
        pytest.skip("BLPAPI_ROOT does not name a Bloomberg SDK")
    script = (
        "import json\n"
        "from xbbg import _core\n"  # the import guard loads the runtime
        "again = _core.load_sdk()\n"
        "try:\n"
        "    _core.load_sdk({'explicit': 'does-not-exist'})\n"
        "    replaced = None\n"
        "except ImportError as exc:\n"
        "    replaced = str(exc)\n"
        "print(json.dumps({'loaded': _core.loaded_sdk(), 'again': again, 'replaced': replaced,"
        " 'version': list(_core.sdk_version())}))\n"
    )

    result = _run_core(script, tmp_path, BLPAPI_ROOT=root)

    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    loaded = payload["loaded"]
    assert loaded["source"] == "blpapi_root"
    assert loaded["detail"] == "BLPAPI_ROOT"
    assert loaded["version"] == ".".join(str(part) for part in payload["version"])
    assert Path(loaded["path"]).name.startswith(("libblpapi3", "blpapi3"))
    assert Path(root).resolve() in Path(loaded["path"]).resolve().parents
    # Without an explicit path, loading again returns the loaded runtime.
    assert payload["again"]["path"] == loaded["path"]
    # A different explicit path never replaces the loaded runtime.
    assert payload["replaced"] is not None
    assert "already loaded" in payload["replaced"]


def test_the_runtime_search_reaches_the_explicit_path_first(tmp_path):
    root = os.environ.get("BLPAPI_ROOT")
    if not root or not Path(root).is_dir():
        pytest.skip("BLPAPI_ROOT does not name a Bloomberg SDK")
    script = (
        "import json\n"
        "from xbbg import _sdk\n"
        "info = _sdk.get_sdk_info()\n"
        "print(json.dumps({'names': [s['name'] for s in info['sources']], 'active': info['active'],"
        " 'runtime_version': info['runtime_version']}))\n"
    )

    result = _run_core(script, tmp_path, BLPAPI_ROOT=root)

    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["active"] == "sdk_env"
    assert "sdk_env" in payload["names"]
    assert payload["runtime_version"]


def test_sdk_version_refuses_to_abort_without_a_runtime(core, tmp_path):
    script = (
        "import importlib.util, sys, xbbg\n"
        "from importlib.machinery import PathFinder\n"
        "spec = PathFinder.find_spec('xbbg._core', list(xbbg.__path__))\n"
        "module = importlib.util.module_from_spec(spec)\n"  # no guard, so no runtime
        "spec.loader.exec_module(module)\n"
        "assert module.loaded_sdk() is None\n"
        "try:\n"
        "    module.sdk_version()\n"
        "except RuntimeError as error:\n"
        "    print('refused:', error)\n"
    )

    result = _run_core(script, tmp_path, XBBG_CACHE_DIR=str(tmp_path / "no-cache"), HOME=str(tmp_path / "home"))

    assert result.returncode == 0, result.stdout + result.stderr
    assert "refused:" in result.stdout
