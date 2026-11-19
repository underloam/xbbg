"""Bloomberg SDK cache provisioning via Python API, CLI, and automatic fallback."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import types
import zipfile

import pytest

pytestmark = pytest.mark.thread_unsafe(reason="mutates xbbg._sdk state and environment")


def _host_platform_and_wheel_tag() -> tuple[str, str, str] | None:
    machine = platform.machine().lower()
    x86_64 = machine in {"x86_64", "amd64"}
    if sys.platform == "win32" and x86_64:
        return ("windows-x86_64", "win_amd64", "blpapi3_64.dll")
    if sys.platform.startswith("linux") and x86_64:
        return ("linux-x86_64", "manylinux_2_17_x86_64", "libblpapi3_64.so")
    if sys.platform == "darwin" and machine in {"arm64", "aarch64"}:
        return ("macos-aarch64", "macosx_11_0_arm64", "libblpapi3_64.so")
    return None


def _make_wheel(
    directory: Path,
    *,
    version: str = "3.26.9.1",
    wheel_tag: str = "manylinux_2_17_x86_64",
    lib_name: str = "libblpapi3_64.so",
    lib_bytes: bytes = b"synthetic-blpapi-elf-bytes",
) -> Path:
    """Create a valid uncompressed ``.whl`` archive with a SHA-256 ``RECORD``."""
    wheel_path = directory / f"blpapi-{version}-cp312-cp312-{wheel_tag}.whl"
    lib_member = f"blpapi/{lib_name}"
    digest = base64.urlsafe_b64encode(hashlib.sha256(lib_bytes).digest()).rstrip(b"=").decode("ascii")
    dist_info = f"blpapi-{version}.dist-info"
    metadata = f"Metadata-Version: 2.1\nName: blpapi\nVersion: {version}\n".encode()
    wheel_meta = (
        f"Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: false\nTag: cp312-cp312-{wheel_tag}\n"
    ).encode()
    record = f"{lib_member},sha256={digest},{len(lib_bytes)}\n{dist_info}/RECORD,,\n".encode()

    with zipfile.ZipFile(wheel_path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(lib_member, lib_bytes)
        archive.writestr(f"{dist_info}/METADATA", metadata)
        archive.writestr(f"{dist_info}/WHEEL", wheel_meta)
        archive.writestr(f"{dist_info}/RECORD", record)
    return wheel_path


def _scrubbed_env(scratch: Path, **extra: str) -> dict[str, str]:
    stripped = ("BLPAPI_", "XBBG_", "DYLD_", "LD_")
    env = {k: v for k, v in os.environ.items() if not k.upper().startswith(stripped)}
    for key in ("HOME", "USERPROFILE", "LOCALAPPDATA", "XDG_CACHE_HOME", "PROGRAMFILES", "PROGRAMFILES(X86)"):
        env[key] = str(scratch / key.lower().replace("(", "_").replace(")", ""))
    env["XBBG_CACHE_DIR"] = str(scratch / "cache")
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    env.update(extra)
    return env


def _real_blpapi_library() -> Path | None:
    root = os.environ.get("BLPAPI_ROOT")
    if not root:
        return None
    root_path = Path(root)
    if not root_path.is_dir():
        return None
    info = _host_platform_and_wheel_tag()
    if info is None:
        return None
    _, _, lib_name = info
    for candidate in (
        root_path / lib_name,
        root_path / "lib" / lib_name,
        root_path / "Linux" / lib_name,
        root_path / "Darwin" / lib_name,
        root_path / "bin" / lib_name,
    ):
        if candidate.is_file():
            return candidate
    return None


# --- unit tests (state & precedence) -------------------------------------------------------------


def test_set_sdk_source_validation_and_already_loaded_guard(monkeypatch):
    from xbbg import _core_guard, _sdk

    monkeypatch.setattr(_sdk, "_sdk_source", None)
    monkeypatch.setattr(_sdk, "_sdk_info", None)
    monkeypatch.delitem(sys.modules, _core_guard.CORE_MODULE, raising=False)

    with pytest.raises(ValueError, match="must not be blank"):
        _sdk.set_sdk_source("   ")

    _sdk.set_sdk_source("conda:conda-forge")
    assert _sdk.get_sdk_source() == "conda:conda-forge"

    _sdk.set_sdk_source(None)
    assert _sdk.get_sdk_source() is None

    loaded_core = types.ModuleType(_core_guard.CORE_MODULE)
    loaded_core.loaded_sdk = lambda: {"path": "/already/loaded/libblpapi3_64.so"}
    monkeypatch.setitem(sys.modules, _core_guard.CORE_MODULE, loaded_core)

    with pytest.raises(RuntimeError, match="already loaded"):
        _sdk.set_sdk_source("conda:conda-forge")


def test_load_runtime_passes_sdk_source_only_without_explicit_path(monkeypatch, tmp_path):
    from xbbg import _sdk

    calls: list[tuple[dict, str | None]] = []

    class FakeCore:
        def loaded_sdk(self):
            return None

        def load_sdk(self, request=None, sdk_source=None):
            calls.append((request, sdk_source))
            return {
                "path": "/cache/libblpapi3_64.so",
                "source": "cache",
                "detail": "/cache",
                "version": "3.26.9.1",
                "rejected": [],
            }

    monkeypatch.setattr(_sdk, "_manual_sdk_path", None)
    monkeypatch.setattr(_sdk, "_sdk_source", "conda:conda-forge")
    monkeypatch.setattr(_sdk, "_blpapi_package_init", lambda: None)

    _sdk.load_runtime(FakeCore())
    assert calls == [({}, "conda:conda-forge")]

    calls.clear()
    monkeypatch.setattr(_sdk, "_manual_sdk_path", tmp_path)
    _sdk.load_runtime(FakeCore())
    # Explicit path stays authoritative: sdk_source is never forwarded.
    assert calls == [({"explicit": str(tmp_path)}, None)]


def test_provision_sdk_requires_a_source(monkeypatch):
    from xbbg import _sdk

    monkeypatch.setattr(_sdk, "_sdk_source", None)
    monkeypatch.delenv("XBBG_SDK_SOURCE", raising=False)

    with pytest.raises(ValueError, match="source is required"):
        _sdk.provision_sdk()


# --- native integration tests (no Bloomberg runtime required) -----------------------------------


def test_provision_sdk_and_installed_sdks_without_runtime(tmp_path):
    wheel = _make_wheel(tmp_path)
    cache = tmp_path / "cache"
    script = (
        "import json, sys, xbbg\n"
        "from pathlib import Path\n"
        "wheel, cache = sys.argv[1], sys.argv[2]\n"
        "first = xbbg.provision_sdk(wheel, platform='linux-x86_64', cache_dir=cache)\n"
        "second = xbbg.provision_sdk(wheel, platform='linux-x86_64', cache_dir=cache)\n"
        "installed = xbbg.installed_sdks(cache, platform='linux-x86_64')\n"
        "other = xbbg.installed_sdks(cache, platform='macos-aarch64')\n"
        "default_dir = xbbg.default_sdk_cache_dir()\n"
        "print(json.dumps({\n"
        "  'first_version': first['version'],\n"
        "  'first_platform': first['platform'],\n"
        "  'first_cache_hit': first['cache_hit'],\n"
        "  'first_exists': first['library_path'].is_file(),\n"
        "  'second_cache_hit': second['cache_hit'],\n"
        "  'same_path': str(first['library_path']) == str(second['library_path']),\n"
        "  'installed_count': len(installed),\n"
        "  'installed_version': installed[0]['version'],\n"
        "  'other_count': len(other),\n"
        "  'default_dir': str(default_dir) if default_dir else None,\n"
        "  'pypi_index': xbbg.BLOOMBERG_PYPI_INDEX,\n"
        "}))\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script, str(wheel), str(cache)],
        capture_output=True,
        text=True,
        env=_scrubbed_env(tmp_path),
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload["first_version"] == "3.26.9.1"
    assert payload["first_platform"] == "linux-x86_64"
    assert payload["first_cache_hit"] is False
    assert payload["first_exists"] is True
    assert payload["second_cache_hit"] is True
    assert payload["same_path"] is True
    assert payload["installed_count"] == 1
    assert payload["installed_version"] == "3.26.9.1"
    assert payload["other_count"] == 0
    assert payload["default_dir"] == str(cache)
    assert payload["pypi_index"].endswith("/simple/")


def test_python_m_xbbg_sdk_cli_works_without_runtime(tmp_path):
    wheel = _make_wheel(tmp_path)
    cache = tmp_path / "cache"
    env = _scrubbed_env(tmp_path)

    help_run = subprocess.run(
        [sys.executable, "-m", "xbbg.sdk", "--help"],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    assert help_run.returncode == 0, help_run.stdout + help_run.stderr
    assert "usage: xbbg-sdk" in help_run.stdout
    assert "XBBG_SDK_SOURCE" in help_run.stdout

    install_run = subprocess.run(
        [
            sys.executable,
            "-m",
            "xbbg.sdk",
            "--source",
            str(wheel),
            "--platform",
            "linux-x86_64",
            "--cache-dir",
            str(cache),
        ],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    assert install_run.returncode == 0, install_run.stdout + install_run.stderr
    assert "3.26.9.1" in install_run.stdout

    list_run = subprocess.run(
        [sys.executable, "-m", "xbbg.sdk", "--installed", "--cache-dir", str(cache)],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    assert list_run.returncode == 0, list_run.stdout + list_run.stderr
    assert "3.26.9.1" in list_run.stdout
    assert "linux-x86_64" in list_run.stdout


def test_auto_provision_from_env_and_explicit_path_authority(tmp_path):
    host = _host_platform_and_wheel_tag()
    if host is None:
        pytest.skip("no xbbg cache platform for this host")
    platform_name, wheel_tag, lib_name = host
    real_lib = _real_blpapi_library()
    if real_lib is None:
        pytest.skip("BLPAPI_ROOT does not name a real Bloomberg runtime")

    wheel = _make_wheel(
        tmp_path,
        version="3.26.5.1",
        wheel_tag=wheel_tag,
        lib_name=lib_name,
        lib_bytes=real_lib.read_bytes(),
    )

    # 1. XBBG_SDK_SOURCE provisions and loads automatically when nothing else exists.
    auto_script = (
        "import json, xbbg\n"
        "info = xbbg.get_sdk_info()\n"
        "print(json.dumps({\n"
        "  'active': info['active'],\n"
        "  'runtime_version': info['runtime_version'],\n"
        "  'cache_sources': [s['name'] for s in info['sources'] if s['name'] == 'xbbg_cache'],\n"
        "  'install_dir_exists': next(s['install_dir'] for s in info['sources'] if s['name'] == 'xbbg_cache').is_dir(),\n"
        "}))\n"
    )
    auto_run = subprocess.run(
        [sys.executable, "-c", auto_script],
        capture_output=True,
        text=True,
        env=_scrubbed_env(tmp_path / "env_auto", XBBG_SDK_SOURCE=str(wheel)),
        timeout=120,
        check=False,
    )
    assert auto_run.returncode == 0, auto_run.stdout + auto_run.stderr
    auto_payload = json.loads(auto_run.stdout.strip().splitlines()[-1])
    assert auto_payload["active"] == "xbbg_cache"
    assert auto_payload["runtime_version"]
    assert auto_payload["cache_sources"] == ["xbbg_cache"]
    assert auto_payload["install_dir_exists"] is True

    # 2. xbbg.set_sdk_source(...) works before loading _core.
    prog_script = (
        "import json, sys, xbbg\n"
        "xbbg.set_sdk_source(sys.argv[1])\n"
        "info = xbbg.get_sdk_info()\n"
        "print(json.dumps({'active': info['active'], 'platform': sys.argv[2]}))\n"
    )
    prog_run = subprocess.run(
        [sys.executable, "-c", prog_script, str(wheel), platform_name],
        capture_output=True,
        text=True,
        env=_scrubbed_env(tmp_path / "prog_auto"),
        timeout=120,
        check=False,
    )
    assert prog_run.returncode == 0, prog_run.stdout + prog_run.stderr
    prog_payload = json.loads(prog_run.stdout.strip().splitlines()[-1])
    assert prog_payload["active"] == "xbbg_cache"

    # 3. set_sdk_path stays strictly authoritative even when XBBG_SDK_SOURCE is set.
    bad_dir = tmp_path / "bad_sdk"
    bad_dir.mkdir()
    (bad_dir / lib_name).write_bytes(b"not-a-valid-library")
    explicit_script = (
        "import sys, xbbg\n"
        "try:\n"
        "    xbbg.set_sdk_path(sys.argv[1])\n"
        "except ValueError as err:\n"
        "    print('REFUSED:', err)\n"
        "else:\n"
        "    raise AssertionError('set_sdk_path unexpectedly fell back to provisioning')\n"
    )
    explicit_run = subprocess.run(
        [sys.executable, "-c", explicit_script, str(bad_dir)],
        capture_output=True,
        text=True,
        env=_scrubbed_env(tmp_path / "explicit_auth", XBBG_SDK_SOURCE=str(wheel)),
        timeout=120,
        check=False,
    )
    assert explicit_run.returncode == 0, explicit_run.stdout + explicit_run.stderr
    assert "REFUSED:" in explicit_run.stdout
