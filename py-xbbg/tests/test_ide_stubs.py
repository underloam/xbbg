"""Generated stubs and IDE config edits must not depend on the default text encoding.

Python 3.15 made UTF-8 the default (PEP 686); Python 3.11-3.14 on Windows default to the
ANSI code page. Each test runs a child interpreter whose default is not UTF-8, which is what
Windows users of 3.11-3.14 have, and checks the files it writes as UTF-8.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import textwrap
import tomllib

import pytest

import xbbg

TEXT = "Z\u00fcrich \u2014 \u6771\u4eac"
# Quotes and backslashes must be escaped inside TOML and JSON strings.
STUBS_DIR = 'stubs-caf\u00e9 "q" \\ end'
DESCRIPTION = "Security\u2019s ticker, caf\u00e9"

CHILD = textwrap.dedent(
    f"""
    import locale
    from pathlib import Path
    import sys

    from xbbg import schema

    print(locale.getencoding())
    if sys.argv[1] == "configure":
        stubs = Path.cwd() / {STUBS_DIR!a}
        schema.configure_ide_stubs(stubs, ide=sys.argv[2])
    else:
        stubs = Path.cwd() / "stubs"
        ticker = schema.ElementInfo(
            name="security", description={DESCRIPTION!a}, data_type="String", type_name="",
            is_array=False, is_optional=False, enum_values=None, children=[],
        )
        request = schema.ElementInfo(
            name="Request", description={DESCRIPTION!a}, data_type="Sequence", type_name="",
            is_array=False, is_optional=False, enum_values=None, children=[ticker],
        )
        operation = schema.OperationSchema(name="Lookup", description="", request=request, responses=[])
        schema.get_schema = lambda service: schema.ServiceSchema(
            service=service, description="", operations=[operation], cached_at=""
        )
        schema.generate_stubs("//blp/demo", stubs)
    print(stubs)
    """
)


def _run_child(project: Path, *args: str) -> str:
    """Run ``CHILD`` in ``project`` with a non-UTF-8 default encoding; return the stubs path."""
    # A marker keeps project-root detection in ``project`` instead of a real parent directory.
    (project / ".git").mkdir(exist_ok=True)
    env = {
        **os.environ,
        "PYTHONPATH": str(Path(xbbg.__file__).resolve().parents[1]),
        # Windows: the ANSI code page. POSIX: the C locale's ASCII, without the coercion to
        # UTF-8 that Python otherwise applies to it.
        "PYTHONUTF8": "0",
        "PYTHONCOERCECLOCALE": "0",
        "LC_ALL": "C",
        # Only the child's stdout, so it can report non-ASCII paths.
        "PYTHONIOENCODING": "utf-8",
    }
    result = subprocess.run(
        [sys.executable, "-X", "utf8=0", "-c", CHILD, *args],
        cwd=project,
        capture_output=True,
        env=env,
        timeout=120,
        check=False,
    )
    stdout = result.stdout.decode("utf-8", "replace")
    assert result.returncode == 0, stdout + result.stderr.decode("utf-8", "replace")
    lines = stdout.splitlines()
    assert "utf" not in lines[0].lower(), f"child default encoding is {lines[0]}, so this test proves nothing"
    return lines[-1]


def test_vscode_settings_keep_existing_non_ascii_entries(tmp_path: Path) -> None:
    settings = tmp_path / ".vscode" / "settings.json"
    settings.parent.mkdir()
    settings.write_text(json.dumps({"files.exclude": {f"**/{TEXT}": True}}, ensure_ascii=False), encoding="utf-8")

    stubs = _run_child(tmp_path, "configure", "vscode")

    assert json.loads(settings.read_text(encoding="utf-8")) == {
        "files.exclude": {f"**/{TEXT}": True},
        "python.analysis.extraPaths": [stubs],
    }


@pytest.mark.parametrize(
    ("ide", "filename", "section", "keys"),
    [
        ("pyproject", "pyproject.toml", "[tool.pyright]", ("tool", "pyright", "extraPaths")),
        ("ty", "ty.toml", "[environment]", ("environment", "extra-paths")),
    ],
)
def test_toml_configs_stay_valid_utf8_toml(
    tmp_path: Path, ide: str, filename: str, section: str, keys: tuple[str, ...]
) -> None:
    config_file = tmp_path / filename
    config_file.write_text(f'title = "{TEXT}"\n\n{section}\n', encoding="utf-8")

    stubs = _run_child(tmp_path, "configure", ide)

    config = tomllib.loads(config_file.read_text(encoding="utf-8"))
    assert config["title"] == TEXT
    for key in keys:
        config = config[key]
    assert config == [stubs]


def test_generated_stub_modules_are_utf8_python(tmp_path: Path) -> None:
    stubs = Path(_run_child(tmp_path, "stubs"))

    for name in ("blp_demo.pyi", "blp_demo.py"):
        source = (stubs / name).read_bytes()
        compile(source, name, "exec")  # Python reads source files as UTF-8
        assert DESCRIPTION in source.decode("utf-8")
