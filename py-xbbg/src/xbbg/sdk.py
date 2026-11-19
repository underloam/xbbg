"""CLI for provisioning and inspecting the shared xbbg Bloomberg SDK cache.

Invoke as ``xbbg-sdk`` or ``python -m xbbg.sdk``. Works before any Bloomberg
runtime is installed on the machine.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import sys

from . import _core_guard


def run_cli(
    argv: Sequence[str] | None = None,
    *,
    prog: str = "xbbg-sdk",
    env: Mapping[str, str] | None = None,
) -> tuple[int, str, str]:
    """Run the native SDK cache CLI and return ``(exit_code, stdout, stderr)``."""
    args = list(sys.argv[1:] if argv is None else argv)
    env_map = dict(env) if env is not None else None
    core = _core_guard.import_without_runtime()
    try:
        return core.run_sdk_cli(prog, args, env_map)
    finally:
        if core.loaded_sdk() is None:
            _core_guard.forget_core(core)


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``xbbg-sdk`` and ``python -m xbbg.sdk``."""
    code, stdout, stderr = run_cli(argv, prog="xbbg-sdk")
    if stdout:
        sys.stdout.write(stdout)
    if stderr:
        sys.stderr.write(stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
