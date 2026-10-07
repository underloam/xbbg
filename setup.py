"""Build hook that stamps the distribution version into the Rust extension.

Project metadata lives in pyproject.toml; setuptools-scm computes the version
when the build starts. ``build_rust`` exports that exact version to Cargo so
``xbbg._core.__version__`` names the distribution the binary was built for, and
the Python package refuses an extension that came from a different build.
"""

from __future__ import annotations

import os

from setuptools import setup
from setuptools_rust import build_rust


class StampedBuildRust(build_rust):
    """Compile ``xbbg._core`` with the version of the distribution being built."""

    def run(self) -> None:
        # Read by bindings/pyo3-xbbg/build.rs (rerun-if-env-changed), so a new
        # version always recompiles the extension. The PEP 517 hook process is
        # dedicated to this build, and setuptools-rust passes os.environ to Cargo.
        os.environ["XBBG_DIST_VERSION"] = self.distribution.get_version()
        super().run()


setup(cmdclass={"build_rust": StampedBuildRust})
