"""Regression tests for installed release version agreement."""

from __future__ import annotations

from packaging.version import Version
import pytest

from scripts.check_release_versions import validate_versions


def test_rejects_stale_native_core_when_python_release_versions_match() -> None:
    with pytest.raises(ValueError, match=r"_core\.version\(\).*1\.4\.7"):
        validate_versions(
            "v1.4.12",
            distribution="1.4.12",
            package="1.4.12",
            binding="1.4.12",
            core="1.4.7",
        )


@pytest.mark.parametrize(
    ("component", "source"),
    [
        ("distribution", r"importlib\.metadata\.version"),
        ("package", r"xbbg\.__version__"),
        ("binding", r"_core\.__version__"),
    ],
)
def test_rejects_each_mixed_python_version(component: str, source: str) -> None:
    versions = dict.fromkeys(("distribution", "package", "binding", "core"), "1.4.12")
    versions[component] = "1.4.11"

    with pytest.raises(ValueError, match=source):
        validate_versions("1.4.12", **versions)


def test_accepts_matching_stable_release_with_tag_prefix() -> None:
    assert validate_versions(
        "v1.4.12",
        distribution="1.4.12",
        package="1.4.12",
        binding="1.4.12",
        core="1.4.12",
    ) == Version("1.4.12")


@pytest.mark.parametrize(
    ("expected", "python_version", "cargo_version"),
    [
        ("v1.5.0a1", "1.5.0a1", "1.5.0-alpha.1"),
        ("1.5.0-beta.3", "1.5.0b3", "1.5.0-beta.3"),
        ("v1.5.0rc2", "1.5.0rc2", "1.5.0-rc.2"),
    ],
)
def test_accepts_equivalent_python_and_cargo_prereleases(
    expected: str, python_version: str, cargo_version: str
) -> None:
    assert validate_versions(
        expected,
        distribution=python_version,
        package=python_version,
        binding=python_version,
        core=cargo_version,
    ) == Version(python_version)


def test_rejects_invalid_artifact_version_with_source_diagnostic() -> None:
    with pytest.raises(ValueError, match=r"_core\.version\(\).*not-a-version"):
        validate_versions(
            "1.4.12",
            distribution="1.4.12",
            package="1.4.12",
            binding="1.4.12",
            core="not-a-version",
        )


@pytest.mark.parametrize("version", ["unknown", "0+unknown", "1.4.13.dev1+gabc1234"])
def test_rejects_invalid_or_unresolved_release_even_when_all_versions_agree(version: str) -> None:
    with pytest.raises(ValueError, match="expected release"):
        validate_versions(version, distribution=version, package=version, binding=version, core=version)
