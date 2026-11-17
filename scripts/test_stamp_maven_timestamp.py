"""Tests for the checked Maven outputTimestamp stamp used by the Java release jobs."""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.stamp_maven_timestamp import main, stamp_output_timestamp

POM = """<project>
  <properties>
    <project.build.outputTimestamp>2026-11-03T00:00:00Z</project.build.outputTimestamp>
  </properties>
</project>
"""


def test_replaces_the_one_timestamp_element() -> None:
    stamped = stamp_output_timestamp(POM, "2026-11-17T12:00:00+00:00")
    assert "<project.build.outputTimestamp>2026-11-17T12:00:00+00:00</project.build.outputTimestamp>" in stamped
    assert "2026-11-03T00:00:00Z" not in stamped


@pytest.mark.parametrize(("pom", "count"), [("<project/>", 0), (POM.replace("</properties>", POM), 2)])
def test_rejects_a_pom_without_exactly_one_element(pom: str, count: int) -> None:
    with pytest.raises(ValueError, match=f"found {count}"):
        stamp_output_timestamp(pom, "2026-11-17T12:00:00Z")


@pytest.mark.parametrize("timestamp", ["", "yesterday", "2026-11-17T12:00:00"])
def test_rejects_timestamps_without_a_utc_offset(timestamp: str) -> None:
    with pytest.raises(ValueError, match="timestamp"):
        stamp_output_timestamp(POM, timestamp)


def test_main_stamps_the_file_and_fails_without_writing(tmp_path: Path) -> None:
    pom = tmp_path / "pom.xml"
    pom.write_text(POM, encoding="utf-8")
    assert main(["2026-11-17T12:00:00Z", "--pom", str(pom)]) == 0
    assert "2026-11-17T12:00:00Z" in pom.read_text(encoding="utf-8")

    empty = tmp_path / "empty.xml"
    empty.write_text("<project/>", encoding="utf-8")
    with pytest.raises(SystemExit) as exit_info:
        main(["2026-11-17T12:00:00Z", "--pom", str(empty)])
    assert exit_info.value.code == 1
    assert empty.read_text(encoding="utf-8") == "<project/>"
