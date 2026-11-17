"""Tests for the release privacy check: its rules, log parsing and a scan of a real repository."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess

import pytest

from scripts.check_release_privacy import (
    COMMIT_START,
    FIELD_END,
    Commit,
    email_is_allowed,
    identity_pattern,
    main,
    parse_log,
    scan_commit,
    scan_text,
)

# Built by concatenation so this file does not trip the check it tests.
PERSONAL_EMAIL = "jane.doe" + "@" + "mail-provider.com"
HOME = "/Us" + "ers/janedoe/src/xbbg"
WINDOWS_HOME = "C:" + "\\Users\\janedoe\\xbbg"
PRIVATE_IPS = ["10." + "1.2.3", "172." + "20.0.4", "192." + "168.1.20", "100." + "64.0.9"]
AWS_KEY = "AK" + "IA" + "ABCDEFGHIJKLMNOP"
GITHUB_TOKEN = "gh" + "p_" + "a" * 36
PRIVATE_KEY = "-----BEGIN " + "RSA PRIVATE KEY-----"
NODE_ID = "[nid:" + "12345]"
USER_ID = "uu" + "id=" + "1234567"


def rules(text: str, **kwargs) -> set[str]:
    return {rule for rule, _ in scan_text(text, **kwargs)}


def test_flags_personal_email_but_not_addresses_that_identify_nobody() -> None:
    assert scan_text(f"contact {PERSONAL_EMAIL} for access") == [("email address", PERSONAL_EMAIL)]
    for address in (
        "noreply@xbbg.invalid",
        "12345+someone@users.noreply.github.com",
        "noreply@github.com",
        "git@github.com",
        "dev@example.com",
        "ops@team.example.org",
        "person@host.test",
    ):
        assert email_is_allowed(address), address


def test_flags_home_paths_but_not_placeholders() -> None:
    assert rules(f"BLPAPI_ROOT={HOME}/vendor") == {"home directory path"}
    assert rules(f"set PATH={WINDOWS_HOME}") == {"home directory path"}
    for text in ("/Users/runner/work/xbbg", "/home/user/.cache/xbbg", "C:\\Users\\analyst\\AppData", "/Users/you/bin"):
        assert scan_text(text) == [], text


def test_flags_private_addresses_but_not_versions_or_documentation_addresses() -> None:
    for address in PRIVATE_IPS:
        assert scan_text(f"server {address}.") == [("private IP address", address)]
    for text in ("blpapi 3.26.5.1", "ucrt >=10.0.20348.0", "192.0.2.10:8194", "127.0.0.1", "0.0.0.0", "1.2.3.4"):
        assert scan_text(text) == [], text


def test_flags_secrets_and_bloomberg_identifiers() -> None:
    assert rules(f"key = {AWS_KEY}") == {"AWS access key"}
    assert rules(f"token: {GITHUB_TOKEN}") == {"GitHub token"}
    assert rules(PRIVATE_KEY) == {"private key"}
    assert rules(f"Unknown/Invalid Security {NODE_ID}") == {"Bloomberg node id"}
    assert rules(f"auth {USER_ID}") == {"Bloomberg user id"}
    assert scan_text("Unknown/Invalid Security [nid:...]") == []


def test_identity_terms_match_whole_names_only() -> None:
    terms = [identity_pattern("exampleperson")]
    assert scan_text("reported by ExamplePerson", identities=terms) == [("local identity", "ExamplePerson")]
    assert scan_text("exampleperson2 and anexampleperson", identities=terms) == []


def test_allow_suppresses_deliberately_public_text() -> None:
    assert scan_text(f"security contact: {PERSONAL_EMAIL}", allow=[PERSONAL_EMAIL.upper()]) == []


def test_parse_log_tracks_paths_and_new_line_numbers() -> None:
    header = FIELD_END.join(["a" * 40, "A", "noreply@xbbg.invalid", "A", "noreply@xbbg.invalid", "subject\n\nbody\n"])
    output = (
        f"{COMMIT_START}{header}{FIELD_END}\n"
        "diff --git a/src/x.py b/src/x.py\n"
        "--- a/src/x.py\n"
        "+++ b/src/x.py\n"
        "@@ -3,0 +4,2 @@\n"
        "+first\n"
        "++++ content that starts with pluses\n"
        "@@ -10 +12 @@\n"
        "-old\n"
        "+second\n"
        "diff --git a/gone.txt b/gone.txt\n"
        "deleted file mode 100644\n"
        "--- a/gone.txt\n"
        "+++ /dev/null\n"
        "@@ -1 +0,0 @@\n"
        "-bye\n"
    )

    [commit] = parse_log(output)

    assert commit.message == "subject\n\nbody\n"
    assert commit.added == [
        ("src/x.py", 4, "first"),
        ("src/x.py", 5, "+++ content that starts with pluses"),
        ("src/x.py", 12, "second"),
    ]


def test_scan_commit_reports_identity_message_and_line_locations() -> None:
    commit = Commit(
        sha="b" * 40,
        author_name="Jane",
        author_email=PERSONAL_EMAIL,
        committer_name="Jane",
        committer_email=PERSONAL_EMAIL,
        message=f"fix: reconnect\n\nTested in {HOME}",
        added=[("a.py", 7, f"HOST = '{PRIVATE_IPS[0]}'")],
    )

    findings = scan_commit(commit)

    assert [(finding.location, finding.rule) for finding in findings] == [
        ("author", "email address"),
        ("message:3", "home directory path"),
        ("a.py:7", "private IP address"),
    ]


@pytest.fixture
def repository(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", os.devnull)
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    monkeypatch.chdir(tmp_path)

    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-c", "user.name=xbbg contributors", "-c", "user.email=noreply@xbbg.invalid", *args],
            check=True,
            capture_output=True,
        )

    git("init", "-q")
    (tmp_path / "clean.txt").write_text("servers: a.example.com:8194\n", encoding="utf-8")
    git("add", "clean.txt")
    git("commit", "-q", "-m", "feat: clean")
    git("tag", "v0.0.1")
    (tmp_path / "leak.txt").write_text(f"one\nBLPAPI_ROOT={HOME}\n", encoding="utf-8")
    git("add", "leak.txt")
    git("commit", "-q", "-m", "fix: leak")
    return tmp_path


def test_main_scans_the_range_since_the_latest_tag(repository: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--no-local-identity"]) == 1
    out = capsys.readouterr().out
    assert "leak.txt:2: home directory path" in out
    assert "clean.txt" not in out

    assert main(["--no-local-identity", "--range", "HEAD~1"]) == 0
    assert main(["--no-local-identity", "--allow", HOME]) == 0
    assert main(["--no-local-identity", "--range", "HEAD~1", "--term", "a.example.com"]) == 1
