#!/usr/bin/env python3
"""Check a range of commits for personal data, secrets and machine details.

RELEASE_RULES.md requires this before pushing and again before every release::

    python scripts/check_release_privacy.py --range origin/main..HEAD   # before pushing
    python scripts/check_release_privacy.py                             # since the latest v* tag

It reads every commit's author and committer, its message and every line it
adds. Fixed rules catch personal email addresses, home-directory paths, private
IPv4 addresses, secrets and Bloomberg identifiers. Identity rules catch the
person running the check: login name, host name, the account's full name, and
the global git identity when this clone commits under a different one.

Each finding names the commit, where it was found and the rule; exit status is 1
when there is one. Fix findings before pushing (amend or rebase). Text that is
deliberately public, such as a published security contact, can be passed with
--allow; so can findings in commits that are already public, which a rebase
cannot remove.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Iterator, Sequence
from contextlib import suppress
from dataclasses import dataclass, field
import getpass
import ipaddress
import os
import re
import socket
import subprocess
import sys

# The top-level domain is letters, so `action@v1.2.3` and `crate@1.48.0` are not addresses.
EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,}(?![A-Za-z0-9-])")
# Domains reserved for examples or tests, and GitHub's no-reply addresses.
ALLOWED_EMAIL_DOMAINS = ("example.com", "example.org", "example.net", "users.noreply.github.com")
ALLOWED_EMAIL_SUFFIXES = (".example", ".invalid", ".test", ".localhost")
ALLOWED_EMAIL_LOCAL_PARTS = frozenset({"noreply", "no-reply", "donotreply"})
# SSH remotes and the address GitHub signs dependency-bot commits with.
ALLOWED_EMAILS = frozenset({"git@github.com", "support@github.com"})

HOME_PATH = re.compile(r"(?:/Users/|/home/|\b[A-Za-z]:(?:\\{1,2}|/)Users(?:\\{1,2}|/))([A-Za-z0-9._-]+)")
# Names used as placeholders in docs and by CI runners and containers.
PLACEHOLDER_USERS = frozenset(
    {
        "admin",
        "analyst",
        "bbg",
        "build",
        "builder",
        "default",
        "dev",
        "example",
        "me",
        "name",
        "public",
        "root",
        "runner",
        "runneradmin",
        "shared",
        "someone",
        "u",
        "ubuntu",
        "user",
        "username",
        "vscode",
        "you",
    }
)

IPV4 = re.compile(r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3})(?!\.?\d)")
# Non-global addresses point into someone's network. Documentation ranges are
# there to be published; loopback, link-local and reserved addresses name no host.
DOCUMENTATION_NETWORKS = tuple(
    ipaddress.ip_network(network) for network in ("192.0.2.0/24", "198.51.100.0/24", "203.0.113.0/24")
)

PATTERN_RULES = (
    ("private key", re.compile(r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----")),
    ("AWS access key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("GitHub token", re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,})\b")),
    ("Slack token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}")),
    ("API key", re.compile(r"\bsk-[A-Za-z0-9_-]{20,}")),
    ("Bloomberg node id", re.compile(r"\[nid:\s*\d+\]")),
    ("Bloomberg user id", re.compile(r"(?i)\b(?:uuid|emrs_?id)\b\W{0,3}\d{5,}")),
)

# Login names that identify a CI runner or container, not a person.
GENERIC_LOGINS = frozenset({"admin", "build", "builder", "ci", "dev", "docker", "root", "runner", "ubuntu", "user"})

COMMIT_START = "\x1e"
FIELD_END = "\x1f"
LOG_FORMAT = f"{COMMIT_START}%H{FIELD_END}%an{FIELD_END}%ae{FIELD_END}%cn{FIELD_END}%ce{FIELD_END}%B{FIELD_END}"
HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@")


@dataclass(frozen=True, slots=True)
class Finding:
    """One match: the commit, where in it, the rule and the matched text."""

    commit: str
    location: str
    rule: str
    text: str

    def __str__(self) -> str:
        return f"{self.commit[:12]} {self.location}: {self.rule}: {self.text}"


@dataclass(slots=True)
class Commit:
    """A commit's identities, message and added lines as (path, line number, text)."""

    sha: str
    author_name: str
    author_email: str
    committer_name: str
    committer_email: str
    message: str
    added: list[tuple[str, int, str]] = field(default_factory=list)


def identity_pattern(term: str) -> re.Pattern[str]:
    """Match `term` case-insensitively where it is not part of a longer name."""
    return re.compile(rf"(?<![A-Za-z0-9]){re.escape(term)}(?![A-Za-z0-9])", re.IGNORECASE)


def email_is_allowed(address: str) -> bool:
    """Whether an address identifies nobody: reserved, no-reply or GitHub's own."""
    address = address.lower()
    local, _, domain = address.rpartition("@")
    return (
        address in ALLOWED_EMAILS
        or local.split("+")[0] in ALLOWED_EMAIL_LOCAL_PARTS
        or domain in ALLOWED_EMAIL_DOMAINS
        or domain in {suffix.lstrip(".") for suffix in ALLOWED_EMAIL_SUFFIXES}
        or domain.endswith(ALLOWED_EMAIL_SUFFIXES)
        or any(domain.endswith(f".{allowed}") for allowed in ALLOWED_EMAIL_DOMAINS)
    )


def _is_private_ipv4(text: str) -> bool:
    """Whether `text` is an address inside a network: private, shared or similar."""
    try:
        address = ipaddress.IPv4Address(text)
    except ipaddress.AddressValueError:
        return False
    if (
        address.is_global
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.packed[0] == 0
    ):
        return False
    return not any(address in network for network in DOCUMENTATION_NETWORKS)


def scan_text(
    text: str,
    *,
    identities: Sequence[re.Pattern[str]] = (),
    allow: Sequence[str] = (),
) -> list[tuple[str, str]]:
    """Return (rule, matched text) for everything in `text` the rules flag."""
    found: list[tuple[str, str]] = []
    for match in EMAIL.finditer(text):
        if not email_is_allowed(match.group()):
            found.append(("email address", match.group()))
    for match in HOME_PATH.finditer(text):
        if match.group(1).lower() not in PLACEHOLDER_USERS:
            found.append(("home directory path", match.group()))
    for match in IPV4.finditer(text):
        if _is_private_ipv4(match.group(1)):
            found.append(("private IP address", match.group(1)))
    for rule, pattern in PATTERN_RULES:
        found.extend((rule, match.group()) for match in pattern.finditer(text))
    for pattern in identities:
        found.extend(("local identity", match.group()) for match in pattern.finditer(text))
    allowed = [value.lower() for value in allow]
    unique: list[tuple[str, str]] = []
    for rule, value in found:
        lowered = value.lower()
        if any(entry in lowered or lowered in entry for entry in allowed) or (rule, value) in unique:
            continue
        unique.append((rule, value))
    return unique


def parse_log(output: str) -> Iterator[Commit]:
    """Parse `git log -p --unified=0` output written with LOG_FORMAT."""
    for chunk in output.split(COMMIT_START)[1:]:
        sha, author_name, author_email, committer_name, committer_email, message, patch = chunk.split(FIELD_END, 6)
        commit = Commit(sha, author_name, author_email, committer_name, committer_email, message)
        path = ""
        line_number = 0
        in_header = False
        for line in patch.splitlines():
            if line.startswith("diff --git "):
                in_header = True
                path = ""
            elif in_header:
                if line.startswith("+++ "):
                    target = line[4:]
                    path = target[2:] if target.startswith("b/") else target
                elif hunk := HUNK.match(line):
                    in_header = False
                    line_number = int(hunk.group(1))
            elif hunk := HUNK.match(line):
                line_number = int(hunk.group(1))
            elif line.startswith("+") and path and path != "/dev/null":
                commit.added.append((path, line_number, line[1:]))
                line_number += 1
        yield commit


def scan_commit(
    commit: Commit,
    *,
    identities: Sequence[re.Pattern[str]] = (),
    allow: Sequence[str] = (),
) -> list[Finding]:
    """Scan a commit's identities, message and added lines."""
    findings: list[Finding] = []

    def check(location: str, text: str) -> None:
        for rule, value in scan_text(text, identities=identities, allow=allow):
            findings.append(Finding(commit.sha, location, rule, value))

    check("author", f"{commit.author_name} <{commit.author_email}>")
    if (commit.committer_name, commit.committer_email) != (commit.author_name, commit.author_email):
        check("committer", f"{commit.committer_name} <{commit.committer_email}>")
    for offset, line in enumerate(commit.message.splitlines(), start=1):
        check(f"message:{offset}", line)
    for path, line_number, text in commit.added:
        check(f"{path}:{line_number}", text)
    return findings


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True, encoding="utf-8", errors="replace"
    ).stdout


def _git_config(*args: str) -> str:
    try:
        return _git("config", *args).strip()
    except subprocess.CalledProcessError:
        return ""


def local_identity_terms() -> set[str]:
    """Names that identify the person running the check on this machine."""
    terms: set[str] = set()
    with suppress(Exception):
        terms.add(getpass.getuser())
    with suppress(OSError):
        terms.add(socket.gethostname().split(".")[0])
    with suppress(ImportError, KeyError, AttributeError):
        import pwd

        terms.add(pwd.getpwuid(os.getuid()).pw_gecos.split(",")[0])
    # A clone that commits under another identity must not leak the global one.
    for key in ("user.name", "user.email"):
        global_value = _git_config("--global", key)
        if global_value and global_value != _git_config(key):
            terms.add(global_value)
    return {term.strip() for term in terms if len(term.strip()) >= 3 and term.strip().lower() not in GENERIC_LOGINS}


def default_range() -> str:
    """Commits since the latest version tag reachable from HEAD."""
    tag = _git("describe", "--tags", "--abbrev=0", "--match", "v[0-9]*", "HEAD").strip()
    return f"{tag}..HEAD"


def check_range(
    rev_range: str,
    *,
    terms: Iterable[str] = (),
    allow: Sequence[str] = (),
) -> list[Finding]:
    """Scan every commit in `rev_range`."""
    output = _git("log", "--no-color", "--no-ext-diff", "-p", "--unified=0", f"--format={LOG_FORMAT}", rev_range)
    identities = [identity_pattern(term) for term in sorted(set(terms))]
    findings: list[Finding] = []
    for commit in parse_log(output):
        findings.extend(scan_commit(commit, identities=identities, allow=allow))
    return findings


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--range", dest="rev_range", help="commits to scan (default: latest v* tag..HEAD)")
    parser.add_argument("--term", action="append", default=[], help="extra text that must not appear (repeatable)")
    parser.add_argument(
        "--allow",
        action="append",
        default=[],
        help="text that may appear; a finding is skipped when its text contains this or is part of it (repeatable)",
    )
    parser.add_argument(
        "--no-local-identity",
        action="store_true",
        help="skip the login, host, full-name and global git identity checks (CI)",
    )
    args = parser.parse_args(argv)

    rev_range = args.rev_range or default_range()
    terms = set(args.term)
    if not args.no_local_identity:
        terms |= local_identity_terms()
    findings = check_range(rev_range, terms=terms, allow=args.allow)
    for finding in findings:
        print(finding)
    commits = len({finding.commit for finding in findings})
    if findings:
        print(f"{len(findings)} finding(s) in {commits} commit(s) of {rev_range}", file=sys.stderr)
        return 1
    print(f"No findings in {rev_range}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
