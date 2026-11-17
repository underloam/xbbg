# Release Rules

Read these before every release, and before pushing commits that will ship in one. [RELEASE_PROCESS.md](RELEASE_PROCESS.md) makes this Step 0. The rules apply to everything a release publishes:
- commits in the release range: identities, messages and content;
- tags;
- `CHANGELOG.md` and the GitHub release notes;
- package metadata and docs;
- tests, fixtures and examples;
- benchmark output and provenance;
- issue and pull-request text written for the release.

## 1. Mark breaking changes, fixes included

Mark a CHANGELOG entry `**BREAKING (<area>)**` when it changes any of the following, whether the change is a feature or a fix:

- **Arrow types.** The Arrow type of any output column or field. A fix that makes a field's type correct (for example timestamp → string) changes a caller's schema; once results are stored, a type change is a migration.
- **`xbbg.*` metadata.** The name, presence or encoding of any `xbbg.*` schema-metadata key, or the shape a public diagnostics accessor returns.
- **Result shape.** Columns added, removed, renamed or reordered, the meaning of a row, or the key column.
- **Returns and raises.** What a call returns or raises for the same input: an exception that becomes a result or the reverse, a different exception class, a different error `service`/`operation`.
- **Public Rust items.** Public Rust structs that users build with literals (a new field), and enums that are not `#[non_exhaustive]` (a new variant).
- **Defaults.** A default that changes behaviour (configuration, formats, delivery or overflow policies).

Name the area (`Rust`, `output shape`, `raw metadata`, `subscriptions`, `errors`, `defaults`), say what changed, and say what callers must do. `Fixed` entries carry the marker too when they qualify; do not leave a breaking fix unmarked because it is a fix.

## 2. No personal data, credentials or market data

Nothing the project publishes may identify a person, a machine, an account or a network, or carry data from a Bloomberg session.

- **Commit identity.** Commit to this repository as `xbbg contributors <noreply@xbbg.invalid>` or with a GitHub no-reply address. Set it per clone with `git config user.name` and `git config user.email`. Never commit with a personal email address.
  - Commit dates record your local UTC offset. Commit with `TZ=UTC` set if the offset would say where you are.
- **Machines and networks.** No absolute local paths (`/Users/<name>`, `/home/<name>`, `C:\Users\<name>`), login or host names, or internal IP addresses.
  - Use `localhost`, the documentation ranges `192.0.2.0/24`, `198.51.100.0/24` and `203.0.113.0/24`, and `example.com` names (`a.example.com:8194`).
  - Placeholders already in use (`/Users/runner`, `/home/user`, `C:\Users\analyst`) are fine.
- **Bloomberg identities and entitlements.** No B-PIPE or Server API host names, application names in use, user IDs, UUIDs, EMRS IDs, SIDs, EIDs from a real account, authorization IP addresses, TLS material or tokens.
  - When quoting a Bloomberg message, cut identifiers the way issue reports do: `[nid:...]`, `req-...`.
- **Market data.** No values, prices, screenshots or terminal output captured from Bloomberg or any other licensed source, in tests, fixtures, docs, benchmarks, issues or release notes.
  - Security identifiers and field mnemonics are public and may appear.
  - Values are always synthetic.
  - Benchmarks record shapes and timings, never data.
- **Secrets.** Never commit credentials, tokens or keys. If one is committed, revoke and rotate it: rewriting history does not unpublish it.
- **Reports from users.** Reproductions and logs copied into tests, commits or notes keep only what the fix needs, with hosts, identifiers and values replaced by placeholders.

## 3. Checks

1. **Before pushing.** Scan the commits you are about to push and fix every finding by amending or rebasing:

   ```bash
   python scripts/check_release_privacy.py --range origin/main..HEAD
   ```

2. **Before triggering a release.** Scan the release range (default: the latest `v*` tag to `HEAD`):

   ```bash
   python scripts/check_release_privacy.py
   ```

   - Findings in commits that are already on `main` cannot be removed by a rebase. Decide each one:
     - if the text is deliberately public (for example a published security contact), rerun with `--allow <text>`;
     - if it is a secret, revoke it;
     - otherwise remove it from the files in a follow-up commit.
   - Note the decision in the release pull request or run.

3. **Read the `[Unreleased]` notes against rule 1.** Check every entry that mentions a type, a column, metadata, an error or a default for a missing `BREAKING` marker.

The scanner checks:
- author and committer identities;
- commit messages and every added line;
- fixed patterns: email addresses, home-directory paths, internal IPv4 addresses, secrets, Bloomberg node and user IDs;
- the identity of whoever runs it: login and host name, the account's full name, and the global git identity when the clone commits under another one.

Pass `--term <text>` for anything else that must not appear, such as server names. A clean scan does not prove compliance: it does not recognise market data or every kind of identifier, so rule 2 still needs a read of the diff.
