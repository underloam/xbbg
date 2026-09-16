# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| 1.0.x   | Yes       |
| 0.x     | Security fixes only |

## Reporting a Vulnerability

If you discover a security vulnerability in xbbg, please report it responsibly:

1. **Do not** open a public GitHub issue for security vulnerabilities.
2. Use GitHub's private vulnerability reporting:
   [Report a vulnerability](https://github.com/underloam/xbbg/security/advisories/new).
   This opens a private advisory visible only to the maintainers. Include:
   - A description of the vulnerability
   - Steps to reproduce
   - Potential impact
3. You should receive an acknowledgment within 48 hours.

We will work with you to understand and address the issue before any public disclosure.

## Scope

xbbg is a client library that connects to Bloomberg services. Security concerns may include:

- Credential or session token exposure in logs or error messages
- Unsafe handling of Bloomberg SDK pointers in the Rust FFI layer
- Dependency vulnerabilities in Rust (`cargo-audit` / `cargo-deny`) or Python supply chain
- Injection via user-supplied Bloomberg field names or overrides passed to the SDK

## Hardening

- Rust FFI bindings are checked with `cargo-deny` for license and advisory compliance.
- The CI pipeline runs `cargo audit` on every push.
- Bloomberg SDK credentials are never logged; request middleware scrubs auth fields from `RequestContext`.
