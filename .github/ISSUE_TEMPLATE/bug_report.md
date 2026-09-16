---
name: Bug Report
about: Create a report to help us improve xbbg
title: '[BUG] '
labels: bug
assignees: ''
---

## Bug Description

A clear and concise description of what the bug is.

## To Reproduce

Steps to reproduce the behavior:

1. Import statement: `...`
2. Function call: `...`
3. See error: `...`

## Expected Behavior

A clear and concise description of what you expected to happen.

## Actual Behavior

A clear and concise description of what actually happened.

## Code Example

```python
# Please provide a minimal reproducible example
import xbbg
# Your code here
```

## Error Message / Traceback

```text
Paste the full error message or traceback here
```

## Environment

- **xbbg version**: [e.g., 1.0.0]
- **Python version**: [e.g., 3.11.5]
- **Operating System**: [e.g., Windows 11, Ubuntu 22.04, macOS 14]
- **Bloomberg API package/version**: [e.g., blpapi 3.26.1.1]
- **Bloomberg connection/API mode**:
  - [ ] DAPI / Desktop API / Terminal
  - [ ] ZFP
  - [ ] SAPI
  - [ ] B-PIPE
  - [ ] Other: [describe]
- **xbbg backend requested**: [e.g., default `narwhals`, `native`, `pyarrow`, `pandas`, `polars`, `duckdb`]
- **Optional conversion backend versions**: [if used, e.g., pandas 2.2.3, polars 1.35.0, duckdb 1.2.0, narwhals 2.0.0]

Please redact credentials, tokens, account numbers, proprietary Bloomberg data, portfolio identifiers, and screenshots containing licensed data before posting.

## Additional Context

Add any other context about the problem here, such as:

- Bloomberg Terminal version, if using DAPI/Desktop API
- Bloomberg service/operation or Terminal function, if known [e.g., `//blp/refdata`, `ReferenceDataRequest`, PORT]
- Whether the same security/field/request works in Bloomberg Terminal, Excel, or raw `blpapi`
- Network/auth details that are safe to share [e.g., ZFP, SAPI app auth, B-PIPE TLS; do not include credentials, tokens, private keys, portfolio IDs, or licensed data]
- Any workarounds you've tried

## Checklist

- [ ] I have checked the [documentation](https://xbbg.org/) for similar issues
- [ ] I have searched existing [issues](https://github.com/underloam/xbbg/issues) to see if this has been reported
- [ ] I can provide a minimal reproducible example
