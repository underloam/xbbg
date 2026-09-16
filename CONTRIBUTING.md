# Contributing to xbbg

Thank you for your interest in contributing to xbbg!

## Development Setup

### Prerequisites

- Python 3.10+
- Rust 1.75+
- [pixi](https://pixi.sh/) (recommended) or pip
- Bloomberg C++ SDK (for building the Rust backend)

Contributors must obtain and use the Bloomberg SDK under their own Bloomberg agreements. Repository helper scripts only assist local setup; they do not grant Bloomberg service access, SDK redistribution rights, data rights, credentials, or entitlements.

### Getting Started

1. Clone the repository:
   ```bash
   git clone https://github.com/underloam/xbbg.git
   cd xbbg
   ```

2. Set up a Bloomberg SDK you are authorized to use:
   ```bash
   # macOS/Linux
   bash ./scripts/sdktool.sh

   # Windows PowerShell
   # .\scripts\sdktool.ps1
   ```

3. Install dependencies and build:
   ```bash
   pixi install && pixi run install
   ```

4. Run tests:
   ```bash
   pixi run test
   ```

### Bloomberg SDK Compatibility

The minimum supported Bloomberg C SDK version is defined in
`defs/bloomberg.toml` (`min_sdk_version`). If you add a dependency on a new
`blpapi_*` function or type, verify it exists in the minimum version:

```bash
# Check against min SDK (reads defs/bloomberg.toml)
bash scripts/abi-check.sh

# Check against specific versions
bash scripts/abi-check.sh --versions "3.24.6.1 3.26.2.1"
```

CI runs this automatically as the **ABI Compat** job.

## Code Style

### Python
- We use [ruff](https://github.com/astral-sh/ruff) for linting and formatting
- Run `pixi run lint` before committing

### Rust
- Run `cargo fmt` for formatting
- Run `cargo clippy` for linting

## Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Make your changes
4. Ensure tests pass and code is formatted
5. Commit with a descriptive message following [Conventional Commits](https://www.conventionalcommits.org/):
   - `feat:` for new features
   - `fix:` for bug fixes
   - `docs:` for documentation changes
   - `refactor:` for code refactoring
   - `test:` for adding tests
   - `ci:` for CI/CD changes
6. Push and open a pull request

## Reporting Issues

Please use [GitHub Issues](https://github.com/underloam/xbbg/issues) to report bugs or request features.

When reporting a bug, include:
- Python version (`python --version`)
- xbbg version (`python -c "import xbbg; print(xbbg.__version__)"`)
- Operating system
- Minimal code to reproduce the issue

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
