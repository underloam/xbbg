//! Standalone CLI binary for provisioning the shared xbbg runtime cache and
//! staging the Bloomberg C++ SDK for local source builds (replacing `sdktool`).
//!
//! Because `xbbg-sdk` never depends on `xbbg-blpapi-sys`, this binary builds and
//! runs on a fresh checkout before any Bloomberg headers or runtimes exist:
//!
//! ```bash
//! cargo run -p xbbg-sdk --features provision -- cpp
//! ```

use std::io;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut stdout = io::stdout().lock();
    let mut stderr = io::stderr().lock();
    let code = xbbg_sdk::cli::run(
        "xbbg-sdk",
        &args,
        &|name| std::env::var(name).ok(),
        &mut stdout,
        &mut stderr,
    );
    std::process::exit(code);
}
