//! Generate the native stub; run scripts/generate_package_stub.py for package exports.

use std::path::{Path, PathBuf};

use pyo3_stub_gen::Result;
const CORE_EXCEPTION_EXPORTS: &str = r#"    "BlpError",
    "BlpSessionError",
    "BlpRequestError",
    "BlpLimitError",
    "BlpSecurityError",
    "BlpFieldError",
    "BlpSubscriptionDataLossError",
    "BlpValidationError",
    "BlpTimeoutError",
    "BlpInternalError",
"#;

const CORE_EXCEPTION_STUBS: &str = r#"class BlpError(builtins.Exception): ...

class BlpSessionError(BlpError): ...

class BlpRequestError(BlpError): ...

class BlpLimitError(BlpRequestError): ...

class BlpSecurityError(BlpRequestError): ...

class BlpFieldError(BlpRequestError): ...

class BlpSubscriptionDataLossError(BlpRequestError):
    topic: builtins.str
    detail: builtins.str

class BlpValidationError(BlpError): ...

class BlpTimeoutError(BlpError): ...

class BlpInternalError(BlpError): ...

"#;

const CORE_PROVENANCE_EXPORTS: &str = r#"    "__version__",
    "__build_info__",
"#;

const CORE_PROVENANCE_STUBS: &str = r#"class _BuildInfo(typing.TypedDict):
    profile: builtins.str
    target: builtins.str
    rustFlags: builtins.list[builtins.str]
    rustcVersion: builtins.str
    gitCommit: builtins.str
    gitDescribe: builtins.str
    allocator: builtins.str
    optLevel: builtins.str
    targetFeatures: builtins.list[builtins.str]

__version__: builtins.str
__build_info__: _BuildInfo

"#;

fn main() -> Result<()> {
    let stub = _core::stub_info()?;
    stub.generate()?;
    let core_stub = move_core_stub()?;
    fix_generated_core_stub(&core_stub)?;
    Ok(())
}

fn package_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    manifest_dir
        .parent()
        .and_then(|bindings_dir| bindings_dir.parent())
        .expect("pyo3-xbbg must live under <workspace>/bindings/pyo3-xbbg")
        .join("py-xbbg/src/xbbg")
}

/// pyo3-stub-gen writes mixed-layout modules as `xbbg/_core/__init__.pyi`. A
/// `_core/` directory beside the `_core` extension makes `xbbg._core` import as
/// an empty namespace package whenever the binary is missing, so the stub is
/// shipped as `xbbg/_core.pyi` instead.
fn move_core_stub() -> Result<PathBuf> {
    let package = package_dir();
    let generated_dir = package.join("_core");
    let core_stub = package.join("_core.pyi");
    std::fs::rename(generated_dir.join("__init__.pyi"), &core_stub)?;
    std::fs::remove_dir(&generated_dir)?;
    Ok(core_stub)
}

fn fix_generated_core_stub(core_stub: &Path) -> Result<()> {
    let mut generated = std::fs::read_to_string(core_stub)?;
    // Git checkouts may use CRLF; generated stubs and insertion markers use LF.
    if generated.contains('\r') {
        generated.retain(|character| character != '\r');
    }

    let all_marker = "__all__ = [\n";
    let class_marker = "@typing.final\nclass ArrowColumn:";
    if !generated.contains(all_marker) || !generated.contains(class_marker) {
        return Err(std::io::Error::other(
            "generated core stub postprocessing markers were not found",
        )
        .into());
    }
    if !generated.contains("class BlpSubscriptionDataLossError") {
        let all_offset = generated.find(all_marker).expect("checked above") + all_marker.len();
        generated.insert_str(all_offset, CORE_EXCEPTION_EXPORTS);
        let class_offset = generated.find(class_marker).expect("checked above");
        generated.insert_str(class_offset, CORE_EXCEPTION_STUBS);
    }
    if !generated.contains("__build_info__: _BuildInfo") {
        let all_offset = generated.find(all_marker).expect("checked above") + all_marker.len();
        generated.insert_str(all_offset, CORE_PROVENANCE_EXPORTS);
        let class_offset = generated.find(class_marker).expect("checked above");
        generated.insert_str(class_offset, CORE_PROVENANCE_STUBS);
    }

    let untyped = "def check_entitlements(self, service: builtins.str, eids: typing.Sequence[builtins.int]) -> typing.Any:";
    let typed = "def check_entitlements(self, service: builtins.str, eids: typing.Sequence[builtins.int]) -> typing.Awaitable[EntitlementReport]:";
    if let Some(offset) = generated.find(untyped) {
        generated.replace_range(offset..offset + untyped.len(), typed);
    } else if !generated.contains(typed) {
        return Err(std::io::Error::other(
            "generated PyEngine.check_entitlements signature was not found",
        )
        .into());
    }
    std::fs::write(core_stub, generated)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adds_provenance_to_a_copy_of_core_stub() -> Result<()> {
        let copy = std::env::temp_dir().join(format!("xbbg-core-stub-{}.pyi", std::process::id()));
        std::fs::copy(package_dir().join("_core.pyi"), &copy)?;
        let mut source = std::fs::read_to_string(&copy)?;
        // Exercise insertion even after integration commits the generated provenance.
        if let Some(start) = source.find("class _BuildInfo(") {
            let declaration = "__build_info__: _BuildInfo";
            let end = source[start..]
                .find(declaration)
                .expect("provenance class must have its module attribute")
                + start
                + declaration.len();
            source.replace_range(start..end, "");
            for export in ["    \"__version__\",", "    \"__build_info__\","] {
                let offset = source.find(export).expect("provenance must be exported");
                source.replace_range(offset..offset + export.len(), "");
            }
        }
        assert!(!source.contains("__version__:"));
        assert!(!source.contains("__build_info__:"));
        std::fs::write(&copy, source)?;
        fix_generated_core_stub(&copy)?;
        let generated = std::fs::read_to_string(&copy)?;
        for declaration in [
            "__version__: builtins.str",
            "__build_info__: _BuildInfo",
            "class _BuildInfo(typing.TypedDict):",
            "    profile: builtins.str",
            "    target: builtins.str",
            "    rustFlags: builtins.list[builtins.str]",
            "    rustcVersion: builtins.str",
            "    gitCommit: builtins.str",
            "    gitDescribe: builtins.str",
            "    allocator: builtins.str",
            "    optLevel: builtins.str",
            "    targetFeatures: builtins.list[builtins.str]",
            "    \"__version__\",",
            "    \"__build_info__\",",
        ] {
            assert!(generated.contains(declaration), "missing {declaration}");
        }
        fix_generated_core_stub(&copy)?;
        assert_eq!(generated, std::fs::read_to_string(&copy)?);
        std::fs::remove_file(copy)?;
        Ok(())
    }
}
