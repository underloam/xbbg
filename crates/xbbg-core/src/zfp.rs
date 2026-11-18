use crate::errors::{BlpError, Result};

use crate::options::SessionOptions;
use crate::sdk;
use crate::tls::TlsOptions;

const REMOTE_8194: i32 = 8194;
const REMOTE_8196: i32 = 8196;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ZfpRemote {
    Remote8194,
    Remote8196,
}

impl ZfpRemote {
    fn to_raw(self) -> i32 {
        match self {
            Self::Remote8194 => REMOTE_8194,
            Self::Remote8196 => REMOTE_8196,
        }
    }
}

impl std::str::FromStr for ZfpRemote {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "8194" => Ok(Self::Remote8194),
            "8196" => Ok(Self::Remote8196),
            _ => Err(format!(
                "invalid ZFP remote port: {s} (expected \"8194\" or \"8196\")"
            )),
        }
    }
}

impl std::fmt::Display for ZfpRemote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Remote8194 => f.write_str("8194"),
            Self::Remote8196 => f.write_str("8196"),
        }
    }
}

pub fn configure_zfp_options(
    options: &mut SessionOptions,
    tls: &TlsOptions,
    remote: ZfpRemote,
) -> Result<()> {
    let rc = unsafe {
        sdk().blpapi_ZfpUtil_getOptionsForLeasedLines(
            options.as_raw(),
            tls.as_ptr(),
            remote.to_raw(),
        )
    };
    if rc != 0 {
        return Err(BlpError::Internal {
            detail: format!("ZfpUtil::getOptionsForLeasedLines failed: rc={rc}"),
        });
    }
    Ok(())
}
