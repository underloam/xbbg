//! Explicit consumer delivery contracts; raw updates and current state are distinct.

use std::fmt;
use std::str::FromStr;

use arrow_array::RecordBatch;

use crate::BlpAsyncError;

/// Whether a subscription delivers sparse deltas or conflated current images.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SubscriptionDeliveryMode {
    #[default]
    Deltas,
    State,
}

impl SubscriptionDeliveryMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deltas => "deltas",
            Self::State => "state",
        }
    }
}

impl fmt::Display for SubscriptionDeliveryMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for SubscriptionDeliveryMode {
    type Err = BlpAsyncError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            value if value.eq_ignore_ascii_case("deltas") => Ok(Self::Deltas),
            value if value.eq_ignore_ascii_case("state") => Ok(Self::State),
            _ => Err(BlpAsyncError::ConfigError {
                detail: "delivery must be 'deltas' or 'state'".into(),
            }),
        }
    }
}

/// Updates intentionally evicted from one consumer's local delta queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionGap {
    pub dropped_updates: u64,
}

impl SubscriptionGap {
    pub const fn cause(self) -> &'static str {
        "local_overflow"
    }
}

/// One backend-independent batch plus continuity information that cannot be
/// discarded by a dataframe conversion.
#[derive(Debug)]
pub struct SubscriptionDelivery {
    pub kind: SubscriptionDeliveryMode,
    pub data: RecordBatch,
    pub gap_before: Option<SubscriptionGap>,
}
