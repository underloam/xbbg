//! Request and subscription state types with Arrow builders.

mod bql;
mod bsrch;
mod bulkdata;
mod channel;
mod fieldinfo;
mod generic;
mod histdata;
mod histdata_stream;
mod intradaybar;
mod intradaybar_stream;
mod intradaytick;
mod intradaytick_stream;
mod refdata;
mod subscription;
pub mod typed_builder;
mod update;
mod update_arrow;
mod value_utils;

pub use bql::BqlState;
pub use bsrch::BsrchState;
pub use bulkdata::BulkDataState;
pub(crate) use channel::SubscriptionTerminator;
pub use channel::{subscription_channel, SubscriptionReceiver, SubscriptionSender};
pub use fieldinfo::FieldInfoState;
pub use generic::GenericState;
pub use histdata::HistDataState;
pub use histdata_stream::HistDataStreamState;
pub use intradaybar::IntradayBarState;
pub use intradaybar_stream::IntradayBarStreamState;
pub use intradaytick::IntradayTickState;
pub use intradaytick_stream::IntradayTickStreamState;
pub use refdata::{LongMode, OutputFormat, RefDataState};
pub(crate) use subscription::{subscription_forwarder_channel, SubscriptionForwarder};
pub use subscription::{MessageOutcome, SubscriptionMetrics, SubscriptionState};
pub use update::{
    FieldIndex, FieldKind, FieldLayout, FieldMeta, SubscriptionUpdate, TopicId, UpdateField,
    UpdateValue,
};
pub use update_arrow::{subscription_update_to_record_batch, SubscriptionArrowBatcher};
pub(crate) use value_utils::ResponseMetadata;
pub use value_utils::{
    FieldExceptionMeta, SecurityErrorMeta, METADATA_KEY_EID_DATA, METADATA_KEY_FIELD_EXCEPTIONS,
    METADATA_KEY_SECURITY_ERRORS,
};
