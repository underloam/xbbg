use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex, Weak};

use napi::bindgen_prelude::{ClassInstance, Error, Status};
use napi_derive::napi;
use xbbg_async::testing::SubscriptionFixture;

use crate::{
    EngineConfig, JsSubscription, OverflowPolicy, SubscribeRequest, blp_async_error_to_napi, coded,
    subscription_delivery_from_input,
};

#[derive(Default)]
#[napi(object)]
pub struct SubscriptionFixtureOptions {
    pub delivery: Option<String>,
    pub overflow_policy: Option<String>,
    pub stream_capacity: Option<u32>,
    pub flush_threshold: Option<u32>,
    pub all_fields: Option<bool>,
    pub rows: Option<bool>,
    pub aliases: Option<HashMap<String, String>>,
    pub zero_as_null: Option<Vec<String>>,
}

pub(crate) struct SubscriptionPublicationPause {
    ready: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl SubscriptionPublicationPause {
    pub(crate) async fn hold(&self) {
        self.ready.notify_one();
        self.release.notified().await;
    }
}

#[napi]
pub struct JsSubscriptionPublicationPause {
    pause: Arc<SubscriptionPublicationPause>,
}

#[napi]
impl JsSubscriptionPublicationPause {
    #[napi]
    pub async fn ready(&self) {
        self.pause.ready.notified().await;
    }

    #[napi]
    pub fn release(&self) {
        self.pause.release.notify_one();
    }
}

#[napi]
pub struct JsSubscriptionFixture {
    fixture: SubscriptionFixture,
    batch_items: usize,
    pauses: Mutex<Vec<Weak<SubscriptionPublicationPause>>>,
}

/// Synthetic events enter the production shared feed and decoder. This factory
/// exists only in explicit test-support builds, never in shipped default addons.
#[napi]
pub async fn create_subscription_fixture(
    stream_capacity: Option<u32>,
    overflow_policy: Option<String>,
) -> napi::Result<JsSubscriptionFixture> {
    let mut config = EngineConfig::default();
    if let Some(capacity) = stream_capacity {
        if capacity == 0 {
            return Err(coded(
                Status::InvalidArg,
                "VALIDATION",
                "streamCapacity must be positive",
            ));
        }
        config.subscription_stream_capacity = capacity as usize;
    }
    if let Some(policy) = overflow_policy {
        config.overflow_policy = OverflowPolicy::from_str(&policy)
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
    }
    let batch_items = config.subscription_flush_threshold;
    Ok(JsSubscriptionFixture {
        fixture: SubscriptionFixture::new(config),
        batch_items,
        pauses: Mutex::new(Vec::new()),
    })
}

#[napi]
impl JsSubscriptionFixture {
    #[napi]
    pub async fn subscribe(
        &self,
        tickers: Vec<String>,
        fields: Vec<String>,
        options: Option<SubscriptionFixtureOptions>,
    ) -> napi::Result<JsSubscription> {
        let options = options.unwrap_or_default();
        if options.flush_threshold == Some(0) {
            return Err(coded(
                Status::InvalidArg,
                "VALIDATION",
                "flushThreshold must be positive",
            ));
        }
        let delivery = subscription_delivery_from_input(options.delivery.as_deref())?;
        let overflow_policy = options
            .overflow_policy
            .as_deref()
            .map(OverflowPolicy::from_str)
            .transpose()
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
        let batch_items = options
            .flush_threshold
            .map_or(self.batch_items, |limit| limit as usize);
        let stream = self
            .fixture
            .subscribe(SubscribeRequest {
                topics: tickers,
                fields,
                delivery,
                all_fields: options.all_fields.unwrap_or(false),
                deliver_rows: options.rows.unwrap_or(true),
                aliases: options.aliases.unwrap_or_default().into_iter().collect(),
                zero_as_null: options.zero_as_null.unwrap_or_default(),
                stream_capacity: options.stream_capacity.map(|capacity| capacity as usize),
                flush_threshold: options.flush_threshold.map(|limit| limit as usize),
                overflow_policy,
                ..SubscribeRequest::default()
            })
            .await
            .map_err(blp_async_error_to_napi)?;
        Ok(JsSubscription::from_stream(stream, batch_items))
    }

    #[napi]
    pub fn publish_json(&self, topic: String, json: String) -> napi::Result<()> {
        self.fixture
            .publish_json(&topic, &json)
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub fn data_loss(&self, topic: String) -> napi::Result<()> {
        self.fixture
            .data_loss(&topic)
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub fn pause_next_delivery(
        &self,
        subscription: ClassInstance<JsSubscription>,
    ) -> napi::Result<JsSubscriptionPublicationPause> {
        let pause = Arc::new(SubscriptionPublicationPause {
            ready: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = subscription
            .publication_pause
            .lock()
            .expect("subscription publication pause poisoned");
        if slot.is_some() {
            return Err(coded(
                Status::InvalidArg,
                "VALIDATION",
                "a delivery pause is already armed",
            ));
        }
        *slot = Some(Arc::clone(&pause));
        self.pauses
            .lock()
            .expect("fixture pauses poisoned")
            .push(Arc::downgrade(&pause));
        Ok(JsSubscriptionPublicationPause { pause })
    }

    #[napi]
    pub async fn settle(&self) {
        self.fixture.settle().await;
    }

    #[napi]
    pub fn terminate(&self) {
        for pause in self
            .pauses
            .lock()
            .expect("fixture pauses poisoned")
            .drain(..)
        {
            if let Some(pause) = pause.upgrade() {
                pause.release.notify_one();
            }
        }
        self.fixture.terminate();
    }
}
