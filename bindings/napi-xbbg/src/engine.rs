//! N-API `JsEngine` core methods and Arrow IPC/zero-copy helpers.

use std::io::Cursor;
use std::sync::Arc;

use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use napi::bindgen_prelude::{Buffer, Error, Status};
use napi_derive::napi;
use xbbg_async::engine::{Engine, EngineConfig, RequestParams, ServerAddr, Transport};
use xbbg_host::errors::{HostError, parse_args};
use xbbg_host::preflight::{prepare_auction_subscribe, prepare_etf_inav_subscribe};
use xbbg_host::schema::SchemaJsonCache;
use xbbg_host::subscribe::{PreparedSubscribe, prepare_subscribe};

use crate::arrow_zero_copy::{NativeArrowBatch, NativeArrowIpcFile};
use crate::config::{
    EngineConfigInput, EntitlementReport, FeedInfoOutput, FieldInfoOutput, RequestInput,
    SessionConnectionsOutput, StringPair,
};
use crate::errors::{blp_async_error_to_napi, coded, host_error_to_napi};
use crate::request::pairs_to_map;
use crate::subscriptions::JsSubscription;

pub(crate) fn to_ipc_buffer(batch: RecordBatch) -> napi::Result<Buffer> {
    let schema = batch.schema();
    let mut cursor = Cursor::new(Vec::<u8>::new());

    {
        let mut writer = StreamWriter::try_new(&mut cursor, &schema).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Arrow IPC writer init failed: {e}"),
            )
        })?;

        writer.write(&batch).map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Arrow IPC write failed: {e}"),
            )
        })?;

        writer.finish().map_err(|e| {
            Error::new(
                Status::GenericFailure,
                format!("Arrow IPC finalize failed: {e}"),
            )
        })?;
    }

    Ok(Buffer::from(cursor.into_inner()))
}

pub(crate) fn to_native_record_batch(batch: RecordBatch) -> napi::Result<NativeArrowBatch> {
    NativeArrowBatch::from_record_batch(batch)
}

/// Parse the JSON arguments of `subscribe` and run the host's validation and
/// request assembly. `engine_batch_items` is the engine's default read batch size.
pub(crate) fn prepare_subscribe_args(
    args: &str,
    engine_batch_items: usize,
) -> Result<PreparedSubscribe, HostError> {
    prepare_subscribe(parse_args(args)?, engine_batch_items)
}

#[napi]
pub struct JsEngine {
    pub(crate) engine: Arc<Engine>,
    schema_json_cache: SchemaJsonCache,
    subscription_batch_items: usize,
}

#[napi]
impl JsEngine {
    #[napi(constructor)]
    pub fn new(host: Option<String>, port: Option<u16>) -> napi::Result<Self> {
        let host = host.unwrap_or_else(|| "localhost".to_string());
        let port = port.unwrap_or(8194);
        let config = EngineConfig {
            transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
            ..Default::default()
        };
        Self::start_engine(config)
    }

    #[napi(factory)]
    pub fn with_config(config: EngineConfigInput) -> napi::Result<Self> {
        Self::start_engine(config.try_into()?)
    }

    /// Name of the result column holding each row's security.
    #[napi(getter)]
    pub fn key_column(&self) -> &'static str {
        self.engine.key_column().as_str()
    }

    /// Connect asynchronously with host/port defaults.
    ///
    /// Engine startup (Bloomberg session connect plus service warmup —
    /// seconds, up to the 30s session timeout) runs on the blocking pool
    /// instead of the JS thread. Prefer this over `new JsEngine(...)`, which
    /// freezes the Node event loop for the duration of the connect.
    #[napi(factory)]
    pub async fn connect(host: Option<String>, port: Option<u16>) -> napi::Result<Self> {
        let host = host.unwrap_or_else(|| "localhost".to_string());
        let port = port.unwrap_or(8194);
        let config = EngineConfig {
            transport: Transport::Direct(vec![ServerAddr::new(host, port)]),
            ..Default::default()
        };
        Self::start_engine_async(config).await
    }

    /// Connect asynchronously from a full configuration.
    /// See [`JsEngine::connect`].
    #[napi(factory)]
    pub async fn connect_with_config(config: EngineConfigInput) -> napi::Result<Self> {
        Self::start_engine_async(config.try_into()?).await
    }

    #[napi]
    pub async fn request(&self, params: RequestInput) -> napi::Result<NativeArrowBatch> {
        let rust_params: RequestParams = params.try_into()?;
        let batch = self
            .engine
            .request(rust_params)
            .await
            .map_err(blp_async_error_to_napi)?;
        to_native_record_batch(batch)
    }

    #[napi]
    pub async fn request_raw(&self, params: RequestInput) -> napi::Result<Buffer> {
        let rust_params: RequestParams = params.try_into()?;
        let batch = self
            .engine
            .request(rust_params)
            .await
            .map_err(blp_async_error_to_napi)?;
        to_ipc_buffer(batch)
    }

    /// Run a request and encode the result once, off the JS thread, as an Arrow
    /// IPC file with its schema metadata. nodejs-polars `readIPC` reads only the
    /// file format; `request_raw` keeps returning the stream format.
    #[napi]
    pub async fn request_ipc_file(&self, params: RequestInput) -> napi::Result<NativeArrowIpcFile> {
        let rust_params: RequestParams = params.try_into()?;
        let batch = self
            .engine
            .request(rust_params)
            .await
            .map_err(blp_async_error_to_napi)?;
        NativeArrowIpcFile::from_record_batch(&batch)
    }

    /// Return the Bloomberg identity seat type: "BPS", "NONBPS", or "INVALID".
    #[napi]
    pub async fn seat_type(&self) -> napi::Result<String> {
        self.engine
            .seat_type()
            .await
            .map(|seat| seat.as_str().to_string())
            .map_err(blp_async_error_to_napi)
    }

    /// Check whether the authorized identity is entitled to all supplied EIDs.
    #[napi]
    pub async fn check_entitlements(
        &self,
        service: String,
        eids: Vec<i32>,
    ) -> napi::Result<EntitlementReport> {
        self.engine
            .check_entitlements(&service, &eids)
            .await
            .map(|report| EntitlementReport {
                entitled: report.entitled,
                failed_eids: report.failed_eids,
            })
            .map_err(blp_async_error_to_napi)
    }

    /// Return whether the authorized identity may use the Bloomberg service.
    #[napi]
    pub async fn identity_is_authorized(&self, service: String) -> napi::Result<bool> {
        self.engine
            .identity_is_authorized(&service)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub async fn resolve_field_types(
        &self,
        fields: Vec<String>,
        overrides: Option<Vec<StringPair>>,
        default_type: Option<String>,
    ) -> napi::Result<Vec<StringPair>> {
        let map = self
            .engine
            .resolve_field_types(
                &fields,
                pairs_to_map(overrides).as_ref(),
                default_type.as_deref().unwrap_or("string"),
            )
            .await
            .map_err(blp_async_error_to_napi)?;

        Ok(map
            .into_iter()
            .map(|(key, value)| StringPair { key, value })
            .collect())
    }

    #[napi]
    pub fn get_field_info(&self, field: String) -> Option<FieldInfoOutput> {
        self.engine
            .get_field_info(&field)
            .map(|info| FieldInfoOutput {
                field_id: info.field_id,
                arrow_type: info.arrow_type,
                description: info.description,
                category: info.category,
            })
    }

    #[napi]
    pub fn clear_field_cache(&self) -> napi::Result<()> {
        self.engine
            .clear_field_cache()
            .map_err(|error| coded(Status::GenericFailure, "INTERNAL", error))
    }

    #[napi]
    pub fn save_field_cache(&self) -> napi::Result<()> {
        self.engine
            .save_field_cache()
            .map_err(|e| Error::new(Status::GenericFailure, e))
    }

    #[napi]
    pub async fn validate_fields(&self, fields: Vec<String>) -> napi::Result<Vec<String>> {
        self.engine
            .validate_fields(&fields)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub fn is_field_validation_enabled(&self) -> bool {
        self.engine.is_field_validation_enabled()
    }

    #[napi]
    pub async fn get_schema(&self, service: String) -> napi::Result<String> {
        self.schema_json_cache
            .schema(&self.engine, &service)
            .await
            .map(|json| json.to_string())
            .map_err(host_error_to_napi)
    }

    #[napi]
    pub async fn get_operation(&self, service: String, operation: String) -> napi::Result<String> {
        self.schema_json_cache
            .operation(&self.engine, &service, &operation)
            .await
            .map(|json| json.to_string())
            .map_err(host_error_to_napi)
    }

    #[napi]
    pub async fn list_operations(&self, service: String) -> napi::Result<Vec<String>> {
        self.engine
            .list_operations(&service)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub fn get_cached_schema(&self, service: String) -> Option<String> {
        self.schema_json_cache
            .cached_schema(&self.engine, &service)
            .map(|json| json.to_string())
    }

    #[napi]
    pub fn invalidate_schema(&self, service: String) -> napi::Result<()> {
        self.schema_json_cache
            .invalidate(&self.engine, &service)
            .map_err(host_error_to_napi)
    }

    #[napi]
    pub fn clear_schema_cache(&self) -> napi::Result<()> {
        self.schema_json_cache
            .clear(&self.engine)
            .map_err(host_error_to_napi)
    }

    #[napi]
    pub fn list_cached_schemas(&self) -> Vec<String> {
        self.engine.list_cached_schemas()
    }

    #[napi]
    pub async fn get_enum_values(
        &self,
        service: String,
        operation: String,
        element: String,
    ) -> napi::Result<Option<Vec<String>>> {
        self.engine
            .get_enum_values(&service, &operation, &element)
            .await
            .map_err(blp_async_error_to_napi)
    }

    #[napi]
    pub async fn list_valid_elements(
        &self,
        service: String,
        operation: String,
    ) -> napi::Result<Option<Vec<String>>> {
        self.engine
            .list_valid_elements(&service, &operation)
            .await
            .map_err(blp_async_error_to_napi)
    }

    /// Validate, prepare and open one subscription (host `prepare_subscribe`).
    #[napi]
    pub async fn subscribe(&self, args: String) -> napi::Result<JsSubscription> {
        let prepared = prepare_subscribe_args(&args, self.subscription_batch_items)
            .map_err(host_error_to_napi)?;
        self.subscribe_prepared(prepared).await
    }

    /// Resolve auction venues and open one venue subscription (host preflight).
    #[napi]
    pub async fn subscribe_auction(&self, args: String) -> napi::Result<JsSubscription> {
        let args = parse_args(&args).map_err(host_error_to_napi)?;
        let prepared = prepare_auction_subscribe(&self.engine, args, self.subscription_batch_items)
            .await
            .map_err(host_error_to_napi)?;
        self.subscribe_prepared(prepared).await
    }

    /// Resolve each ETF's iNAV ticker and open one iNAV subscription (host preflight).
    #[napi]
    pub async fn subscribe_etf_inav(&self, args: String) -> napi::Result<JsSubscription> {
        let args = parse_args(&args).map_err(host_error_to_napi)?;
        let prepared =
            prepare_etf_inav_subscribe(&self.engine, args, self.subscription_batch_items)
                .await
                .map_err(host_error_to_napi)?;
        self.subscribe_prepared(prepared).await
    }

    /// Current shared-feed diagnostics, without identity or transport configuration.
    #[napi]
    pub fn subscription_feeds(&self) -> Vec<FeedInfoOutput> {
        self.engine
            .subscription_feeds()
            .into_iter()
            .map(Into::into)
            .collect()
    }

    /// Servers each Bloomberg session is connected to: one entry per session,
    /// request workers first, then subscription workers. No credentials.
    #[napi]
    pub fn connections(&self) -> Vec<SessionConnectionsOutput> {
        self.engine
            .connections()
            .into_iter()
            .map(Into::into)
            .collect()
    }

    #[napi]
    pub fn signal_shutdown(&self) {
        self.engine.signal_shutdown();
    }

    /// Whether at least one request worker session is healthy.
    #[napi]
    pub fn is_available(&self) -> bool {
        self.engine
            .request_pool_health()
            .iter()
            .any(|(_, health)| *health == xbbg_async::engine::WorkerHealth::Healthy)
    }

    async fn subscribe_prepared(
        &self,
        prepared: PreparedSubscribe,
    ) -> napi::Result<JsSubscription> {
        let stream = self
            .engine
            .subscribe(prepared.request)
            .await
            .map_err(blp_async_error_to_napi)?;
        Ok(JsSubscription::from_stream(stream, prepared.batch_items))
    }

    fn start_engine(config: EngineConfig) -> napi::Result<Self> {
        let subscription_batch_items = config.subscription_flush_threshold;
        let engine = Engine::start(config).map_err(blp_async_error_to_napi)?;
        Ok(Self {
            engine: Arc::new(engine),
            schema_json_cache: SchemaJsonCache::default(),
            subscription_batch_items,
        })
    }

    async fn start_engine_async(config: EngineConfig) -> napi::Result<Self> {
        let subscription_batch_items = config.subscription_flush_threshold;
        let engine =
            napi::tokio::task::spawn_blocking(move || Engine::start(config).map_err(Box::new))
                .await
                .map_err(|join_error| {
                    coded(
                        Status::GenericFailure,
                        "INTERNAL",
                        format!("engine startup task failed: {join_error}"),
                    )
                })?
                .map_err(|error| blp_async_error_to_napi(*error))?;
        Ok(Self {
            engine: Arc::new(engine),
            schema_json_cache: SchemaJsonCache::default(),
            subscription_batch_items,
        })
    }
}
