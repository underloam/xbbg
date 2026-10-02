use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Implementation, ServerCapabilities, ServerInfo};
use rmcp::{tool, tool_handler, tool_router, ErrorData, ServerHandler, ServiceExt};
use serde_json::{json, Value};
use tokio::sync::{OnceCell, Semaphore};
use xbbg_async::engine::{Engine, EngineConfig, RequestParams, RetryPolicy, ServerAddr, Transport};
use xbbg_async::BlpAsyncError;
use xbbg_core::errors::ValidationError;
use xbbg_core::{AuthConfig, BlpError};
use xbbg_recipes::{recipe_auction_snapshot, recipe_resolve_venues, RecipeError};

mod request_adapter;
mod serialization;
mod stdin;

use request_adapter::{
    auction_snapshot_params, bdh_request_params, bdib_request_params, bdp_request_params,
    bds_request_params, bflds_request_params, bql_request_params, bsrch_request_params,
    check_entitlements_params, generic_request_params, resolve_venues_params, AuctionSnapshotArgs,
    BdhArgs, BdibArgs, BdpArgs, BdsArgs, BfldsArgs, BqlArgs, BsrchArgs, CheckEntitlementsArgs,
    RequestArgs, ResolveVenuesArgs,
};

use serialization::{
    bounded_error_display, bounded_error_text, bounded_json_text, entitlement_check_to_json,
    json_serialized_len, record_batch_to_json, should_offload, should_offload_items, ResultLimits,
    MIN_RESULT_BYTES,
};

struct XbbgMcpServer {
    #[allow(dead_code)] // read by rmcp's generated ServerHandler impl
    tool_router: ToolRouter<Self>,
    engine: OnceCell<Arc<Engine>>,
    engine_config: EngineConfig,
    result_limits: ResultLimits,
    serialization_permits: Arc<Semaphore>,
}

impl XbbgMcpServer {
    fn new_from_env() -> Result<Self, String> {
        const MAX_CONCURRENT_SERIALIZATIONS: usize = 2;

        let (engine_config, result_limits) = load_settings_from_env()?;
        Ok(Self {
            tool_router: Self::tool_router(),
            engine: OnceCell::new(),
            engine_config,
            result_limits,
            serialization_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_SERIALIZATIONS)),
        })
    }

    fn bounded_input<T>(&self, result: Result<T, ErrorData>) -> Result<T, ErrorData> {
        result.map_err(|error| bound_existing_error(error, &self.result_limits))
    }

    async fn engine(&self) -> Result<&Arc<Engine>, ErrorData> {
        self.engine
            .get_or_try_init(|| async {
                let config = self.engine_config.clone();
                let limits = self.result_limits.clone();
                tokio::task::spawn_blocking(move || {
                    Engine::start(config)
                        .map(Arc::new)
                        .map_err(|error| map_request_error(error, &limits))
                })
                .await
                .map_err(|error| bounded_internal_error(&error, &self.result_limits))?
            })
            .await
    }

    async fn run_blocking_serialization<T, F>(&self, operation: F) -> Result<T, ErrorData>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ErrorData> + Send + 'static,
    {
        let permit = self
            .serialization_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                ErrorData::internal_error(
                    "MCP result serialization is shutting down".to_string(),
                    None,
                )
            })?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await
        .map_err(|error| bounded_internal_error(&error, &self.result_limits))?
    }

    async fn execute_request(&self, params: RequestParams) -> Result<CallToolResult, ErrorData> {
        let batch = self
            .engine()
            .await?
            .request(params)
            .await
            .map_err(|error| map_request_error(error, &self.result_limits))?;
        self.serialize_batch(batch).await
    }

    async fn serialize_batch(&self, batch: RecordBatch) -> Result<CallToolResult, ErrorData> {
        let payload = if should_offload(&batch, &self.result_limits) {
            let limits = self.result_limits.clone();
            self.run_blocking_serialization(move || record_batch_to_json(&batch, &limits))
                .await?
        } else {
            record_batch_to_json(&batch, &self.result_limits)?
        };
        Ok(CallToolResult::structured(payload))
    }
}

#[tool_router]
impl XbbgMcpServer {
    #[tool(
        description = "Bloomberg reference data request (bdp). Returns structured JSON with schema metadata and bounded rows.",
        annotations(
            title = "Bloomberg Reference Data",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bdp(
        &self,
        Parameters(args): Parameters<BdpArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(self.bounded_input(bdp_request_params(args))?)
            .await
    }

    #[tool(
        description = "Bloomberg historical data request (bdh). Dates must be YYYYMMDD or YYYY-MM-DD.",
        annotations(
            title = "Bloomberg Historical Data",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bdh(
        &self,
        Parameters(args): Parameters<BdhArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(self.bounded_input(bdh_request_params(args))?)
            .await
    }

    #[tool(
        description = "Bloomberg bulk data request (bds). Uses the bulk extractor and requires exactly one bulk field.",
        annotations(
            title = "Bloomberg Bulk Data",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bds(
        &self,
        Parameters(args): Parameters<BdsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(self.bounded_input(bds_request_params(args))?)
            .await
    }

    #[tool(
        description = "Bloomberg intraday bar request (bdib). Datetimes must be ISO-8601 strings and interval must be positive.",
        annotations(
            title = "Bloomberg Intraday Bars",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bdib(
        &self,
        Parameters(args): Parameters<BdibArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(self.bounded_input(bdib_request_params(args))?)
            .await
    }

    #[tool(
        description = "Bloomberg Query Language request (bql).",
        annotations(
            title = "Bloomberg Query Language",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bql(
        &self,
        Parameters(args): Parameters<BqlArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(self.bounded_input(bql_request_params(args))?)
            .await
    }

    #[tool(
        description = "Bloomberg search request (bsrch). The domain selects the saved Bloomberg search, extra parameters are passed through as named request elements.",
        annotations(
            title = "Bloomberg Search",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bsrch(
        &self,
        Parameters(args): Parameters<BsrchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(self.bounded_input(bsrch_request_params(args))?)
            .await
    }

    #[tool(
        description = "Bloomberg field metadata lookup (bflds). Supply either concrete field ids or a search_spec, but not both.",
        annotations(
            title = "Bloomberg Field Metadata",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn bflds(
        &self,
        Parameters(args): Parameters<BfldsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(self.bounded_input(bflds_request_params(args))?)
            .await
    }

    #[tool(
        description = "Route composite equity tickers/ISINs to the primary exchange listing and preferreds to venue pricing sources. Returns one row per input in input order, including duplicates, with status/error and venue metadata in bounded structured JSON with Arrow schema metadata.",
        annotations(
            title = "Resolve Bloomberg Auction Venues",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn resolve_venues(
        &self,
        Parameters(args): Parameters<ResolveVenuesArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let params = self.bounded_input(resolve_venues_params(args))?;
        let batch = recipe_resolve_venues(
            self.engine().await?,
            params.securities,
            params.pcs_overrides,
        )
        .await
        .map_err(|error| map_recipe_error(error, &self.result_limits))?;
        self.serialize_batch(batch).await
    }

    #[tool(
        description = "Read auction/imbalance values from Bloomberg reference data on the primary venue. Uses the default auction field group unless fields or groups are selected; groups expand first, then explicit fields, with duplicates removed. Returns one row per input with status/error and bounded structured JSON with Arrow schema metadata. Time-of-day fields are terminal-local. Reference data carries no delayed/real-time flag; use subscriptions for that.",
        annotations(
            title = "Bloomberg Auction Snapshot",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn auction_snapshot(
        &self,
        Parameters(args): Parameters<AuctionSnapshotArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let params = self.bounded_input(auction_snapshot_params(args))?;
        let batch = recipe_auction_snapshot(
            self.engine().await?,
            params.securities,
            params.fields,
            params.pcs_overrides,
        )
        .await
        .map_err(|error| map_recipe_error(error, &self.result_limits))?;
        self.serialize_batch(batch).await
    }

    #[tool(
        description = "Check a nonempty list of Bloomberg entitlement IDs against a service. The service defaults to //blp/refdata.",
        annotations(
            title = "Check Bloomberg Entitlements",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn check_entitlements(
        &self,
        Parameters(args): Parameters<CheckEntitlementsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let (service, eids) = self.bounded_input(check_entitlements_params(args))?;
        let check = self
            .engine()
            .await?
            .check_entitlements(&service, &eids)
            .await
            .map_err(|error| map_request_error(error, &self.result_limits))?;
        let work_items = eids.len().saturating_add(check.failed_eids.len());
        let payload = if should_offload_items(work_items) {
            let limits = self.result_limits.clone();
            self.run_blocking_serialization(move || {
                entitlement_check_to_json(service, eids, check, &limits)
            })
            .await?
        } else {
            entitlement_check_to_json(service, eids, check, &self.result_limits)?
        };
        Ok(CallToolResult::structured(payload))
    }

    #[tool(
        description = "Generic Bloomberg request. Supports raw/custom service and operation strings, including RawRequest via request_operation. return_eids supports ReferenceDataRequest (including BDS/bulk), HistoricalDataRequest, IntradayBarRequest, and IntradayTickRequest.",
        annotations(
            title = "Bloomberg Raw Request",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn request(
        &self,
        Parameters(args): Parameters<RequestArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.execute_request(self.bounded_input(generic_request_params(args))?)
            .await
    }
}

fn server_version() -> &'static str {
    option_env!("XBBG_MCP_VERSION").unwrap_or(env!("CARGO_PKG_VERSION"))
}

#[tool_handler]
impl ServerHandler for XbbgMcpServer {
    fn get_info(&self) -> ServerInfo {
        // The server only advertises tools for now; request execution stays lazy so stdio startup
        // does not require a live Bloomberg session before the client can initialize.
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new(env!("CARGO_PKG_NAME"), server_version())
                    .with_title("xbbg MCP")
                    .with_description(
                        "Request/response Bloomberg tools backed directly by xbbg-async. Current env configuration supports single-host connectivity, core auth modes, and request-pool tuning.",
                    ),
            )
            .with_instructions(
                "Use bdp, bdh, bds, bdib, bql, bsrch, bflds, resolve_venues, auction_snapshot, check_entitlements, or request. resolve_venues routes composite equities to primary exchange listings and preferreds to venue pricing sources, preserving one status/error row per input. auction_snapshot reads Bloomberg reference data on the primary venue; time-of-day fields are terminal-local, and reference data carries no delayed/real-time flag (use subscriptions for that). Results are structured JSON with Arrow schema and metadata, explicit truncation counts, and bounded rows, cells, strings, metadata, and total bytes. This MCP server currently exposes host/port, selected auth env vars, core pool settings, and result limits rather than the full EngineConfig surface. For XBBG_MCP_VALIDATION_MODE/XBBG_VALIDATION_MODE use disabled (default), lenient, or strict; for XBBG_MCP_SDK_LOG_LEVEL/XBBG_SDK_LOG_LEVEL use off (default), fatal, error, warn, info, debug, or trace; for XBBG_MCP_OVERFLOW_POLICY/XBBG_OVERFLOW_POLICY use drop_newest (default) or block.",
            )
    }
}

fn load_settings_from_env() -> Result<(EngineConfig, ResultLimits), String> {
    // Keep the initial env surface intentionally narrow: expose the subset we can document and
    // support honestly for MCP deployment, rather than implying full parity with every EngineConfig knob.
    let mut config = EngineConfig::default();

    let host = env_string(&[
        "XBBG_MCP_HOST",
        "XBBG_MCP_SERVER_HOST",
        "XBBG_HOST",
        "XBBG_SERVER_HOST",
        "XBBG_SERVER",
    ])
    .unwrap_or_else(|| "localhost".to_string());
    let port = env_u16(&[
        "XBBG_MCP_PORT",
        "XBBG_MCP_SERVER_PORT",
        "XBBG_PORT",
        "XBBG_SERVER_PORT",
    ])?
    .unwrap_or(8194);
    config.transport = Transport::Direct(vec![ServerAddr::new(host, port)]);
    config.request_pool_size =
        env_usize(&["XBBG_MCP_REQUEST_POOL_SIZE", "XBBG_REQUEST_POOL_SIZE"])?
            .unwrap_or(config.request_pool_size);
    config.validation_mode = env_parsed(
        &["XBBG_MCP_VALIDATION_MODE", "XBBG_VALIDATION_MODE"],
        "validation_mode",
    )?
    .unwrap_or(config.validation_mode);
    config.field_cache_path = env_string(&["XBBG_MCP_FIELD_CACHE_PATH", "XBBG_FIELD_CACHE_PATH"])
        .map(PathBuf::from)
        .or_else(|| config.field_cache_path.clone());
    if let Some(warmup_services) = env_csv(&["XBBG_MCP_WARMUP_SERVICES", "XBBG_WARMUP_SERVICES"]) {
        config.warmup_services = warmup_services;
    }
    config.sdk_log_level = env_parsed(
        &["XBBG_MCP_SDK_LOG_LEVEL", "XBBG_SDK_LOG_LEVEL"],
        "sdk_log_level",
    )?
    .unwrap_or(config.sdk_log_level);
    config.num_start_attempts = env_usize(&[
        "XBBG_MCP_NUM_START_ATTEMPTS",
        "XBBG_NUM_START_ATTEMPTS",
        "XBBG_MAX_ATTEMPT",
    ])?
    .unwrap_or(config.num_start_attempts);
    config.auto_restart_on_disconnection = env_bool(&[
        "XBBG_MCP_AUTO_RESTART_ON_DISCONNECTION",
        "XBBG_AUTO_RESTART_ON_DISCONNECTION",
        "XBBG_AUTO_RESTART",
    ])?
    .unwrap_or(config.auto_restart_on_disconnection);
    config.retry_policy = RetryPolicy {
        max_retries: env_u32(&["XBBG_MCP_RETRY_MAX_RETRIES", "XBBG_RETRY_MAX_RETRIES"])?
            .unwrap_or(config.retry_policy.max_retries),
        initial_delay_ms: env_u64(&[
            "XBBG_MCP_RETRY_INITIAL_DELAY_MS",
            "XBBG_RETRY_INITIAL_DELAY_MS",
        ])?
        .unwrap_or(config.retry_policy.initial_delay_ms),
        backoff_factor: env_f64(&["XBBG_MCP_RETRY_BACKOFF_FACTOR", "XBBG_RETRY_BACKOFF_FACTOR"])?
            .unwrap_or(config.retry_policy.backoff_factor),
        max_delay_ms: env_u64(&["XBBG_MCP_RETRY_MAX_DELAY_MS", "XBBG_RETRY_MAX_DELAY_MS"])?
            .unwrap_or(config.retry_policy.max_delay_ms),
    };
    config.overflow_policy = env_parsed(
        &["XBBG_MCP_OVERFLOW_POLICY", "XBBG_OVERFLOW_POLICY"],
        "overflow_policy",
    )?
    .unwrap_or(config.overflow_policy);
    config.auth = build_auth_from_env()?;

    let defaults = ResultLimits::default();
    let result_limits = ResultLimits {
        max_rows: env_limit(&["XBBG_MCP_MAX_ROWS"], "max_rows", defaults.max_rows, 1)?,
        max_cells: env_limit(&["XBBG_MCP_MAX_CELLS"], "max_cells", defaults.max_cells, 1)?,
        max_metadata_properties: env_limit(
            &["XBBG_MCP_MAX_METADATA_PROPERTIES"],
            "max_metadata_properties",
            defaults.max_metadata_properties,
            1,
        )?,
        max_metadata_bytes: env_limit(
            &["XBBG_MCP_MAX_METADATA_BYTES"],
            "max_metadata_bytes",
            defaults.max_metadata_bytes,
            1,
        )?,
        max_string_chars: env_limit(
            &["XBBG_MCP_MAX_STRING_CHARS"],
            "max_string_chars",
            defaults.max_string_chars,
            1,
        )?,
        max_string_bytes: env_limit(
            &["XBBG_MCP_MAX_STRING_BYTES"],
            "max_string_bytes",
            defaults.max_string_bytes,
            '…'.len_utf8(),
        )?,
        max_result_bytes: env_limit(
            &["XBBG_MCP_MAX_RESULT_BYTES"],
            "max_result_bytes",
            defaults.max_result_bytes,
            MIN_RESULT_BYTES,
        )?,
    };

    Ok((config, result_limits))
}

fn build_auth_from_env() -> Result<Option<AuthConfig>, String> {
    let auth_method = env_string(&["XBBG_MCP_AUTH_METHOD", "XBBG_AUTH_METHOD"]);
    let app_name = env_string(&["XBBG_MCP_APP_NAME", "XBBG_APP_NAME"]);
    let dir_property = env_string(&["XBBG_MCP_DIR_PROPERTY", "XBBG_DIR_PROPERTY"]);
    let user_id = env_string(&["XBBG_MCP_USER_ID", "XBBG_USER_ID"]);
    let ip_address = env_string(&["XBBG_MCP_IP_ADDRESS", "XBBG_IP_ADDRESS"]);
    let token = env_string(&["XBBG_MCP_TOKEN", "XBBG_TOKEN"]);

    let Some(method) = auth_method.map(|value| value.to_ascii_lowercase()) else {
        if app_name.is_some()
            || dir_property.is_some()
            || user_id.is_some()
            || ip_address.is_some()
            || token.is_some()
        {
            return Err(
                "auth_method is required when auth-specific environment variables are set"
                    .to_string(),
            );
        }
        return Ok(None);
    };

    let auth = match method.as_str() {
        "" | "none" => None,
        "user" => Some(AuthConfig::User),
        "app" => Some(AuthConfig::App {
            app_name: required_env_value(&app_name, "app_name", &method)?,
        }),
        "userapp" => Some(AuthConfig::UserApp {
            app_name: required_env_value(&app_name, "app_name", &method)?,
        }),
        "dir" | "directory" => Some(AuthConfig::Directory {
            property_name: required_env_value(&dir_property, "dir_property", &method)?,
        }),
        "manual" => Some(AuthConfig::Manual {
            app_name: required_env_value(&app_name, "app_name", &method)?,
            user_id: required_env_value(&user_id, "user_id", &method)?,
            ip_address: required_env_value(&ip_address, "ip_address", &method)?,
        }),
        "token" => Some(AuthConfig::Token {
            token: required_env_value(&token, "token", &method)?,
        }),
        other => {
            return Err(format!(
                "invalid auth_method '{other}' (expected none, user, app, userapp, dir, directory, manual, or token)"
            ));
        }
    };

    Ok(auth)
}

fn required_env_value(value: &Option<String>, field: &str, method: &str) -> Result<String, String> {
    value
        .clone()
        .ok_or_else(|| format!("{field} is required for auth_method={method}"))
}

fn env_string(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        env::var(key)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    })
}

fn env_csv(keys: &[&str]) -> Option<Vec<String>> {
    keys.iter().find_map(|key| {
        env::var(key).ok().map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
    })
}

fn env_usize(keys: &[&str]) -> Result<Option<usize>, String> {
    env_parse(keys, "usize", str::parse)
}

fn env_limit(keys: &[&str], label: &str, default: usize, minimum: usize) -> Result<usize, String> {
    let value = env_usize(keys)?.unwrap_or(default);
    if value < minimum {
        return Err(format!("{label} must be at least {minimum}, got {value}"));
    }
    Ok(value)
}

fn env_u16(keys: &[&str]) -> Result<Option<u16>, String> {
    env_parse(keys, "u16", str::parse)
}

fn env_u32(keys: &[&str]) -> Result<Option<u32>, String> {
    env_parse(keys, "u32", str::parse)
}

fn env_u64(keys: &[&str]) -> Result<Option<u64>, String> {
    env_parse(keys, "u64", str::parse)
}

fn env_f64(keys: &[&str]) -> Result<Option<f64>, String> {
    env_parse(keys, "f64", str::parse)
}

fn env_bool(keys: &[&str]) -> Result<Option<bool>, String> {
    env_parse(keys, "bool", parse_bool)
}

fn env_parsed<T>(keys: &[&str], label: &str) -> Result<Option<T>, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    env_parse(keys, label, str::parse)
}

fn env_parse<T, E, F>(keys: &[&str], label: &str, parser: F) -> Result<Option<T>, String>
where
    F: Fn(&str) -> Result<T, E>,
    E: std::fmt::Display,
{
    for key in keys {
        if let Ok(raw) = env::var(key) {
            let value = raw.trim();
            if value.is_empty() {
                continue;
            }
            return parser(value)
                .map(Some)
                .map_err(|err| format!("invalid {label} in {key}: {err}"));
        }
    }
    Ok(None)
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("expected true/false style boolean, got '{value}'")),
    }
}

fn truncation_data(truncated: bool) -> Option<Value> {
    truncated.then(|| json!({"message_truncated": true}))
}

fn bound_existing_error(error: ErrorData, limits: &ResultLimits) -> ErrorData {
    let ErrorData {
        code,
        message,
        data,
    } = error;
    let upstream_message_truncated = data
        .as_ref()
        .and_then(|value| value.get("message_truncated"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let marker_only = matches!(
        data.as_ref(),
        Some(Value::Object(object))
            if object.len() == 1 && object.contains_key("message_truncated")
    );
    let (message, locally_truncated) = bounded_error_text(&message, limits);
    let message_truncated = locally_truncated || upstream_message_truncated;
    let data_omitted = data.is_some() && !marker_only;
    let truncation = (message_truncated || data_omitted).then(|| {
        json!({
            "message_truncated": message_truncated,
            "error_data_omitted": data_omitted,
        })
    });
    ErrorData::new(code, message, truncation)
}

fn bounded_invalid_params(message: &str, limits: &ResultLimits) -> ErrorData {
    let (message, truncated) = bounded_error_text(message, limits);
    ErrorData::invalid_params(message, truncation_data(truncated))
}

fn bounded_internal_error(error: &impl std::fmt::Display, limits: &ResultLimits) -> ErrorData {
    let (message, truncated) = bounded_error_display(error, limits);
    ErrorData::internal_error(message, truncation_data(truncated))
}

fn map_recipe_error(error: RecipeError, limits: &ResultLimits) -> ErrorData {
    match error {
        RecipeError::Engine(error) => map_request_error(*error, limits),
        RecipeError::InvalidArgument(detail) => bounded_invalid_params(&detail, limits),
        other @ (RecipeError::Ext(_) | RecipeError::Arrow(_) | RecipeError::Other(_)) => {
            bounded_internal_error(&other, limits)
        }
    }
}

fn map_request_error(error: BlpAsyncError, limits: &ResultLimits) -> ErrorData {
    match error {
        BlpAsyncError::ConfigError { detail } => bounded_invalid_params(&detail, limits),
        BlpAsyncError::Blp(blp_error) | BlpAsyncError::BlpError(blp_error) => {
            map_blp_error(blp_error, limits)
        }
        other => bounded_internal_error(&other, limits),
    }
}

fn map_blp_error(error: BlpError, limits: &ResultLimits) -> ErrorData {
    match error {
        BlpError::InvalidArgument { detail } => bounded_invalid_params(&detail, limits),
        BlpError::SchemaOperationNotFound { service, operation } => {
            let (service, service_truncated) = bounded_error_text(&service, limits);
            let (operation, operation_truncated) = bounded_error_text(&operation, limits);
            let (message, message_truncated) = bounded_error_display(
                &format_args!("unknown Bloomberg operation '{operation}' for service '{service}'"),
                limits,
            );
            ErrorData::invalid_params(
                message,
                truncation_data(service_truncated || operation_truncated || message_truncated),
            )
        }
        BlpError::SchemaElementNotFound { parent, name } => {
            let (parent, parent_truncated) = bounded_error_text(&parent, limits);
            let (name, name_truncated) = bounded_error_text(&name, limits);
            let (message, message_truncated) = bounded_error_display(
                &format_args!("unknown Bloomberg request element '{name}' under '{parent}'"),
                limits,
            );
            ErrorData::invalid_params(
                message,
                truncation_data(parent_truncated || name_truncated || message_truncated),
            )
        }
        BlpError::SchemaTypeMismatch {
            element,
            expected,
            found,
        } => {
            let (element, element_truncated) = bounded_error_text(&element, limits);
            let (expected, expected_truncated) = bounded_error_text(&expected, limits);
            let (found, found_truncated) = bounded_error_text(&found, limits);
            let (message, message_truncated) = bounded_error_display(
                &format_args!("type mismatch at '{element}': expected {expected}, found {found}"),
                limits,
            );
            ErrorData::invalid_params(
                message,
                truncation_data(
                    element_truncated || expected_truncated || found_truncated || message_truncated,
                ),
            )
        }
        BlpError::SchemaUnsupported { element, detail } => {
            let (element, element_truncated) = bounded_error_text(&element, limits);
            let (detail, detail_truncated) = bounded_error_text(&detail, limits);
            let (message, message_truncated) = bounded_error_display(
                &format_args!("unsupported schema construct at '{element}': {detail}"),
                limits,
            );
            ErrorData::invalid_params(
                message,
                truncation_data(element_truncated || detail_truncated || message_truncated),
            )
        }
        BlpError::Validation { message, errors } => {
            const ERROR_DATA_RESERVE: usize = 768;
            const PRIMARY_ERROR_RESERVE: usize = 768;

            let total_errors = errors.len();
            let available_bytes = limits.max_result_bytes.saturating_sub(ERROR_DATA_RESERVE);
            let primary_reserve = if total_errors > 0 && limits.max_cells > 0 {
                available_bytes.min(PRIMARY_ERROR_RESERVE)
            } else {
                0
            };
            let message_budget = available_bytes.saturating_sub(primary_reserve);
            let (message, message_truncated) = bounded_json_text(&message, limits, message_budget);
            let message_bytes = json_serialized_len(&message).unwrap_or(available_bytes);
            let mut remaining_bytes = available_bytes.saturating_sub(message_bytes);
            let mut returned_errors = Vec::new();
            let mut diagnostics_truncated = false;
            let mut errors = errors.into_iter().take(limits.max_cells);

            if let Some(primary) = errors.next() {
                let entry_budget = remaining_bytes.saturating_sub(1);
                if let Some((entry, truncated)) =
                    bounded_validation_entry(primary, limits, entry_budget)
                {
                    let entry_bytes = json_serialized_len(&entry)
                        .unwrap_or(remaining_bytes)
                        .saturating_add(1);
                    remaining_bytes = remaining_bytes.saturating_sub(entry_bytes);
                    diagnostics_truncated |= truncated;
                    returned_errors.push(entry);
                } else {
                    diagnostics_truncated = true;
                    remaining_bytes = 0;
                }
            }

            if remaining_bytes > 0 {
                for error in errors {
                    let entry_budget = remaining_bytes.saturating_sub(1);
                    let Some((entry, truncated)) =
                        bounded_validation_entry(error, limits, entry_budget)
                    else {
                        diagnostics_truncated = true;
                        break;
                    };
                    let entry_bytes = json_serialized_len(&entry)
                        .unwrap_or(remaining_bytes)
                        .saturating_add(1);
                    remaining_bytes = remaining_bytes.saturating_sub(entry_bytes);
                    diagnostics_truncated |= truncated;
                    returned_errors.push(entry);
                }
            }

            let returned_error_count = returned_errors.len();
            ErrorData::invalid_params(
                message,
                Some(json!({
                    "total_errors": total_errors,
                    "returned_errors": returned_error_count,
                    "omitted_errors": total_errors.saturating_sub(returned_error_count),
                    "message_truncated": message_truncated,
                    "truncated": message_truncated
                        || diagnostics_truncated
                        || returned_error_count < total_errors,
                    "errors": returned_errors,
                })),
            )
        }
        other => bounded_internal_error(&other, limits),
    }
}

fn bounded_validation_entry(
    error: ValidationError,
    limits: &ResultLimits,
    max_bytes: usize,
) -> Option<(Value, bool)> {
    let ValidationError {
        path,
        message,
        suggestion,
    } = error;
    let minimum = json!({"path": "", "message": "", "truncated": false});
    let minimum_bytes = json_serialized_len(&minimum).ok()?;
    if max_bytes < minimum_bytes {
        return None;
    }

    let string_budget = max_bytes.saturating_sub(minimum_bytes.saturating_sub(4));
    let path_budget = (string_budget / 3).max(2);
    let (path, path_truncated) = bounded_json_text(&path, limits, path_budget);
    let base = json!({"path": path, "message": "", "truncated": false});
    let base_bytes = json_serialized_len(&base).ok()?;
    let message_budget = max_bytes.saturating_sub(base_bytes.saturating_sub(2));
    let (message, message_truncated) = bounded_json_text(&message, limits, message_budget);
    let mut entry = json!({
        "path": path,
        "message": message,
        "truncated": false,
    });
    let mut entry_truncated = path_truncated || message_truncated;

    if let Some(suggestion) = suggestion {
        let entry_bytes = json_serialized_len(&entry).ok()?;
        let mut empty_suggestion = entry.clone();
        empty_suggestion["suggestion"] = json!("");
        let suggestion_overhead = json_serialized_len(&empty_suggestion)
            .ok()?
            .saturating_sub(entry_bytes)
            .saturating_sub(2);
        let suggestion_budget = max_bytes
            .saturating_sub(entry_bytes)
            .saturating_sub(suggestion_overhead);
        if suggestion_budget >= 2 {
            let (suggestion, suggestion_truncated) =
                bounded_json_text(&suggestion, limits, suggestion_budget);
            empty_suggestion["suggestion"] = json!(suggestion);
            if json_serialized_len(&empty_suggestion).ok()? <= max_bytes {
                entry = empty_suggestion;
                entry_truncated |= suggestion_truncated;
            } else {
                entry_truncated = true;
            }
        } else {
            entry_truncated = true;
        }
    }

    if entry_truncated {
        entry["truncated"] = Value::Bool(true);
    } else {
        entry.as_object_mut()?.remove("truncated");
    }
    (json_serialized_len(&entry).ok()? <= max_bytes).then_some((entry, entry_truncated))
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Warnings and errors from rmcp and the engine go to stderr (RUST_LOG raises the level).
    // Without a subscriber a transport failure ends the process silently with status 0.
    xbbg_log::init();
    let server = XbbgMcpServer::new_from_env()?;
    let (stdin, stdin_monitor) = stdin::stdin()?;
    let quit_reason = server
        .serve((stdin, tokio::io::stdout()))
        .await?
        .waiting()
        .await?;
    match quit_reason {
        rmcp::service::QuitReason::Closed => stdin_monitor.ensure_clean_shutdown()?,
        rmcp::service::QuitReason::Cancelled => {
            return Err(std::io::Error::other("MCP service was cancelled").into());
        }
        rmcp::service::QuitReason::JoinError(err) => return Err(err.into()),
        reason => {
            return Err(std::io::Error::other(format!(
                "MCP service stopped unexpectedly: {reason:?}"
            ))
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use xbbg_core::EntitlementCheck;

    #[test]
    fn mcp_tool_names_and_input_schemas_are_stable() {
        let tools = XbbgMcpServer::tool_router().list_all();
        let names = tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "auction_snapshot",
                "bdh",
                "bdib",
                "bdp",
                "bds",
                "bflds",
                "bql",
                "bsrch",
                "check_entitlements",
                "request",
                "resolve_venues"
            ]
        );

        let tool_by_name = tools
            .iter()
            .map(|tool| (tool.name.as_ref(), tool))
            .collect::<HashMap<_, _>>();
        let bdp_schema = tool_by_name
            .get("bdp")
            .expect("bdp tool")
            .input_schema
            .get("properties")
            .expect("bdp properties");
        assert!(bdp_schema.get("tickers").is_some());
        assert!(bdp_schema.get("fields").is_some());
        assert!(bdp_schema.get("validate_fields").is_some());
        assert!(bdp_schema.get("return_eids").is_some());

        for tool_name in ["bdh", "bdib", "bds"] {
            let schema = tool_by_name
                .get(tool_name)
                .expect("dedicated EID-capable tool")
                .input_schema
                .get("properties")
                .expect("tool properties");
            assert!(
                schema.get("return_eids").is_some(),
                "{tool_name} must advertise return_eids"
            );
        }

        let entitlement_schema = tool_by_name
            .get("check_entitlements")
            .expect("check_entitlements tool")
            .input_schema
            .get("properties")
            .expect("check_entitlements properties");
        assert!(entitlement_schema.get("eids").is_some());
        assert!(entitlement_schema.get("service").is_some());

        for tool_name in ["resolve_venues", "auction_snapshot"] {
            let tool = tool_by_name.get(tool_name).expect("auction tool");
            let schema = tool.input_schema.get("properties").unwrap();
            assert_eq!(
                tool.input_schema.get("required"),
                Some(&json!(["securities"]))
            );
            assert_eq!(schema["securities"]["type"], "array");
            assert_eq!(schema["securities"]["minItems"], 1);
            assert!(schema["securities"].get("maxItems").is_none());
            assert!(schema.get("pcs_overrides").is_some());
            let annotations = tool.annotations.as_ref().unwrap();
            assert_eq!(annotations.read_only_hint, Some(true));
            assert_eq!(annotations.destructive_hint, Some(false));
        }
        let snapshot_schema = tool_by_name["auction_snapshot"]
            .input_schema
            .get("properties")
            .unwrap();
        assert_eq!(snapshot_schema["fields"]["maxItems"], 256);
        assert_eq!(snapshot_schema["groups"]["maxItems"], 8);

        let generic_schema = tool_by_name
            .get("request")
            .expect("generic request tool")
            .input_schema
            .get("properties")
            .expect("request properties");
        assert!(generic_schema.get("request_operation").is_some());
        assert!(generic_schema.get("return_eids").is_some());
        assert!(generic_schema.get("request_id").is_some());
        assert!(generic_schema.get("jsonElements").is_none());
    }

    #[test]
    fn entitlement_result_is_structured_json() {
        let payload = entitlement_check_to_json(
            "//blp/refdata".to_string(),
            vec![101, 202],
            EntitlementCheck {
                entitled: false,
                failed_eids: vec![202],
            },
            &ResultLimits::default(),
        )
        .unwrap();

        assert_eq!(payload["service"], "//blp/refdata");
        assert_eq!(payload["eids"], json!([101, 202]));
        assert_eq!(payload["entitled"], false);
        assert_eq!(payload["failed_eids"], json!([202]));
        assert_eq!(payload["total_eids"], 2);
        assert_eq!(payload["returned_eids"], 2);
        assert_eq!(payload["truncated"]["output"], false);
    }

    #[test]
    fn validation_errors_are_bounded_as_complete_structured_records() {
        let limits = ResultLimits {
            max_result_bytes: MIN_RESULT_BYTES,
            max_string_chars: 8_192,
            max_string_bytes: 8_192,
            ..ResultLimits::default()
        };
        let errors = (0..100)
            .map(|index| ValidationError {
                path: format!("field.{index}"),
                message: "invalid".repeat(1_000),
                suggestion: Some("PX_LAST".repeat(1_000)),
            })
            .collect();

        let error = map_blp_error(
            BlpError::Validation {
                message: "validation failed".repeat(128),
                errors,
            },
            &limits,
        );
        assert!(serde_json::to_vec(&error).unwrap().len() <= limits.max_result_bytes);
        let data = error.data.expect("bounded validation diagnostics");

        assert_eq!(data["total_errors"], 100);
        assert!(data["returned_errors"].as_u64().unwrap() < 100);
        assert!(data["returned_errors"].as_u64().unwrap() > 0);
        assert_eq!(
            data["returned_errors"].as_u64().unwrap() + data["omitted_errors"].as_u64().unwrap(),
            100
        );
        assert!(data["errors"]
            .as_array()
            .unwrap()
            .iter()
            .all(|entry| { entry.get("path").is_some() && entry.get("message").is_some() }));
        let primary = &data["errors"][0];
        assert_eq!(primary["path"], "field.0");
        assert!(primary["message"].as_str().unwrap().ends_with('…'));
        assert!(primary.get("suggestion").is_none());
        assert_eq!(primary["truncated"], true);
    }

    #[test]
    fn escaped_error_text_respects_the_compact_json_budget() {
        let limits = ResultLimits {
            max_result_bytes: MIN_RESULT_BYTES,
            max_string_chars: 2_000,
            max_string_bytes: 2_000,
            ..ResultLimits::default()
        };

        let error = bounded_invalid_params(&"\u{1}".repeat(2_000), &limits);

        assert!(serde_json::to_vec(&error).unwrap().len() <= limits.max_result_bytes);
        assert_eq!(error.data.unwrap()["message_truncated"], true);
    }

    #[test]
    fn adapter_error_data_is_replaced_by_a_bounded_omission_marker() {
        let limits = ResultLimits {
            max_result_bytes: MIN_RESULT_BYTES,
            ..ResultLimits::default()
        };
        let original =
            ErrorData::invalid_params("invalid request", Some(json!({"raw": "x".repeat(100_000)})));

        let error = bound_existing_error(original, &limits);

        assert!(serde_json::to_vec(&error).unwrap().len() <= limits.max_result_bytes);
        assert_eq!(error.data.unwrap()["error_data_omitted"], true);
    }

    #[test]
    fn recipe_errors_keep_request_classification_and_output_bounds() {
        let limits = ResultLimits {
            max_result_bytes: MIN_RESULT_BYTES,
            ..ResultLimits::default()
        };
        for error in [
            RecipeError::Engine(Box::new(BlpAsyncError::ConfigError {
                detail: "x".repeat(100_000),
            })),
            RecipeError::InvalidArgument("x".repeat(100_000)),
        ] {
            let error = map_recipe_error(error, &limits);
            assert_eq!(error.code, rmcp::model::ErrorCode::INVALID_PARAMS);
            assert!(serde_json::to_vec(&error).unwrap().len() <= limits.max_result_bytes);
            assert_eq!(error.data.unwrap()["message_truncated"], true);
        }
        let error = map_recipe_error(RecipeError::Other("x".repeat(100_000)), &limits);
        assert_eq!(error.code, rmcp::model::ErrorCode::INTERNAL_ERROR);
        assert!(serde_json::to_vec(&error).unwrap().len() <= limits.max_result_bytes);
        assert_eq!(error.data.unwrap()["message_truncated"], true);
    }
}
