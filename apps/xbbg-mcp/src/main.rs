use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use rmcp::handler::server::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Implementation, ServerCapabilities, ServerConfig};
use rmcp::{ErrorData, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use serde_json::{Value, json};
use tokio::sync::{OnceCell, Semaphore};
use xbbg_async::BlpAsyncError;
use xbbg_async::engine::{Engine, EngineConfig, KeyColumn, RequestParams};
use xbbg_async::error_presentation::blp_error_display;
use xbbg_core::BlpError;
use xbbg_core::errors::ValidationError;
use xbbg_host::teardown::{DeferredDrop, wait_idle_timeout};
use xbbg_recipes::{
    RecipeError, recipe_auction_snapshot, recipe_delay_status, recipe_resolve_venues,
};

mod request_adapter;
mod runtime;
mod serialization;
mod settings;
mod stdin;

use request_adapter::{
    AuctionSnapshotArgs, BdhArgs, BdibArgs, BdpArgs, BdsArgs, BfldsArgs, BqlArgs, BsrchArgs,
    CheckEntitlementsArgs, DelayStatusArgs, RequestArgs, ResolveVenuesArgs,
    auction_snapshot_params, bdh_request_params, bdib_request_params, bdp_request_params,
    bds_request_params, bflds_request_params, bql_request_params, bsrch_request_params,
    check_entitlements_params, delay_status_params, generic_request_params, resolve_venues_params,
};

use serialization::{
    ResultLimits, bounded_error_display, bounded_error_text, bounded_json_text,
    entitlement_check_to_json, json_serialized_len, record_batch_to_json, should_offload,
    should_offload_items,
};

/// The engine, started on first use. `DeferredDrop` moves the last release off whatever thread
/// holds it: dropping an engine stops its Bloomberg sessions and joins its runtime, which must
/// not happen on a Tokio worker. `main` waits for the deferred drop before the process exits.
type EngineCell = OnceCell<DeferredDrop<Engine>>;

struct XbbgMcpServer {
    #[allow(dead_code)] // read by rmcp's generated ServerHandler impl
    tool_router: ToolRouter<Self>,
    engine: Arc<EngineCell>,
    engine_config: EngineConfig,
    result_limits: ResultLimits,
    serialization_permits: Arc<Semaphore>,
}

impl XbbgMcpServer {
    fn new_from_env() -> Result<Self, String> {
        const MAX_CONCURRENT_SERIALIZATIONS: usize = 2;

        let (engine_config, result_limits) = settings::load_from_env()?;
        if engine_config.key_column == KeyColumn::Ticker {
            xbbg_log::warn!(
                "results name the security column 'ticker', which collides with Bloomberg's \
                 TICKER field and becomes 'security' in the next major release; set \
                 XBBG_MCP_KEY_COLUMN=security to switch now"
            );
        }
        Ok(Self {
            tool_router: Self::tool_router(),
            engine: Arc::new(OnceCell::new()),
            engine_config,
            result_limits,
            serialization_permits: Arc::new(Semaphore::new(MAX_CONCURRENT_SERIALIZATIONS)),
        })
    }

    fn bounded_input<T>(&self, result: Result<T, ErrorData>) -> Result<T, ErrorData> {
        result.map_err(|error| bound_existing_error(error, &self.result_limits))
    }

    /// A second owner of the engine cell, so the caller decides when the last release happens.
    fn engine_cell(&self) -> Arc<EngineCell> {
        Arc::clone(&self.engine)
    }

    async fn engine(&self) -> Result<&Engine, ErrorData> {
        self.engine
            .get_or_try_init(|| async {
                let config = self.engine_config.clone();
                let limits = self.result_limits.clone();
                tokio::task::spawn_blocking(move || {
                    Engine::start(config)
                        .map(DeferredDrop::new)
                        .map_err(|error| map_request_error(error, &limits))
                })
                .await
                .map_err(|error| bounded_internal_error(&error, &self.result_limits))?
            })
            .await
            .map(|engine| &**engine)
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
        description = "Bloomberg Query Language request (bql). app_name sets clientContext.appName and overrides XBBG_MCP_BQL_APP_NAME. Without either setting, the current Bloomberg default is unchanged.",
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
        let params = self.bounded_input(bql_request_params(args))?;
        self.execute_request(params).await
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
        description = "Read auction/imbalance values from Bloomberg reference data on the primary venue. Uses the default auction field group unless fields or groups are selected; groups expand first, then explicit fields, with duplicates removed. Returns one row per input with status/error and bounded structured JSON with Arrow schema metadata. Time-of-day fields are terminal-local. Values can be delayed for this session; delay_status reports which tickers are.",
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
        description = "Report per ticker whether this Bloomberg session receives delayed or real-time data. delayed is what this session receives (Bloomberg IS_DELAYED_STREAM), not the exchange's standard delay: delay_minutes is set only when delayed, and exchange_delay is the raw EXCHANGE_DELAY text. Returns one row per unique ticker in input order, including exch_code, real-time feed EIDs (realtime_eid1 to realtime_eid4), delayed_eid, and error, as bounded structured JSON with Arrow schema metadata. check_identity=true also checks the session identity against those feed EIDs on //blp/mktdata and fills realtime_entitled and delayed_entitled; it needs SAPI/B-PIPE authorization or an EMRS-enrolled Desktop API user, otherwise the call fails.",
        annotations(
            title = "Bloomberg Delayed-Data Status",
            read_only_hint = true,
            destructive_hint = false,
            idempotent_hint = false,
            open_world_hint = true
        )
    )]
    async fn delay_status(
        &self,
        Parameters(args): Parameters<DelayStatusArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let params = self.bounded_input(delay_status_params(args))?;
        let engine = self.engine().await?;
        let batch = recipe_delay_status(engine, params.tickers, params.check_identity)
            .await
            .map_err(|error| map_recipe_error(error, &self.result_limits))?;
        self.serialize_batch(batch).await
    }

    #[tool(
        description = "Generic Bloomberg request. Supports raw/custom service and operation strings, including RawRequest via request_operation. return_eids supports ReferenceDataRequest (including BDS/bulk), HistoricalDataRequest, IntradayBarRequest, and IntradayTickRequest. on_delayed supports ReferenceDataRequest (not BDS/bulk), HistoricalDataRequest, IntradayBarRequest, and IntradayTickRequest.",
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
    fn get_info(&self) -> ServerConfig {
        // The server only advertises tools for now; request execution stays lazy so stdio startup
        // does not require a live Bloomberg session before the client can initialize.
        ServerConfig::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new(env!("CARGO_PKG_NAME"), server_version())
                    .with_title("xbbg MCP")
                    .with_description(
                        "Request/response Bloomberg tools backed directly by xbbg-async. Current env configuration supports single-host connectivity, core auth modes, and request-pool tuning.",
                    ),
            )
            .with_instructions(
                "Use bdp, bdh, bds, bdib, bql, bsrch, bflds, resolve_venues, auction_snapshot, check_entitlements, delay_status, or request. resolve_venues routes composite equities to primary exchange listings and preferreds to venue pricing sources, preserving one status/error row per input. auction_snapshot reads Bloomberg reference data on the primary venue; time-of-day fields are terminal-local. Bloomberg can serve delayed data without marking the rows: delay_status reports per ticker whether this session receives delayed data (check_identity=true also checks feed-EID entitlements on //blp/mktdata and needs SAPI/B-PIPE authorization or an EMRS-enrolled Desktop API user), and on_delayed on bdp, bdh, bdib, or request either reports xbbg.delayed metadata (warn and ignore behave the same) or fails the call (raise). Results are structured JSON with Arrow schema and metadata, explicit truncation counts, and bounded rows, cells, strings, metadata, and total bytes. This MCP server currently exposes host/port, selected auth env vars, core pool settings, and result limits rather than the full EngineConfig surface. For XBBG_MCP_VALIDATION_MODE/XBBG_VALIDATION_MODE use disabled (default), lenient, or strict; for XBBG_MCP_SDK_LOG_LEVEL/XBBG_SDK_LOG_LEVEL use off (default), fatal, error, warn, info, debug, or trace; for XBBG_MCP_OVERFLOW_POLICY/XBBG_OVERFLOW_POLICY use drop_newest (default) or block; for XBBG_MCP_KEY_COLUMN/XBBG_KEY_COLUMN use ticker (default, deprecated) or security, the name of the result column holding each row's security.",
            )
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
        RecipeError::RequestContext { source, .. } => map_recipe_error(*source, limits),
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
        BlpAsyncError::Blp(blp_error) => map_blp_error(blp_error, limits),
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
        other => bounded_internal_error(&blp_error_display(&other), limits),
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
    // The runtime must be loaded before anything touches the engine: nothing links Bloomberg.
    let sdk = match runtime::load() {
        Ok(sdk) => sdk,
        Err(error) => {
            // Printed, not returned: `main`'s error is shown with `Debug`, which would flatten the
            // rejection list onto one line.
            eprintln!("{}", runtime::startup_error(&error));
            std::process::exit(1);
        }
    };
    xbbg_log::info!(
        path = %sdk.path.display(),
        source = %sdk.source,
        detail = %sdk.detail,
        version = %sdk.version,
        "loaded the Bloomberg runtime"
    );
    let server = match XbbgMcpServer::new_from_env() {
        Ok(server) => server,
        Err(message) => {
            // Printed, not returned, for the same reason as the runtime error above.
            eprintln!("xbbg-mcp: {message}");
            std::process::exit(1);
        }
    };
    let (stdin, stdin_monitor) = stdin::stdin()?;
    let engine = server.engine_cell();
    let served = async {
        let quit_reason = server
            .serve((stdin, tokio::io::stdout()))
            .await?
            .waiting()
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(quit_reason)
    }
    .await;
    // The service is done, so this is the last owner of the engine: release it and let its
    // Bloomberg sessions stop before the process exits.
    drop(engine);
    wait_for_engine_teardown().await;
    let quit_reason = served?;
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

/// How long exit waits for the engine's sessions to stop before giving up on them.
const ENGINE_TEARDOWN_TIMEOUT: Duration = Duration::from_secs(10);

async fn wait_for_engine_teardown() {
    // A blocking wait on a condition variable, so it stays off the async workers.
    let finished = tokio::task::spawn_blocking(|| wait_idle_timeout(ENGINE_TEARDOWN_TIMEOUT))
        .await
        .unwrap_or(false);
    if !finished {
        xbbg_log::warn!(
            timeout_secs = ENGINE_TEARDOWN_TIMEOUT.as_secs(),
            "Bloomberg sessions were still stopping at exit"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serialization::MIN_RESULT_BYTES;
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
                "delay_status",
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

        for tool_name in ["bdp", "bdh", "bdib", "request"] {
            let schema = &tool_by_name[tool_name].input_schema;
            assert!(
                schema["properties"].get("on_delayed").is_some(),
                "{tool_name} must advertise on_delayed"
            );
            assert_eq!(
                schema["$defs"]["DelayedDataPolicy"]["enum"],
                json!(["warn", "raise", "ignore"])
            );
        }
        assert!(
            tool_by_name["bds"].input_schema["properties"]
                .get("on_delayed")
                .is_none(),
            "bulk data does not support on_delayed"
        );

        let entitlement_schema = tool_by_name
            .get("check_entitlements")
            .expect("check_entitlements tool")
            .input_schema
            .get("properties")
            .expect("check_entitlements properties");
        assert!(entitlement_schema.get("eids").is_some());
        assert!(entitlement_schema.get("service").is_some());

        let delay_status = tool_by_name["delay_status"];
        assert_eq!(
            delay_status.input_schema.get("required"),
            Some(&json!(["tickers"]))
        );
        let delay_schema = delay_status.input_schema.get("properties").unwrap();
        assert_eq!(delay_schema["tickers"]["minItems"], 1);
        assert_eq!(delay_schema["check_identity"]["type"], "boolean");
        let annotations = delay_status.annotations.as_ref().unwrap();
        assert_eq!(annotations.read_only_hint, Some(true));
        assert_eq!(annotations.destructive_hint, Some(false));

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
        assert!(
            data["errors"]
                .as_array()
                .unwrap()
                .iter()
                .all(|entry| { entry.get("path").is_some() && entry.get("message").is_some() })
        );
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
    fn recipe_context_keeps_mcp_error_classification_and_bounds() {
        let limits = ResultLimits {
            max_result_bytes: MIN_RESULT_BYTES,
            max_string_chars: 128,
            max_string_bytes: 256,
            ..ResultLimits::default()
        };
        for invalid_argument in [false, true] {
            let make_error = || {
                let detail = "synthetic failure ".repeat(1_000);
                if invalid_argument {
                    RecipeError::InvalidArgument(detail)
                } else {
                    RecipeError::Other(detail)
                }
            };
            let expected = map_recipe_error(make_error(), &limits);
            let wrapped =
                make_error().with_bloomberg_request_ids([std::sync::Arc::from("sdk-synthetic")]);
            let actual = map_recipe_error(wrapped, &limits);
            assert_eq!(actual.code, expected.code);
            assert_eq!(actual.message, expected.message);
            assert_eq!(actual.data, expected.data);
            assert!(serde_json::to_vec(&actual).unwrap().len() <= limits.max_result_bytes);
        }
    }

    #[test]
    fn request_failure_context_reaches_mcp_clients() {
        let denial = "User not authorized to use BQL";
        let error = map_blp_error(
            BlpError::RequestFailure {
                service: "//blp/bqlsvc".into(),
                operation: Some("sendQuery".into()),
                cid: None,
                label: None,
                request_id: None,
                bloomberg_request_ids: Vec::new(),
                source: Some(denial.into()),
            },
            &ResultLimits::default(),
        );

        assert_eq!(error.code, rmcp::model::ErrorCode::INTERNAL_ERROR);
        assert!(error.message.contains(denial));
        assert!(error.message.contains("//blp/bqlsvc"));
        assert!(error.message.contains("sendQuery"));
    }

    #[test]
    fn request_failures_without_a_source_retain_request_context() {
        let error = map_blp_error(
            BlpError::RequestFailure {
                service: "//blp/bqlsvc".into(),
                operation: Some("sendQuery".into()),
                cid: Some(xbbg_core::errors::CorrelationContext::U64(42)),
                label: Some("synthetic denial".into()),
                request_id: Some("synthetic-request".into()),
                bloomberg_request_ids: Vec::new(),
                source: None,
            },
            &ResultLimits::default(),
        );

        assert_eq!(error.code, rmcp::model::ErrorCode::INTERNAL_ERROR);
        for context in [
            "//blp/bqlsvc",
            "sendQuery",
            "42",
            "synthetic denial",
            "synthetic-request",
        ] {
            assert!(error.message.contains(context));
        }
        assert!(error.data.is_none());
    }

    #[test]
    fn request_failure_sources_respect_string_and_serialized_budgets() {
        for fragment in ["\u{1}\"\\\n", "拒否🙂", "synthetic denial "] {
            for (max_string_chars, max_string_bytes) in [(2_000, 2_000), (128, 256)] {
                let limits = ResultLimits {
                    max_result_bytes: MIN_RESULT_BYTES,
                    max_string_chars,
                    max_string_bytes,
                    ..ResultLimits::default()
                };
                let source = format!(
                    "User not authorized to use BQL: {}",
                    fragment.repeat(10_000)
                );
                let error = map_blp_error(
                    BlpError::RequestFailure {
                        service: "//blp/bqlsvc".into(),
                        operation: Some("sendQuery".into()),
                        cid: None,
                        label: None,
                        request_id: None,
                        bloomberg_request_ids: Vec::new(),
                        source: Some(source.into()),
                    },
                    &limits,
                );

                assert_eq!(error.code, rmcp::model::ErrorCode::INTERNAL_ERROR);
                assert!(error.message.contains("User not authorized to use BQL"));
                assert!(error.message.ends_with('…'));
                assert!(error.message.chars().count() <= limits.max_string_chars);
                assert!(error.message.len() <= limits.max_string_bytes);
                assert!(serde_json::to_vec(&error).unwrap().len() <= limits.max_result_bytes);
                assert_eq!(error.data.unwrap()["message_truncated"], true);
            }
        }
    }

    #[test]
    fn request_failure_source_formatting_stops_at_the_output_bound() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct StreamingSource(Arc<AtomicUsize>);

        impl std::fmt::Display for StreamingSource {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                for _ in 0..1_000_000 {
                    self.0.fetch_add(1, Ordering::Relaxed);
                    formatter.write_str("synthetic denial 🙂\n")?;
                }
                Ok(())
            }
        }

        impl std::error::Error for StreamingSource {}

        let limits = ResultLimits {
            max_result_bytes: MIN_RESULT_BYTES,
            max_string_chars: 128,
            max_string_bytes: 256,
            ..ResultLimits::default()
        };
        let writes = Arc::new(AtomicUsize::new(0));
        let error = map_blp_error(
            BlpError::RequestFailure {
                service: "//blp/bqlsvc".into(),
                operation: Some("sendQuery".into()),
                cid: None,
                label: None,
                request_id: None,
                bloomberg_request_ids: Vec::new(),
                source: Some(Box::new(StreamingSource(Arc::clone(&writes)))),
            },
            &limits,
        );

        assert!(error.message.contains("synthetic denial"));
        assert!(error.message.ends_with('…'));
        assert_eq!(error.data.as_ref().unwrap()["message_truncated"], true);
        assert!(serde_json::to_vec(&error).unwrap().len() <= limits.max_result_bytes);
        let writes = writes.load(Ordering::Relaxed);
        assert!(writes > 0);
        assert!(writes <= limits.max_string_chars + 1);
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
