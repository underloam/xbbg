//! Engine configuration and result limits from the process environment.
//!
//! The environment is read into an [`EngineConfigSpec`] and `xbbg-host` turns it into the engine
//! configuration, so the pool, transport, TLS and authentication checks are the ones every binding
//! runs. The variable names, their legacy aliases and the defaults live here; the host's messages
//! name configuration members (`requestPoolSize`, `auth.appName`), so [`describe`] puts the
//! variable that set the value in their place.

use std::env;
use std::fmt::Display;

use xbbg_async::engine::EngineConfig;
use xbbg_host::config::{AuthConfigSpec, EngineConfigSpec, RetryPolicySpec};
use xbbg_host::errors::HostError;

use crate::serialization::{MIN_RESULT_BYTES, ResultLimits};

const HOST: &[&str] = &[
    "XBBG_MCP_HOST",
    "XBBG_MCP_SERVER_HOST",
    "XBBG_HOST",
    "XBBG_SERVER_HOST",
    "XBBG_SERVER",
];
const PORT: &[&str] = &[
    "XBBG_MCP_PORT",
    "XBBG_MCP_SERVER_PORT",
    "XBBG_PORT",
    "XBBG_SERVER_PORT",
];
const BQL_APP_NAME: &[&str] = &["XBBG_MCP_BQL_APP_NAME"];
const REQUEST_POOL_SIZE: &[&str] = &["XBBG_MCP_REQUEST_POOL_SIZE", "XBBG_REQUEST_POOL_SIZE"];
const VALIDATION_MODE: &[&str] = &["XBBG_MCP_VALIDATION_MODE", "XBBG_VALIDATION_MODE"];
const FIELD_CACHE_PATH: &[&str] = &["XBBG_MCP_FIELD_CACHE_PATH", "XBBG_FIELD_CACHE_PATH"];
const WARMUP_SERVICES: &[&str] = &["XBBG_MCP_WARMUP_SERVICES", "XBBG_WARMUP_SERVICES"];
const SDK_LOG_LEVEL: &[&str] = &["XBBG_MCP_SDK_LOG_LEVEL", "XBBG_SDK_LOG_LEVEL"];
const NUM_START_ATTEMPTS: &[&str] = &[
    "XBBG_MCP_NUM_START_ATTEMPTS",
    "XBBG_NUM_START_ATTEMPTS",
    "XBBG_MAX_ATTEMPT",
];
const AUTO_RESTART: &[&str] = &[
    "XBBG_MCP_AUTO_RESTART_ON_DISCONNECTION",
    "XBBG_AUTO_RESTART_ON_DISCONNECTION",
    "XBBG_AUTO_RESTART",
];
const RETRY_MAX_RETRIES: &[&str] = &["XBBG_MCP_RETRY_MAX_RETRIES", "XBBG_RETRY_MAX_RETRIES"];
const RETRY_INITIAL_DELAY_MS: &[&str] = &[
    "XBBG_MCP_RETRY_INITIAL_DELAY_MS",
    "XBBG_RETRY_INITIAL_DELAY_MS",
];
const RETRY_BACKOFF_FACTOR: &[&str] =
    &["XBBG_MCP_RETRY_BACKOFF_FACTOR", "XBBG_RETRY_BACKOFF_FACTOR"];
const RETRY_MAX_DELAY_MS: &[&str] = &["XBBG_MCP_RETRY_MAX_DELAY_MS", "XBBG_RETRY_MAX_DELAY_MS"];
const OVERFLOW_POLICY: &[&str] = &["XBBG_MCP_OVERFLOW_POLICY", "XBBG_OVERFLOW_POLICY"];
const KEY_COLUMN: &[&str] = &["XBBG_MCP_KEY_COLUMN", "XBBG_KEY_COLUMN"];
const AUTH_METHOD: &[&str] = &["XBBG_MCP_AUTH_METHOD", "XBBG_AUTH_METHOD"];
const APP_NAME: &[&str] = &["XBBG_MCP_APP_NAME", "XBBG_APP_NAME"];
const DIR_PROPERTY: &[&str] = &["XBBG_MCP_DIR_PROPERTY", "XBBG_DIR_PROPERTY"];
const USER_ID: &[&str] = &["XBBG_MCP_USER_ID", "XBBG_USER_ID"];
const IP_ADDRESS: &[&str] = &["XBBG_MCP_IP_ADDRESS", "XBBG_IP_ADDRESS"];
const TOKEN: &[&str] = &["XBBG_MCP_TOKEN", "XBBG_TOKEN"];

/// The host's member labels (as they appear in its messages or error context) and the variables
/// that set them.
const LABELS: &[(&str, &[&str])] = &[
    ("auth.method", AUTH_METHOD),
    ("auth.appName", APP_NAME),
    ("auth.dirProperty", DIR_PROPERTY),
    ("auth.userId", USER_ID),
    ("auth.ipAddress", IP_ADDRESS),
    ("auth.token", TOKEN),
    ("bqlAppName", BQL_APP_NAME),
    ("requestPoolSize", REQUEST_POOL_SIZE),
    ("validationMode", VALIDATION_MODE),
    ("overflowPolicy", OVERFLOW_POLICY),
    ("keyColumn", KEY_COLUMN),
    ("sdkLogLevel", SDK_LOG_LEVEL),
];

/// Reads the engine configuration and the result limits from the process environment.
pub(crate) fn load_from_env() -> Result<(EngineConfig, ResultLimits), String> {
    load(&|key| env::var(key).ok())
}

type Lookup<'a> = &'a dyn Fn(&str) -> Option<String>;

fn load(get: Lookup<'_>) -> Result<(EngineConfig, ResultLimits), String> {
    // Keep the initial env surface intentionally narrow: expose the subset we can document and
    // support honestly for MCP deployment, rather than implying full parity with every EngineConfig knob.
    let env = Env { get };
    let spec = EngineConfigSpec {
        host: env.string(HOST),
        port: env.parse(PORT, "u16", str::parse)?,
        bql_app_name: env.string(BQL_APP_NAME),
        request_pool_size: env.parse(REQUEST_POOL_SIZE, "u32", str::parse)?,
        validation_mode: env.string(VALIDATION_MODE),
        field_cache_path: env.string(FIELD_CACHE_PATH),
        warmup_services: env.csv(WARMUP_SERVICES),
        sdk_log_level: env.string(SDK_LOG_LEVEL),
        num_start_attempts: env.parse(NUM_START_ATTEMPTS, "u32", str::parse)?,
        auto_restart_on_disconnection: env.parse(AUTO_RESTART, "bool", parse_bool)?,
        retry_policy: Some(RetryPolicySpec {
            max_retries: env.parse(RETRY_MAX_RETRIES, "u32", str::parse)?,
            initial_delay_ms: env.millis(RETRY_INITIAL_DELAY_MS)?,
            backoff_factor: env.parse(RETRY_BACKOFF_FACTOR, "f64", str::parse)?,
            max_delay_ms: env.millis(RETRY_MAX_DELAY_MS)?,
        }),
        overflow_policy: env.string(OVERFLOW_POLICY),
        key_column: env.string(KEY_COLUMN),
        auth: env.auth()?,
        ..EngineConfigSpec::default()
    };
    let config = spec
        .into_engine_config()
        .map_err(|error| env.describe(&error))?;

    let defaults = ResultLimits::default();
    let result_limits = ResultLimits {
        max_rows: env.limit(&["XBBG_MCP_MAX_ROWS"], "max_rows", defaults.max_rows, 1)?,
        max_cells: env.limit(&["XBBG_MCP_MAX_CELLS"], "max_cells", defaults.max_cells, 1)?,
        max_metadata_properties: env.limit(
            &["XBBG_MCP_MAX_METADATA_PROPERTIES"],
            "max_metadata_properties",
            defaults.max_metadata_properties,
            1,
        )?,
        max_metadata_bytes: env.limit(
            &["XBBG_MCP_MAX_METADATA_BYTES"],
            "max_metadata_bytes",
            defaults.max_metadata_bytes,
            1,
        )?,
        max_string_chars: env.limit(
            &["XBBG_MCP_MAX_STRING_CHARS"],
            "max_string_chars",
            defaults.max_string_chars,
            1,
        )?,
        max_string_bytes: env.limit(
            &["XBBG_MCP_MAX_STRING_BYTES"],
            "max_string_bytes",
            defaults.max_string_bytes,
            '…'.len_utf8(),
        )?,
        max_result_bytes: env.limit(
            &["XBBG_MCP_MAX_RESULT_BYTES"],
            "max_result_bytes",
            defaults.max_result_bytes,
            MIN_RESULT_BYTES,
        )?,
    };

    Ok((config, result_limits))
}

struct Env<'a> {
    get: Lookup<'a>,
}

impl Env<'_> {
    /// The first variable with a non-blank value, trimmed.
    fn string(&self, keys: &[&str]) -> Option<String> {
        keys.iter().find_map(|key| {
            (self.get)(key)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        })
    }

    /// The first variable that is set at all, as a comma-separated list; blank entries are dropped.
    fn csv(&self, keys: &[&str]) -> Option<Vec<String>> {
        keys.iter().find_map(|key| {
            (self.get)(key).map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>()
            })
        })
    }

    fn parse<T, E: Display>(
        &self,
        keys: &[&str],
        label: &str,
        parser: impl Fn(&str) -> Result<T, E>,
    ) -> Result<Option<T>, String> {
        for key in keys {
            if let Some(raw) = (self.get)(key) {
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

    /// A millisecond count; the spec carries it as `i64`.
    fn millis(&self, keys: &[&str]) -> Result<Option<i64>, String> {
        self.parse(keys, "u64", |value| {
            let millis: u64 = value
                .parse()
                .map_err(|error: std::num::ParseIntError| error.to_string())?;
            i64::try_from(millis).map_err(|_| format!("{millis} is larger than {}", i64::MAX))
        })
    }

    fn limit(
        &self,
        keys: &[&str],
        label: &str,
        default: usize,
        minimum: usize,
    ) -> Result<usize, String> {
        let value = self.parse(keys, "usize", str::parse)?.unwrap_or(default);
        if value < minimum {
            return Err(format!("{label} must be at least {minimum}, got {value}"));
        }
        Ok(value)
    }

    /// The `auth` member. The host requires a method once it sees an `auth` object, so settings
    /// that name a method's parameters without a method are refused here, in terms of variables.
    fn auth(&self) -> Result<Option<AuthConfigSpec>, String> {
        let app_name = self.string(APP_NAME);
        let dir_property = self.string(DIR_PROPERTY);
        let user_id = self.string(USER_ID);
        let ip_address = self.string(IP_ADDRESS);
        let token = self.string(TOKEN);

        let Some(method) = self.string(AUTH_METHOD) else {
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
        Ok(Some(AuthConfigSpec {
            method,
            app_name,
            dir_property,
            user_id,
            ip_address,
            token,
        }))
    }

    /// The variable of `keys` that supplied the value: the first one set, else the primary name.
    fn active<'k>(&self, keys: &[&'k str]) -> &'k str {
        keys.iter()
            .copied()
            .find(|key| (self.get)(key).is_some_and(|value| !value.trim().is_empty()))
            .unwrap_or(keys[0])
    }

    /// A startup message for a configuration the host rejected: the member it names becomes the
    /// variable that set it.
    fn describe(&self, error: &HostError) -> String {
        let mut message = error.message.clone();
        for (label, keys) in LABELS {
            if message.contains(label) {
                message = message.replace(label, self.active(keys));
            }
        }
        match error.context.element.as_deref() {
            // A member-level fault: the message is the parser's text and does not name it.
            Some(element) => match LABELS.iter().find(|(label, _)| *label == element) {
                Some((_, keys)) => format!("invalid {}: {message}", self.active(keys)),
                None => format!("invalid configuration ({element}): {message}"),
            },
            None => format!("invalid configuration: {message}"),
        }
    }
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("expected true/false style boolean, got '{value}'")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use xbbg_async::KeyColumn;
    use xbbg_async::engine::{OverflowPolicy, ServerAddr, Transport, ValidationMode};
    use xbbg_async::sdk_logging::SdkLogLevel;
    use xbbg_core::AuthConfig;

    use super::*;

    fn settings(vars: &[(&str, &str)]) -> Result<(EngineConfig, ResultLimits), String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect();
        load(&|key| vars.get(key).cloned())
    }

    fn engine(vars: &[(&str, &str)]) -> EngineConfig {
        settings(vars).expect("settings load").0
    }

    fn rejected(vars: &[(&str, &str)]) -> String {
        match settings(vars) {
            Ok(_) => panic!("settings should be rejected: {vars:?}"),
            Err(message) => message,
        }
    }

    fn servers(config: &EngineConfig) -> Vec<ServerAddr> {
        let Transport::Direct(servers) = &config.transport else {
            panic!("expected direct transport");
        };
        servers.clone()
    }

    #[test]
    fn an_empty_environment_keeps_the_engine_defaults() {
        let (config, limits) = settings(&[]).unwrap();
        let defaults = EngineConfig::default();
        assert_eq!(servers(&config), [ServerAddr::new("localhost", 8194)]);
        assert_eq!(config.request_pool_size, defaults.request_pool_size);
        assert_eq!(config.validation_mode, defaults.validation_mode);
        assert_eq!(config.overflow_policy, defaults.overflow_policy);
        assert_eq!(config.key_column, defaults.key_column);
        assert_eq!(config.sdk_log_level, defaults.sdk_log_level);
        assert_eq!(config.num_start_attempts, defaults.num_start_attempts);
        assert_eq!(
            config.retry_policy.max_retries,
            defaults.retry_policy.max_retries
        );
        assert_eq!(config.warmup_services, defaults.warmup_services);
        assert_eq!(config.field_cache_path, defaults.field_cache_path);
        assert_eq!(config.bql_app_name, None);
        assert_eq!(config.auth, None);
        assert!(config.tls.is_none());
        assert_eq!(limits.max_rows, ResultLimits::default().max_rows);
    }

    #[test]
    fn the_variables_fill_the_engine_configuration() {
        let config = engine(&[
            ("XBBG_MCP_HOST", " a.example.com "),
            ("XBBG_MCP_PORT", "18194"),
            ("XBBG_MCP_BQL_APP_NAME", "synthetic-app"),
            ("XBBG_MCP_REQUEST_POOL_SIZE", "4"),
            ("XBBG_MCP_VALIDATION_MODE", "strict"),
            ("XBBG_MCP_FIELD_CACHE_PATH", "fields.json"),
            ("XBBG_MCP_WARMUP_SERVICES", "//blp/refdata, ,//blp/apiflds"),
            ("XBBG_MCP_SDK_LOG_LEVEL", "debug"),
            ("XBBG_MCP_NUM_START_ATTEMPTS", "5"),
            ("XBBG_MCP_AUTO_RESTART_ON_DISCONNECTION", "off"),
            ("XBBG_MCP_RETRY_MAX_RETRIES", "7"),
            ("XBBG_MCP_RETRY_INITIAL_DELAY_MS", "250"),
            ("XBBG_MCP_RETRY_BACKOFF_FACTOR", "1.5"),
            ("XBBG_MCP_RETRY_MAX_DELAY_MS", "9000"),
            ("XBBG_MCP_OVERFLOW_POLICY", "block"),
            ("XBBG_MCP_KEY_COLUMN", "security"),
        ]);
        assert_eq!(servers(&config), [ServerAddr::new("a.example.com", 18194)]);
        assert_eq!(config.bql_app_name.as_deref(), Some("synthetic-app"));
        assert_eq!(config.request_pool_size, 4);
        assert_eq!(config.validation_mode, ValidationMode::Strict);
        assert_eq!(
            config.field_cache_path.as_deref(),
            Some(std::path::Path::new("fields.json"))
        );
        assert_eq!(config.warmup_services, ["//blp/refdata", "//blp/apiflds"]);
        assert_eq!(config.sdk_log_level, SdkLogLevel::Debug);
        assert_eq!(config.num_start_attempts, 5);
        assert!(!config.auto_restart_on_disconnection);
        assert_eq!(config.retry_policy.max_retries, 7);
        assert_eq!(config.retry_policy.initial_delay_ms, 250);
        assert_eq!(config.retry_policy.backoff_factor, 1.5);
        assert_eq!(config.retry_policy.max_delay_ms, 9000);
        assert_eq!(config.overflow_policy, OverflowPolicy::Block);
        assert_eq!(config.key_column, KeyColumn::Security);
    }

    #[test]
    fn legacy_aliases_apply_and_the_mcp_name_wins() {
        let config = engine(&[
            ("XBBG_SERVER", "b.example.com"),
            ("XBBG_SERVER_PORT", "9000"),
            ("XBBG_MAX_ATTEMPT", "2"),
            ("XBBG_AUTO_RESTART", "false"),
        ]);
        assert_eq!(servers(&config), [ServerAddr::new("b.example.com", 9000)]);
        assert_eq!(config.num_start_attempts, 2);
        assert!(!config.auto_restart_on_disconnection);

        let config = engine(&[
            ("XBBG_MCP_HOST", "a.example.com"),
            ("XBBG_HOST", "b.example.com"),
        ]);
        assert_eq!(servers(&config), [ServerAddr::new("a.example.com", 8194)]);
    }

    #[test]
    fn blank_values_count_as_unset_except_an_empty_warmup_list() {
        let config = engine(&[
            ("XBBG_MCP_HOST", "  "),
            ("XBBG_MCP_REQUEST_POOL_SIZE", ""),
            ("XBBG_MCP_WARMUP_SERVICES", ""),
        ]);
        assert_eq!(servers(&config), [ServerAddr::new("localhost", 8194)]);
        assert_eq!(
            config.request_pool_size,
            EngineConfig::default().request_pool_size
        );
        assert!(config.warmup_services.is_empty());
    }

    #[test]
    fn every_auth_method_builds_its_configuration() {
        let auth = |vars: &[(&str, &str)]| engine(vars).auth;
        assert_eq!(auth(&[]), None);
        assert_eq!(auth(&[("XBBG_MCP_AUTH_METHOD", "none")]), None);
        assert_eq!(
            auth(&[("XBBG_MCP_AUTH_METHOD", "USER")]),
            Some(AuthConfig::User)
        );
        assert_eq!(
            auth(&[
                ("XBBG_MCP_AUTH_METHOD", "app"),
                ("XBBG_MCP_APP_NAME", "synthetic-app"),
            ]),
            Some(AuthConfig::App {
                app_name: "synthetic-app".into()
            })
        );
        assert_eq!(
            auth(&[
                ("XBBG_AUTH_METHOD", "userapp"),
                ("XBBG_APP_NAME", "synthetic-app"),
            ]),
            Some(AuthConfig::UserApp {
                app_name: "synthetic-app".into()
            })
        );
        for method in ["dir", "directory"] {
            assert_eq!(
                auth(&[
                    ("XBBG_MCP_AUTH_METHOD", method),
                    ("XBBG_MCP_DIR_PROPERTY", "synthetic-property"),
                ]),
                Some(AuthConfig::Directory {
                    property_name: "synthetic-property".into()
                })
            );
        }
        assert_eq!(
            auth(&[
                ("XBBG_MCP_AUTH_METHOD", "manual"),
                ("XBBG_MCP_APP_NAME", "synthetic-app"),
                ("XBBG_MCP_USER_ID", "synthetic-user"),
                ("XBBG_MCP_IP_ADDRESS", "192.0.2.1"),
            ]),
            Some(AuthConfig::Manual {
                app_name: "synthetic-app".into(),
                user_id: "synthetic-user".into(),
                ip_address: "192.0.2.1".into(),
            })
        );
        assert_eq!(
            auth(&[
                ("XBBG_MCP_AUTH_METHOD", "token"),
                ("XBBG_MCP_TOKEN", "synthetic-token"),
            ]),
            Some(AuthConfig::Token {
                token: "synthetic-token".into()
            })
        );
    }

    #[test]
    fn auth_errors_name_the_variables() {
        assert_eq!(
            rejected(&[("XBBG_MCP_APP_NAME", "synthetic-app")]),
            "auth_method is required when auth-specific environment variables are set"
        );
        assert_eq!(
            rejected(&[("XBBG_MCP_AUTH_METHOD", "app")]),
            "invalid configuration: XBBG_MCP_APP_NAME is required for \
             XBBG_MCP_AUTH_METHOD='app'"
        );
        assert_eq!(
            rejected(&[("XBBG_AUTH_METHOD", "token")]),
            "invalid configuration: XBBG_MCP_TOKEN is required for XBBG_AUTH_METHOD='token'"
        );
        assert_eq!(
            rejected(&[("XBBG_MCP_AUTH_METHOD", "kerberos")]),
            "invalid configuration: Invalid XBBG_MCP_AUTH_METHOD: kerberos. Must be one of \
             ['none', 'user', 'app', 'userapp', 'dir', 'directory', 'manual', 'token']"
        );
    }

    #[test]
    fn engine_limits_are_checked_at_startup_in_terms_of_variables() {
        assert_eq!(
            rejected(&[("XBBG_MCP_REQUEST_POOL_SIZE", "0")]),
            "invalid configuration: XBBG_MCP_REQUEST_POOL_SIZE must be greater than zero"
        );
        assert_eq!(
            rejected(&[("XBBG_REQUEST_POOL_SIZE", "0")]),
            "invalid configuration: XBBG_REQUEST_POOL_SIZE must be greater than zero"
        );
    }

    #[test]
    fn enum_values_are_rejected_with_the_variable_that_set_them() {
        for (key, needle) in [
            ("XBBG_MCP_VALIDATION_MODE", "XBBG_MCP_VALIDATION_MODE"),
            ("XBBG_OVERFLOW_POLICY", "XBBG_OVERFLOW_POLICY"),
            ("XBBG_MCP_KEY_COLUMN", "XBBG_MCP_KEY_COLUMN"),
            ("XBBG_SDK_LOG_LEVEL", "XBBG_SDK_LOG_LEVEL"),
        ] {
            let message = rejected(&[(key, "synthetic-invalid")]);
            assert!(
                message.starts_with(&format!("invalid {needle}: ")),
                "{message}"
            );
            assert!(message.contains("synthetic-invalid"), "{message}");
        }
    }

    #[test]
    fn numbers_that_do_not_parse_name_their_variable() {
        assert!(
            rejected(&[("XBBG_MCP_PORT", "70000")]).starts_with("invalid u16 in XBBG_MCP_PORT: ")
        );
        assert!(
            rejected(&[("XBBG_NUM_START_ATTEMPTS", "many")])
                .starts_with("invalid u32 in XBBG_NUM_START_ATTEMPTS: ")
        );
        assert!(
            rejected(&[("XBBG_MCP_RETRY_INITIAL_DELAY_MS", "-1")])
                .starts_with("invalid u64 in XBBG_MCP_RETRY_INITIAL_DELAY_MS: ")
        );
        assert!(
            rejected(&[("XBBG_MCP_AUTO_RESTART_ON_DISCONNECTION", "maybe")])
                .starts_with("invalid bool in XBBG_MCP_AUTO_RESTART_ON_DISCONNECTION: ")
        );
    }

    #[test]
    fn result_limits_keep_their_minimums() {
        let (_, limits) = settings(&[("XBBG_MCP_MAX_ROWS", "7")]).unwrap();
        assert_eq!(limits.max_rows, 7);
        assert_eq!(
            rejected(&[("XBBG_MCP_MAX_ROWS", "0")]),
            "max_rows must be at least 1, got 0"
        );
        assert_eq!(
            rejected(&[("XBBG_MCP_MAX_STRING_BYTES", "2")]),
            "max_string_bytes must be at least 3, got 2"
        );
        assert!(
            rejected(&[("XBBG_MCP_MAX_CELLS", "lots")])
                .starts_with("invalid usize in XBBG_MCP_MAX_CELLS: ")
        );
    }
}
