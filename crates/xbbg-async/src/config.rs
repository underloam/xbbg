//! Shared normalization for host-extracted engine configuration.
//!
//! Adapters retain their public input shapes and numeric extraction. Session
//! options and engine invariants are checked here before starting any workers.

use crate::BlpAsyncError;
use crate::engine::{EngineConfig, ServerAddr, Socks5Proxy, TlsConfig, Transport};
use xbbg_core::AuthConfig;

/// Borrowed authentication fields, before selecting an authentication method.
#[derive(Default)]
pub struct AuthInput<'a> {
    pub method: Option<&'a str>,
    pub app_name: Option<&'a str>,
    pub dir_property: Option<&'a str>,
    pub user_id: Option<&'a str>,
    pub ip_address: Option<&'a str>,
    pub token: Option<&'a str>,
}

/// Borrowed TLS material. Timeouts are validated even without credentials.
#[derive(Default)]
pub struct TlsInput<'a> {
    pub client_credentials: Option<&'a str>,
    pub client_credentials_password: Option<&'a str>,
    pub trust_material: Option<&'a str>,
    pub handshake_timeout_ms: Option<i32>,
    pub crl_fetch_timeout_ms: Option<i32>,
}

/// Transport fields whose presence is determined by the host adapter.
pub struct TransportInput<'a> {
    pub host: &'a str,
    pub port: u16,
    /// Python detects non-default values; JavaScript can detect explicit keys.
    pub explicit_host_port: bool,
    pub zfp_remote: Option<&'a str>,
    pub socks5_host: Option<&'a str>,
    pub socks5_port: Option<u16>,
}

impl Default for TransportInput<'_> {
    fn default() -> Self {
        Self {
            host: "localhost",
            port: 8194,
            explicit_host_port: false,
            zfp_remote: None,
            socks5_host: None,
            socks5_port: None,
        }
    }
}

/// Fields that need normalization in addition to the extracted `EngineConfig`.
#[derive(Default)]
pub struct ConfigInput<'a> {
    pub auth: AuthInput<'a>,
    pub tls: TlsInput<'a>,
    pub transport: TransportInput<'a>,
    /// Preserve JavaScript's precision until after checking the domain range.
    pub slow_consumer_hi_water_mark: Option<f64>,
    pub slow_consumer_lo_water_mark: Option<f64>,
}

/// Finish an extracted configuration, replacing its auth, TLS, transport and
/// watermarks with normalized input. `field_name` preserves host-facing labels.
/// Servers are borrowed and copied only into the final transport allocation.
pub fn normalize<'a>(
    mut config: EngineConfig,
    input: ConfigInput<'a>,
    servers: impl ExactSizeIterator<Item = (&'a str, u16)>,
    field_name: fn(&str) -> &str,
) -> Result<EngineConfig, BlpAsyncError> {
    if config.subscription_flush_threshold == 0 {
        return Err(config_error(format!(
            "{} must be greater than zero",
            field_name("subscription_flush_threshold")
        )));
    }
    for (value, field) in [
        (config.keep_alive_inactivity_ms, "keep_alive_inactivity_ms"),
        (
            config.keep_alive_response_timeout_ms,
            "keep_alive_response_timeout_ms",
        ),
    ] {
        if value.is_some_and(|value| value < 0) {
            return Err(config_error(format!(
                "{} must be non-negative",
                field_name(field)
            )));
        }
    }
    config.slow_consumer_hi_water_mark = normalize_watermark(
        input.slow_consumer_hi_water_mark,
        field_name("slow_consumer_hi_water_mark"),
        true,
    )?;
    config.slow_consumer_lo_water_mark = normalize_watermark(
        input.slow_consumer_lo_water_mark,
        field_name("slow_consumer_lo_water_mark"),
        false,
    )?;
    config.auth = normalize_auth(input.auth, field_name)?;
    config.transport = normalize_transport(input.transport, servers, field_name)?;
    config.tls = normalize_tls(input.tls, field_name)?;
    config
        .validate()
        .map_err(|error| label_engine_error(error, field_name))?;
    Ok(config)
}

fn config_error(detail: String) -> BlpAsyncError {
    BlpAsyncError::ConfigError { detail }
}

// EngineConfig::validate owns the resource invariants. Its diagnostics contain
// field identifiers, not user values, so only those tokens need host labels.
fn label_engine_error(error: BlpAsyncError, field_name: fn(&str) -> &str) -> BlpAsyncError {
    let BlpAsyncError::ConfigError { detail } = error else {
        return error;
    };
    if !detail.split(' ').any(|word| field_name(word) != word) {
        return config_error(detail);
    }
    let mut labeled = String::with_capacity(detail.len());
    for (index, word) in detail.split(' ').enumerate() {
        if index != 0 {
            labeled.push(' ');
        }
        labeled.push_str(field_name(word));
    }
    config_error(labeled)
}

fn normalize_auth(
    input: AuthInput<'_>,
    field_name: fn(&str) -> &str,
) -> Result<Option<AuthConfig>, BlpAsyncError> {
    let Some(method) = input.method else {
        if input.app_name.is_some()
            || input.dir_property.is_some()
            || input.user_id.is_some()
            || input.ip_address.is_some()
            || input.token.is_some()
        {
            return Err(config_error(format!(
                "{} is required when auth-specific fields are provided",
                field_name("auth_method")
            )));
        }
        return Ok(None);
    };
    let method = method.trim().to_ascii_lowercase();
    let required = |value: Option<&str>, field: &str| {
        value
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                config_error(format!(
                    "{} is required for {}='{method}'",
                    field_name(field),
                    field_name("auth_method")
                ))
            })
    };
    let auth = match method.as_str() {
        "" | "none" => None,
        "user" => Some(AuthConfig::User),
        "app" => Some(AuthConfig::App {
            app_name: required(input.app_name, "app_name")?,
        }),
        "userapp" => Some(AuthConfig::UserApp {
            app_name: required(input.app_name, "app_name")?,
        }),
        "dir" | "directory" => Some(AuthConfig::Directory {
            property_name: required(input.dir_property, "dir_property")?,
        }),
        "manual" => Some(AuthConfig::Manual {
            app_name: required(input.app_name, "app_name")?,
            user_id: required(input.user_id, "user_id")?,
            ip_address: required(input.ip_address, "ip_address")?,
        }),
        "token" => Some(AuthConfig::Token {
            token: required(input.token, "token")?,
        }),
        other => {
            return Err(config_error(format!(
                "Invalid {}: {other}. Must be one of ['none', 'user', 'app', 'userapp', 'dir', 'directory', 'manual', 'token']",
                field_name("auth_method")
            )));
        }
    };
    Ok(auth)
}

fn normalize_tls(
    input: TlsInput<'_>,
    field_name: fn(&str) -> &str,
) -> Result<Option<TlsConfig>, BlpAsyncError> {
    for (value, field) in [
        (input.handshake_timeout_ms, "tls_handshake_timeout_ms"),
        (input.crl_fetch_timeout_ms, "tls_crl_fetch_timeout_ms"),
    ] {
        if value.is_some_and(|value| value < 0) {
            return Err(config_error(format!(
                "{} must be a non-negative integer number of milliseconds",
                field_name(field)
            )));
        }
    }
    match (input.client_credentials, input.trust_material) {
        (None, None) => Ok(None),
        (Some(credentials), Some(trust)) => Ok(Some(TlsConfig {
            client_credentials: credentials.to_owned(),
            client_credentials_password: input
                .client_credentials_password
                .unwrap_or_default()
                .to_owned(),
            trust_material: trust.to_owned(),
            handshake_timeout_ms: input.handshake_timeout_ms,
            crl_fetch_timeout_ms: input.crl_fetch_timeout_ms,
        })),
        (Some(_), None) => Err(config_error(format!(
            "{} set without {}",
            field_name("tls_client_credentials"),
            field_name("tls_trust_material")
        ))),
        (None, Some(_)) => Err(config_error(format!(
            "{} set without {}",
            field_name("tls_trust_material"),
            field_name("tls_client_credentials")
        ))),
    }
}

fn normalize_transport<'a>(
    input: TransportInput<'_>,
    servers: impl ExactSizeIterator<Item = (&'a str, u16)>,
    field_name: fn(&str) -> &str,
) -> Result<Transport, BlpAsyncError> {
    let remote = input
        .zfp_remote
        .map(str::parse)
        .transpose()
        .map_err(config_error)?;
    let proxy = match (input.socks5_host, input.socks5_port) {
        (Some(host), Some(port)) => Some(Socks5Proxy {
            host: host.to_owned(),
            port,
        }),
        (Some(_), None) => {
            return Err(config_error(format!(
                "{} set without {}",
                field_name("socks5_host"),
                field_name("socks5_port")
            )));
        }
        (None, Some(_)) => {
            return Err(config_error(format!(
                "{} set without {}",
                field_name("socks5_port"),
                field_name("socks5_host")
            )));
        }
        (None, None) => None,
    };
    if let Some(remote) = remote {
        if servers.len() != 0 || input.explicit_host_port {
            return Err(config_error(format!(
                "{} cannot be combined with host/port/servers — \
                 ZFP supplies Bloomberg endpoints via the leased-line path",
                field_name("zfp_remote")
            )));
        }
        if proxy.is_some() {
            return Err(config_error(format!(
                "{} cannot be combined with {}",
                field_name("zfp_remote"),
                field_name("socks5_host/socks5_port")
            )));
        }
        return Ok(Transport::Zfp(remote));
    }
    let servers = if servers.len() == 0 {
        vec![ServerAddr {
            host: input.host.to_owned(),
            port: input.port,
            proxy,
        }]
    } else {
        servers
            .map(|(host, port)| ServerAddr {
                host: host.to_owned(),
                port,
                proxy: proxy.clone(),
            })
            .collect()
    };
    Ok(Transport::Direct(servers))
}

fn normalize_watermark(
    value: Option<f64>,
    field: &str,
    inclusive_high: bool,
) -> Result<Option<f32>, BlpAsyncError> {
    let Some(value) = value else {
        return Ok(None);
    };
    let in_range = if inclusive_high {
        (0.0..=1.0).contains(&value)
    } else {
        (0.0..1.0).contains(&value)
    };
    if !in_range {
        let range = if inclusive_high {
            "0.0..=1.0"
        } else {
            "0.0..1.0"
        };
        return Err(config_error(format!("{field} must be in {range}")));
    }
    Ok(Some(value as f32))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn convert(input: ConfigInput<'_>) -> Result<EngineConfig, BlpAsyncError> {
        normalize(
            EngineConfig::default(),
            input,
            std::iter::empty(),
            |field| field,
        )
    }

    fn error_detail(result: Result<EngineConfig, BlpAsyncError>) -> String {
        match result {
            Err(BlpAsyncError::ConfigError { detail }) => detail,
            Err(error) => panic!("expected configuration error, got {error}"),
            Ok(_) => panic!("expected invalid configuration"),
        }
    }

    #[test]
    fn defaults_preserve_direct_transport_and_optional_session_settings() {
        let config = convert(ConfigInput::default()).unwrap();
        let Transport::Direct(servers) = config.transport else {
            panic!("expected direct transport");
        };
        assert_eq!(servers, [ServerAddr::new("localhost", 8194)]);
        assert_eq!(config.auth, None);
        assert!(config.tls.is_none());
        assert_eq!(config.slow_consumer_hi_water_mark, None);
        assert_eq!(config.slow_consumer_lo_water_mark, None);
    }

    #[test]
    fn auth_normalizes_methods_and_preserves_selected_values() {
        for (method, expected) in [
            ("", None),
            (" NoNe ", None),
            (" USER ", Some(AuthConfig::User)),
            (
                "app",
                Some(AuthConfig::App {
                    app_name: "synthetic-app".into(),
                }),
            ),
            (
                "userapp",
                Some(AuthConfig::UserApp {
                    app_name: "synthetic-app".into(),
                }),
            ),
            (
                "dir",
                Some(AuthConfig::Directory {
                    property_name: "synthetic-property".into(),
                }),
            ),
            (
                " DIRECTORY ",
                Some(AuthConfig::Directory {
                    property_name: "synthetic-property".into(),
                }),
            ),
            (
                "manual",
                Some(AuthConfig::Manual {
                    app_name: "synthetic-app".into(),
                    user_id: "synthetic-user".into(),
                    ip_address: "192.0.2.1".into(),
                }),
            ),
            (
                "token",
                Some(AuthConfig::Token {
                    token: "synthetic-token".into(),
                }),
            ),
        ] {
            let config = convert(ConfigInput {
                auth: AuthInput {
                    method: Some(method),
                    app_name: Some("synthetic-app"),
                    dir_property: Some("synthetic-property"),
                    user_id: Some("synthetic-user"),
                    ip_address: Some("192.0.2.1"),
                    token: Some("synthetic-token"),
                },
                ..ConfigInput::default()
            })
            .unwrap();
            assert_eq!(config.auth, expected, "{method}");
        }
    }

    #[test]
    fn auth_requires_a_method_and_nonempty_method_fields() {
        let detail = error_detail(convert(ConfigInput {
            auth: AuthInput {
                app_name: Some("synthetic-app"),
                ..AuthInput::default()
            },
            ..ConfigInput::default()
        }));
        assert_eq!(
            detail,
            "auth_method is required when auth-specific fields are provided"
        );
        for (method, field) in [
            ("app", "app_name"),
            ("userapp", "app_name"),
            ("dir", "dir_property"),
            ("directory", "dir_property"),
            ("manual", "app_name"),
            ("token", "token"),
        ] {
            for value in [None, Some("")] {
                let detail = error_detail(convert(ConfigInput {
                    auth: AuthInput {
                        method: Some(method),
                        app_name: value,
                        dir_property: value,
                        token: value,
                        ..AuthInput::default()
                    },
                    ..ConfigInput::default()
                }));
                assert_eq!(
                    detail,
                    format!("{field} is required for auth_method='{method}'")
                );
            }
        }
        for (user_id, ip_address, field) in [
            (None, Some("192.0.2.1"), "user_id"),
            (Some("synthetic-user"), None, "ip_address"),
        ] {
            let detail = error_detail(convert(ConfigInput {
                auth: AuthInput {
                    method: Some("manual"),
                    app_name: Some("synthetic-app"),
                    user_id,
                    ip_address,
                    ..AuthInput::default()
                },
                ..ConfigInput::default()
            }));
            assert!(detail.starts_with(field));
        }
        let detail = error_detail(convert(ConfigInput {
            auth: AuthInput {
                method: Some("unsupported"),
                ..AuthInput::default()
            },
            ..ConfigInput::default()
        }));
        assert!(detail.starts_with("Invalid auth_method: unsupported."));
    }

    #[test]
    fn tls_rejects_negative_timeouts_with_or_without_material() {
        for credentials in [None, Some("fixtures/client.p12")] {
            for trust in [None, Some("fixtures/trust.p7")] {
                for (handshake, crl, field) in [
                    (Some(-1), None, "tls_handshake_timeout_ms"),
                    (None, Some(-1), "tls_crl_fetch_timeout_ms"),
                ] {
                    let detail = error_detail(convert(ConfigInput {
                        tls: TlsInput {
                            client_credentials: credentials,
                            trust_material: trust,
                            handshake_timeout_ms: handshake,
                            crl_fetch_timeout_ms: crl,
                            ..TlsInput::default()
                        },
                        ..ConfigInput::default()
                    }));
                    assert_eq!(
                        detail,
                        format!("{field} must be a non-negative integer number of milliseconds")
                    );
                }
            }
        }
    }

    #[test]
    fn tls_requires_paired_material_and_preserves_nonnegative_timeouts() {
        for (credentials, trust, expected) in [
            (
                Some("fixtures/client.p12"),
                None,
                "tls_client_credentials set without tls_trust_material",
            ),
            (
                None,
                Some("fixtures/trust.p7"),
                "tls_trust_material set without tls_client_credentials",
            ),
        ] {
            assert_eq!(
                error_detail(convert(ConfigInput {
                    tls: TlsInput {
                        client_credentials: credentials,
                        trust_material: trust,
                        ..TlsInput::default()
                    },
                    ..ConfigInput::default()
                })),
                expected
            );
        }
        let config = convert(ConfigInput {
            tls: TlsInput {
                client_credentials: Some("fixtures/client.p12"),
                trust_material: Some("fixtures/trust.p7"),
                handshake_timeout_ms: Some(0),
                crl_fetch_timeout_ms: Some(25),
                ..TlsInput::default()
            },
            ..ConfigInput::default()
        })
        .unwrap();
        let tls = config.tls.unwrap();
        assert_eq!(tls.client_credentials, "fixtures/client.p12");
        assert_eq!(tls.trust_material, "fixtures/trust.p7");
        assert_eq!(tls.client_credentials_password, "");
        assert_eq!(tls.handshake_timeout_ms, Some(0));
        assert_eq!(tls.crl_fetch_timeout_ms, Some(25));
        let config = convert(ConfigInput {
            tls: TlsInput {
                handshake_timeout_ms: Some(0),
                crl_fetch_timeout_ms: Some(25),
                ..TlsInput::default()
            },
            ..ConfigInput::default()
        })
        .unwrap();
        assert!(config.tls.is_none());
    }

    #[test]
    fn direct_transport_uses_servers_and_broadcasts_proxy() {
        let config = normalize(
            EngineConfig::default(),
            ConfigInput {
                transport: TransportInput {
                    socks5_host: Some("proxy.invalid"),
                    socks5_port: Some(1080),
                    ..TransportInput::default()
                },
                ..ConfigInput::default()
            },
            [("primary.invalid", 8194), ("secondary.invalid", 8196)].into_iter(),
            |field| field,
        )
        .unwrap();
        let Transport::Direct(servers) = config.transport else {
            panic!("expected direct transport");
        };
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].host, "primary.invalid");
        assert_eq!(servers[1].port, 8196);
        for server in servers {
            assert_eq!(
                server.proxy,
                Some(Socks5Proxy {
                    host: "proxy.invalid".into(),
                    port: 1080
                })
            );
        }
    }

    #[test]
    fn zfp_rejects_direct_endpoints_and_proxy() {
        for remote in ["8194", "8196"] {
            let config = convert(ConfigInput {
                transport: TransportInput {
                    zfp_remote: Some(remote),
                    ..TransportInput::default()
                },
                ..ConfigInput::default()
            })
            .unwrap();
            assert!(
                matches!(config.transport, Transport::Zfp(value) if value.to_string() == remote)
            );
        }
        for (explicit_host_port, socks5_host, socks5_port, expected) in [
            (true, None, None, "host/port/servers"),
            (
                false,
                Some("proxy.invalid"),
                Some(1080),
                "socks5_host/socks5_port",
            ),
        ] {
            let detail = error_detail(convert(ConfigInput {
                transport: TransportInput {
                    zfp_remote: Some("8194"),
                    explicit_host_port,
                    socks5_host,
                    socks5_port,
                    ..TransportInput::default()
                },
                ..ConfigInput::default()
            }));
            assert!(detail.contains(&format!("zfp_remote cannot be combined with {expected}")));
        }
        let detail = error_detail(normalize(
            EngineConfig::default(),
            ConfigInput {
                transport: TransportInput {
                    zfp_remote: Some("8194"),
                    ..TransportInput::default()
                },
                ..ConfigInput::default()
            },
            [("primary.invalid", 8194)].into_iter(),
            |field| field,
        ));
        assert!(detail.starts_with("zfp_remote cannot be combined with host/port/servers"));
        assert!(
            error_detail(convert(ConfigInput {
                transport: TransportInput {
                    zfp_remote: Some("8195"),
                    ..TransportInput::default()
                },
                ..ConfigInput::default()
            }))
            .contains("invalid ZFP remote port")
        );
    }

    #[test]
    fn normalization_reuses_engine_resource_validation_and_host_labels() {
        let config = EngineConfig {
            subscription_pool_size: 4,
            max_subscription_sessions: 3,
            ..EngineConfig::default()
        };
        let expected = config.validate().unwrap_err().to_string();
        let detail = error_detail(normalize(
            config.clone(),
            ConfigInput::default(),
            std::iter::empty(),
            |field| field,
        ));
        assert!(expected.ends_with(&detail));
        let detail = error_detail(normalize(
            config,
            ConfigInput::default(),
            std::iter::empty(),
            |field| match field {
                "max_subscription_sessions" => "maxSubscriptionSessions",
                "subscription_pool_size" => "subscriptionPoolSize",
                other => other,
            },
        ));
        assert_eq!(
            detail,
            "maxSubscriptionSessions must be greater than or equal to subscriptionPoolSize"
        );
        let config = EngineConfig {
            request_pool_size: 0,
            ..EngineConfig::default()
        };
        assert_eq!(
            error_detail(normalize(
                config,
                ConfigInput::default(),
                std::iter::empty(),
                |field| field
            )),
            "request_pool_size must be greater than zero"
        );
        let config = EngineConfig {
            subscription_pool_size: 0,
            ..EngineConfig::default()
        };
        assert!(
            normalize(
                config,
                ConfigInput::default(),
                std::iter::empty(),
                |field| field
            )
            .is_ok()
        );
    }

    #[test]
    fn normalization_rejects_invalid_flush_and_keep_alive_settings() {
        for (flush, inactivity, response, expected) in [
            (
                0,
                None,
                None,
                "subscription_flush_threshold must be greater than zero",
            ),
            (
                1,
                Some(-1),
                None,
                "keep_alive_inactivity_ms must be non-negative",
            ),
            (
                1,
                None,
                Some(-1),
                "keep_alive_response_timeout_ms must be non-negative",
            ),
        ] {
            let config = EngineConfig {
                subscription_flush_threshold: flush,
                keep_alive_inactivity_ms: inactivity,
                keep_alive_response_timeout_ms: response,
                ..EngineConfig::default()
            };
            assert_eq!(
                error_detail(normalize(
                    config,
                    ConfigInput::default(),
                    std::iter::empty(),
                    |field| field,
                )),
                expected
            );
        }
        let config = EngineConfig {
            keep_alive_inactivity_ms: Some(0),
            keep_alive_response_timeout_ms: Some(0),
            ..EngineConfig::default()
        };
        let config = normalize(
            config,
            ConfigInput::default(),
            std::iter::empty(),
            |field| field,
        )
        .unwrap();
        assert_eq!(config.keep_alive_inactivity_ms, Some(0));
        assert_eq!(config.keep_alive_response_timeout_ms, Some(0));
    }

    #[test]
    fn direct_transport_preserves_host_port_and_requires_paired_proxy_fields() {
        let config = convert(ConfigInput {
            transport: TransportInput {
                host: "synthetic.invalid",
                port: 8196,
                explicit_host_port: true,
                ..TransportInput::default()
            },
            ..ConfigInput::default()
        })
        .unwrap();
        let Transport::Direct(servers) = config.transport else {
            panic!("expected direct transport");
        };
        assert_eq!(servers, [ServerAddr::new("synthetic.invalid", 8196)]);
        for (socks5_host, socks5_port, expected) in [
            (
                Some("proxy.invalid"),
                None,
                "socks5_host set without socks5_port",
            ),
            (None, Some(1080), "socks5_port set without socks5_host"),
        ] {
            assert_eq!(
                error_detail(convert(ConfigInput {
                    transport: TransportInput {
                        socks5_host,
                        socks5_port,
                        ..TransportInput::default()
                    },
                    ..ConfigInput::default()
                })),
                expected
            );
        }
    }

    #[test]
    fn watermarks_validate_original_precision_and_sdk_ranges() {
        for value in [-0.1, 1.5, f64::NAN, f64::INFINITY] {
            let detail = error_detail(convert(ConfigInput {
                slow_consumer_hi_water_mark: Some(value),
                ..ConfigInput::default()
            }));
            assert_eq!(detail, "slow_consumer_hi_water_mark must be in 0.0..=1.0");
        }
        for value in [-0.1, 1.0, f64::NAN, f64::INFINITY] {
            let detail = error_detail(convert(ConfigInput {
                slow_consumer_lo_water_mark: Some(value),
                ..ConfigInput::default()
            }));
            assert_eq!(detail, "slow_consumer_lo_water_mark must be in 0.0..1.0");
        }
        let detail = error_detail(convert(ConfigInput {
            slow_consumer_hi_water_mark: Some(1.0 + f64::EPSILON),
            ..ConfigInput::default()
        }));
        assert!(detail.contains("slow_consumer_hi_water_mark"));
        let config = convert(ConfigInput {
            slow_consumer_hi_water_mark: Some(1.0),
            slow_consumer_lo_water_mark: Some(0.0),
            ..ConfigInput::default()
        })
        .unwrap();
        assert_eq!(config.slow_consumer_hi_water_mark, Some(1.0));
        assert_eq!(config.slow_consumer_lo_water_mark, Some(0.0));
    }
}
