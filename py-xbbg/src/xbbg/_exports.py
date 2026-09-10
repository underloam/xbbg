from __future__ import annotations

SDK_EXPORTS = (
    "get_sdk_info",
    "set_sdk_path",
    "clear_sdk_path",
)

CORE_EXPORTS = (
    "ArrowTable",
    "ArrowRecordBatch",
    "ArrowSchema",
    "ArrowField",
    "set_log_level",
    "get_log_level",
    "enable_sdk_logging",
)

SERVICE_EXPORTS = (
    "Service",
    "Operation",
    "OutputMode",
    "RequestParams",
    "ExtractorHint",
)

FIELD_CACHE_EXPORTS = (
    "FieldTypeCache",
    "FieldInfo",
    "resolve_field_types",
    "aresolve_field_types",
    "cache_field_types",
    "get_field_info",
    "get_field_cache_stats",
    "clear_field_cache",
)

SCHEMA_EXPORTS = (
    "get_schema",
    "aget_schema",
    "get_operation",
    "aget_operation",
    "list_operations",
    "alist_operations",
    "get_enum_values",
    "aget_enum_values",
    "list_valid_elements",
    "alist_valid_elements",
    "generate_stubs",
    "configure_ide_stubs",
    "ServiceSchema",
    "OperationSchema",
)

SCHEMA_LOOKUP_EXPORTS = (*SCHEMA_EXPORTS, "ElementInfo")

EXCEPTION_EXPORTS = (
    "BlpError",
    "BlpSessionError",
    "BlpRequestError",
    "BlpSubscriptionDataLossError",
    "BlpLimitError",
    "BlpSecurityError",
    "BlpFieldError",
    "BlpValidationError",
    "BlpTimeoutError",
    "BlpInternalError",
    "BlpBPipeError",
)

MODULE_EXPORTS = (
    "ext",
    "markets",
    "testing",
)

BACKEND_EXPORTS = (
    "is_backend_available",
    "check_backend",
    "get_available_backends",
    "print_backend_status",
    "is_format_supported",
    "get_supported_formats",
    "check_format_compatibility",
    "validate_backend_format",
)

PACKAGE_GENERIC_EXPORTS = (
    "arequest",
    "request",
    "aseat_type",
    "seat_type",
    "acheck_entitlements",
    "check_entitlements",
    "aidentity_is_authorized",
    "identity_is_authorized",
)

PACKAGE_SYNC_EXPORTS = (
    "bdp",
    "bds",
    "bdh",
    "bdib",
    "bdtick",
    "bql",
    "bsrch",
    "bqr",
    "bflds",
    "beqs",
    "blkp",
    "bport",
    "bcurves",
    "bgovts",
)

PACKAGE_ASYNC_EXPORTS = (
    "abdp",
    "abds",
    "abdh",
    "abdib",
    "abdtick",
    "abql",
    "absrch",
    "abqr",
    "abflds",
    "abeqs",
    "ablkp",
    "abport",
    "abcurves",
    "abgovts",
)

PACKAGE_STREAMING_EXPORTS = (
    "Tick",
    "Subscription",
    "asubscribe",
    "subscribe",
    "astream",
    "stream",
    "avwap",
    "vwap",
    "amktbar",
    "mktbar",
    "adepth",
    "depth",
    "achains",
    "chains",
)

PACKAGE_TA_EXPORTS = (
    "abta",
    "bta",
    "ta_studies",
    "ta_study_params",
    "generate_ta_stubs",
)

PACKAGE_CONFIG_EXPORTS = (
    "configure",
    "set_backend",
    "get_backend",
)

PACKAGE_OVERRIDE_EXPORTS = (
    "OverrideSpec",
    "ovr",
)

PACKAGE_MIDDLEWARE_EXPORTS = (
    "RequestEnvironment",
    "RequestContext",
    "add_middleware",
    "remove_middleware",
    "clear_middleware",
    "get_middleware",
    "set_middleware",
)

PACKAGE_LIFECYCLE_EXPORTS = (
    "shutdown",
    "reset",
    "is_connected",
)

PACKAGE_BLP_SCHEMA_EXPORTS = (
    "bops",
    "abops",
    "bschema",
    "abschema",
)

PACKAGE_BLP_EXPORTS = (
    "Backend",
    *PACKAGE_GENERIC_EXPORTS,
    *PACKAGE_SYNC_EXPORTS,
    *PACKAGE_ASYNC_EXPORTS,
    *PACKAGE_STREAMING_EXPORTS,
    *PACKAGE_TA_EXPORTS,
    *PACKAGE_CONFIG_EXPORTS,
    *PACKAGE_OVERRIDE_EXPORTS,
    *PACKAGE_MIDDLEWARE_EXPORTS,
    *PACKAGE_LIFECYCLE_EXPORTS,
    *PACKAGE_BLP_SCHEMA_EXPORTS,
)

PACKAGE_EXPORTS = (
    "__version__",
    "_core",
    "Backend",
    "Engine",
    "EngineConfig",
    *PACKAGE_GENERIC_EXPORTS,
    *PACKAGE_SYNC_EXPORTS,
    *PACKAGE_ASYNC_EXPORTS,
    *PACKAGE_STREAMING_EXPORTS,
    *PACKAGE_TA_EXPORTS,
    *PACKAGE_CONFIG_EXPORTS,
    *PACKAGE_OVERRIDE_EXPORTS,
    *PACKAGE_MIDDLEWARE_EXPORTS,
    *PACKAGE_LIFECYCLE_EXPORTS,
    *CORE_EXPORTS,
    *PACKAGE_BLP_SCHEMA_EXPORTS,
    *SDK_EXPORTS,
    *FIELD_CACHE_EXPORTS,
    *SERVICE_EXPORTS,
    *SCHEMA_EXPORTS,
    *EXCEPTION_EXPORTS,
    *MODULE_EXPORTS,
    *BACKEND_EXPORTS,
)

BLP_MODULE_EXPORTS = (
    "Backend",
    "arequest",
    "request",
    "aseat_type",
    "seat_type",
    "acheck_entitlements",
    "check_entitlements",
    "aidentity_is_authorized",
    "identity_is_authorized",
    "abdp",
    "abdh",
    "abds",
    "abdib",
    "abdtick",
    "abql",
    "absrch",
    "abeqs",
    "ablkp",
    "abport",
    "abcurves",
    "abgovts",
    "abqr",
    "bqr",
    "abflds",
    "bflds",
    "abfld",
    "bfld",
    "afieldInfo",
    "fieldInfo",
    "afieldSearch",
    "fieldSearch",
    "bdp",
    "bdh",
    "bds",
    "bdib",
    "bdtick",
    "bql",
    "bsrch",
    "beqs",
    "blkp",
    "bport",
    "bcurves",
    "bgovts",
    "Tick",
    "Subscription",
    "asubscribe",
    "subscribe",
    "astream",
    "stream",
    "avwap",
    "vwap",
    "amktbar",
    "mktbar",
    "adepth",
    "depth",
    "achains",
    "chains",
    "abta",
    "bta",
    "ta_studies",
    "ta_study_params",
    "generate_ta_stubs",
    "configure",
    "set_backend",
    "get_backend",
    "OverrideSpec",
    "ovr",
    "RequestEnvironment",
    "RequestContext",
    "add_middleware",
    "remove_middleware",
    "clear_middleware",
    "get_middleware",
    "set_middleware",
    "Service",
    "Operation",
    "OutputMode",
    "RequestParams",
    "ExtractorHint",
    "abops",
    "bops",
    "abschema",
    "bschema",
)
