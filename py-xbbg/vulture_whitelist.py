"""Static-only inventory of names reached through xbbg's dynamic interfaces.

Vulture parses this file; it must not import xbbg or optional dependencies.
Keep entries tied to the named consumer, rather than excluding whole modules.
"""

from __future__ import annotations

from typing import Any

_dynamic: Any = None

# PEP 562 lazy exports and public helpers resolved from _exports by getattr.
_ = (
    _dynamic.__getattr__,
    _dynamic.__dir__,
    _dynamic.get_available_backends,
    _dynamic.print_backend_status,
    _dynamic.clear_sdk_path,
    _dynamic.generate_stubs,
    _dynamic.generate_ta_stubs,
    _dynamic.Tick,
    _dynamic.clear_cache,
)

# Narwhals discovers these through the narwhals.plugins entry point/protocol.
_ = (
    _dynamic.NATIVE_PACKAGE,
    _dynamic.is_native,
    _dynamic.__narwhals_namespace__,
    _dynamic.to_narwhals,
    _dynamic._with_version,
    _dynamic.clone,
    _dynamic._expr,
    _dynamic.drop,
)

# Introspection metadata on generated sync wrappers and Enum conversion hooks.
_ = (_dynamic.__qualname__, _dynamic.__signature__, _dynamic._missing_)

# Endpoint templates preserve typed signatures and pass locals() to plan builders.
_ = (
    _dynamic.expression,
    _dynamic.include_spread_price,
    _dynamic.include_yield,
    _dynamic.include_condition_codes,
    _dynamic.include_exchange_codes,
    _dynamic.screen,
    _dynamic.screen_type,
    _dynamic.yellowkey,
    _dynamic.language,
    _dynamic.max_results,
    _dynamic.portfolio,
    _dynamic.country,
    _dynamic.currency,
    _dynamic.curve_type,
    _dynamic.subtype,
    _dynamic.curveid,
    _dynamic.bbgid,
    _dynamic.partial_match,
)

# Public Enum vocabulary selected by callers rather than the package itself.
_ = (
    _dynamic.IMBALANCE,
    _dynamic.INDICATIVE,
    _dynamic.STATE,
    _dynamic.HALTS,
    _dynamic.RESULTS,
    _dynamic.COMPOSITE,
    _dynamic.WEEKLY,
    _dynamic.MONTHLY,
    _dynamic.QUARTERLY,
    _dynamic.YEARLY,
    _dynamic.ALL,
    _dynamic.ATM,
    _dynamic.AMERICAN,
    _dynamic.EUROPEAN,
    _dynamic.EXACT,
    _dynamic.CLOSEST,
    _dynamic.DELTA_1M_2M,
    _dynamic.MONEYNESS_60D,
    _dynamic.MONEYNESS_3M,
    _dynamic.MONEYNESS_6M,
    _dynamic.MONEYNESS_12M,
)

# Public dataclass fields consumed by callers and request middleware.
_ = (_dynamic.field_id, _dynamic.arrow_type, _dynamic.started_at, _dynamic.power)

# pytest discovers hooks, markers, and fixture injection by name.
_ = (
    _dynamic.pytest_configure,
    _dynamic.pytest_collection_modifyitems,
    _dynamic.pytestmark,
    _dynamic.stub_engine,
    _dynamic.arrow_endpoint,
)

# unittest.mock reads configured exceptions/callables via its side_effect property.
_ = _dynamic.side_effect

# Fake native engine configuration mirrors PyEngineConfig's dynamic attributes.
_ = (
    _dynamic.request_pool_size,
    _dynamic.subscription_pool_size,
    _dynamic.subscription_flush_threshold,
    _dynamic.max_event_queue_size,
    _dynamic.command_queue_size,
    _dynamic.subscription_stream_capacity,
    _dynamic.warmup_services,
    _dynamic.field_cache_path,
    _dynamic.dir_property,
)

# Dataframe metadata is inspected with getattr; synthetic modules use __path__.
_ = (_dynamic.eid_data, _dynamic.security_errors, _dynamic.field_exceptions, _dynamic.__path__)
