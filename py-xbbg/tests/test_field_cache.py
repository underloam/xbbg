from __future__ import annotations

import asyncio
from unittest import TestCase
import warnings

import pytest

import xbbg._engine as engine_module

_CASE = TestCase()


class FakeEngine:
    def field_cache_stats(self) -> dict[str, int | str]:
        return {
            "entry_count": 7,
            "cache_path": "C:/tmp/xbbg/field_cache.json",
        }

    async def resolve_field_types(
        self,
        fields: list[str],
        overrides: dict[str, str] | None,
        default_type: str,
    ) -> dict[str, str]:
        resolved = dict.fromkeys(fields, default_type)
        if overrides:
            resolved.update(overrides)
        return resolved


def test_get_field_cache_stats_export(monkeypatch):
    """The package root should export field cache stats with cache_path."""
    from xbbg import get_field_cache_stats

    monkeypatch.setattr(engine_module, "_get_engine", lambda: FakeEngine())

    _CASE.assertEqual(
        get_field_cache_stats(),
        {
            "entry_count": 7,
            "cache_path": "C:/tmp/xbbg/field_cache.json",
        },
    )


def test_field_type_cache_exposes_cache_path(monkeypatch):
    """FieldTypeCache should surface the resolved cache path."""
    from xbbg import FieldTypeCache

    monkeypatch.setattr(engine_module, "_get_engine", lambda: FakeEngine())

    cache = FieldTypeCache()

    _CASE.assertEqual(
        cache.stats,
        {
            "entry_count": 7,
            "cache_path": "C:/tmp/xbbg/field_cache.json",
        },
    )
    _CASE.assertEqual(cache.cache_path, "C:/tmp/xbbg/field_cache.json")


def test_default_field_cache_calls_do_not_warn(monkeypatch):
    from xbbg import FieldTypeCache, resolve_field_types

    monkeypatch.setattr(engine_module, "_get_engine", lambda: FakeEngine())

    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        cache = FieldTypeCache()
        _CASE.assertEqual(cache.resolve_types(["PX_LAST"]), {"PX_LAST": "string"})
        _CASE.assertEqual(resolve_field_types(["PX_LAST"]), {"PX_LAST": "string"})

    _CASE.assertEqual(recorded, [])


def test_query_api_argument_is_rejected():
    from xbbg import FieldTypeCache

    with pytest.raises(TypeError, match="query_api"):
        FieldTypeCache().resolve_types(["PX_LAST"], query_api=False)


def test_async_query_api_argument_is_rejected():
    from xbbg import FieldTypeCache

    with pytest.raises(TypeError, match="query_api"):
        FieldTypeCache().aresolve_types(["NAME"], query_api=False)


def test_module_async_query_api_argument_is_rejected():
    from xbbg import aresolve_field_types

    with pytest.raises(TypeError, match="query_api"):
        aresolve_field_types(["NAME"], query_api=False)


def test_unknown_field_cache_kwargs_are_rejected():
    from xbbg import FieldTypeCache

    with pytest.raises(TypeError, match="ignored"):
        FieldTypeCache().resolve_types(["PX_LAST"], ignored="compat")


def test_constructor_cache_path_is_rejected():
    from xbbg import FieldTypeCache

    with pytest.raises(TypeError):
        FieldTypeCache(cache_path="ignored.json")


def test_schema_invalidation_clears_schema_parse_and_blp_field_type_caches(monkeypatch):
    from xbbg import _engine
    import xbbg.schema as schema_module

    class FakeEngine:
        def __init__(self):
            self.invalidated = []
            self.clear_calls = 0
            self.resolve_calls = []

        def invalidate_schema(self, service: str) -> None:
            self.invalidated.append(service)

        def clear_schema_cache(self) -> None:
            self.clear_calls += 1

        async def resolve_field_types(self, fields, overrides, default_type):
            self.resolve_calls.append((list(fields), overrides, default_type))
            return dict.fromkeys(fields, default_type)

    fake_engine = FakeEngine()
    json_schema = '{"service":"//blp/refdata","description":"refdata","operations":[],"cached_at":"1"}'
    monkeypatch.setattr(_engine, "_get_engine", lambda: fake_engine)
    schema_module._parse_service_schema_json.cache_clear()
    _engine._clear_field_type_resolution_cache()

    try:
        parsed_before = schema_module.ServiceSchema.from_json(json_schema)
        _CASE.assertIs(schema_module.ServiceSchema.from_json(json_schema), parsed_before)
        _CASE.assertEqual(
            asyncio.run(_engine._resolve_field_types_cached(["PX_LAST"], None, "string")),
            {"PX_LAST": "string"},
        )
        _CASE.assertEqual(
            asyncio.run(_engine._resolve_field_types_cached(["PX_LAST"], None, "string")),
            {"PX_LAST": "string"},
        )
        _CASE.assertEqual(len(fake_engine.resolve_calls), 1)

        schema_module.invalidate_schema("//blp/refdata")

        parsed_after_invalidate = schema_module.ServiceSchema.from_json(json_schema)
        _CASE.assertIsNot(parsed_after_invalidate, parsed_before)
        _CASE.assertEqual(
            asyncio.run(_engine._resolve_field_types_cached(["PX_LAST"], None, "string")),
            {"PX_LAST": "string"},
        )
        _CASE.assertEqual(len(fake_engine.resolve_calls), 2)

        schema_module.clear_schema_cache()

        parsed_after_clear = schema_module.ServiceSchema.from_json(json_schema)
        _CASE.assertIsNot(parsed_after_clear, parsed_after_invalidate)
        _CASE.assertEqual(
            asyncio.run(_engine._resolve_field_types_cached(["PX_LAST"], None, "string")),
            {"PX_LAST": "string"},
        )
        _CASE.assertEqual(len(fake_engine.resolve_calls), 3)
    finally:
        schema_module._parse_service_schema_json.cache_clear()
        _engine._clear_field_type_resolution_cache()

    _CASE.assertEqual(fake_engine.invalidated, ["//blp/refdata"])
    _CASE.assertEqual(fake_engine.clear_calls, 1)
