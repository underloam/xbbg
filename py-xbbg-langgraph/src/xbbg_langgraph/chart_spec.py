"""Build inline Vega-Lite specifications without fetching data or loading renderers."""

from __future__ import annotations

from datetime import date
import json
import math
import re
from typing import Any

from pydantic import BaseModel

Row = dict[str, str | int | float | bool | None]
_SPEC_SCHEMA = "https://vega.github.io/schema/vega-lite/v5.json"
_X_FIELDS = ("date", "time", "datetime", "timestamp")
_LABEL_FIELDS = ("ticker", "security", "member", "name", "label")
_SERIES_FIELDS = ("ticker", "security", "field", "side", "category")
_VALUE_FIELDS = ("value", "PX_LAST", "close", "price", "weight", "marketValue", "market_value")
_COMPACT_DATE = re.compile(r"^\d{8}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}(?:$|[T\s])")


def _field(name: str) -> str:
    # Vega-Lite treats dots and brackets as access paths, not literal column names.
    return name.replace("\\", "\\\\").replace(".", "\\.").replace("[", "\\[").replace("]", "\\]")


def _number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _numeric(rows: list[Row], field: str) -> bool:
    return any(_number(row.get(field)) for row in rows)


def _require_numeric(rows: list[Row], field: str) -> None:
    if not _numeric(rows, field):
        raise ValueError(f"{field!r} must contain at least one finite numeric value")


def _candidate(fields: dict[str, None], candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in fields:
            return candidate
    folded = {field.casefold(): field for field in fields}
    return next((folded[candidate.casefold()] for candidate in candidates if candidate.casefold() in folded), None)


def _require_field(fields: dict[str, None], explicit: str | None, label: str, candidates: tuple[str, ...]) -> str:
    field = explicit if explicit is not None else _candidate(fields, candidates)
    if field is None or field not in fields:
        raise ValueError(f"Missing {label}; specify an existing row field or include one of: {', '.join(candidates)}")
    return field


def _vega_type(rows: list[Row], field: str) -> str:
    for row in rows:
        value = row.get(field)
        if _number(value):
            return "quantitative"
        if isinstance(value, str) and (_ISO_DATE.match(value) or _COMPACT_DATE.fullmatch(value)):
            return "temporal"
    return "nominal"


def _temporal_rows(rows: list[Row], field: str) -> list[Row]:
    normalized: list[Row] | None = None
    for index, row in enumerate(rows):
        value = row.get(field)
        if isinstance(value, str) and _COMPACT_DATE.fullmatch(value):
            iso_date = date(int(value[:4]), int(value[4:6]), int(value[6:])).isoformat()
            if normalized is None:
                normalized = rows[:index]
            normalized.append({**row, field: iso_date})
        elif normalized is not None:
            normalized.append(row)
    return rows if normalized is None else normalized


def _encoding(field: str, kind: str) -> dict[str, Any]:
    return {"field": _field(field), "title": field, "type": kind}


def _tooltip(rows: list[Row], fields: list[str], *, quantitative: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    return [
        _encoding(field, "quantitative" if field in quantitative else _vega_type(rows, field))
        for field in dict.fromkeys(fields)
    ]


def _series(input: BaseModel, fields: dict[str, None], *, infer: bool = False) -> str | None:
    series = input.series_field
    if series is None and infer:
        series = _candidate(fields, _SERIES_FIELDS)
    if series is not None and series not in fields:
        raise ValueError(f"Missing series_field: {series}")
    return series


def _generated_field(fields: dict[str, None], prefix: str) -> str:
    name = prefix
    while name in fields:
        name += "_"
    fields[name] = None
    return name


def _generic(
    input: BaseModel, rows: list[Row], fields: dict[str, None], chart: str
) -> tuple[dict[str, Any], list[Row], str, list[str], str | None]:
    x = _require_field(fields, input.x_field, "x_field", _X_FIELDS)
    y_fields = input.y_fields
    if y_fields is None:
        y = _candidate(fields, _VALUE_FIELDS)
        if y is None:
            y = next((field for field in fields if field != x and _numeric(rows, field)), None)
        if y is None:
            raise ValueError("Missing y_fields; include at least one numeric value field")
        y_fields = [y]
    for field in y_fields:
        if field not in fields:
            raise ValueError(f"Missing y field: {field}")
        _require_numeric(rows, field)
    series = _series(input, fields, infer=len(y_fields) == 1)
    if _vega_type(rows, x) == "temporal":
        rows = _temporal_rows(rows, x)
    encoding: dict[str, Any] = {"x": _encoding(x, _vega_type(rows, x))}
    body: dict[str, Any] = {
        "mark": {"type": "point" if chart == "scatter" else chart, "tooltip": True},
        "encoding": encoding,
    }
    tip = [x] + ([] if series is None else [series])
    if len(y_fields) == 1:
        y = y_fields[0]
        encoding["y"] = _encoding(y, "quantitative")
        if series is not None:
            encoding["color"] = _encoding(series, "nominal")
        encoding["tooltip"] = _tooltip(rows, [*tip, y], quantitative=(y,))
    else:
        folded_series = _generated_field(fields, "_xbbg_series")
        folded_value = _generated_field(fields, "_xbbg_value")
        body["transform"] = [{"fold": [_field(field) for field in y_fields], "as": [folded_series, folded_value]}]
        encoding["y"] = _encoding(folded_value, "quantitative")
        encoding["color"] = _encoding(folded_series, "nominal")
        if series is not None:
            encoding["detail"] = _encoding(series, "nominal")
        encoding["tooltip"] = _tooltip(rows, [*tip, folded_series, folded_value], quantitative=(folded_value,))
    return body, rows, x, y_fields, series


def _bar(
    input: BaseModel, rows: list[Row], fields: dict[str, None]
) -> tuple[dict[str, Any], list[Row], str, list[str], str | None]:
    x = _require_field(fields, input.x_field or input.label_field, "label_field", _LABEL_FIELDS)
    y = _require_field(
        fields, input.value_field or (input.y_fields[0] if input.y_fields else None), "value_field", _VALUE_FIELDS
    )
    _require_numeric(rows, y)
    series = _series(input, fields)
    if _vega_type(rows, x) == "temporal":
        rows = _temporal_rows(rows, x)
    encoding: dict[str, Any] = {
        "x": {**_encoding(x, _vega_type(rows, x)), "sort": "-y"},
        "y": _encoding(y, "quantitative"),
        "tooltip": _tooltip(rows, [x, *([] if series is None else [series]), y], quantitative=(y,)),
    }
    if series is not None:
        encoding["color"] = _encoding(series, "nominal")
    return {"mark": {"type": "bar", "tooltip": True}, "encoding": encoding}, rows, x, [y], series


def _candlestick(
    input: BaseModel, rows: list[Row], fields: dict[str, None]
) -> tuple[dict[str, Any], list[Row], str, list[str], str | None]:
    x = _require_field(fields, input.x_field, "x_field", _X_FIELDS)
    opening = _require_field(fields, input.open_field, "open_field", ("open", "OPEN", "PX_OPEN"))
    high = _require_field(fields, input.high_field, "high_field", ("high", "HIGH", "PX_HIGH"))
    low = _require_field(fields, input.low_field, "low_field", ("low", "LOW", "PX_LOW"))
    close = _require_field(fields, input.close_field, "close_field", ("close", "CLOSE", "PX_LAST", "last", "value"))
    y_fields = [opening, high, low, close]
    for field in y_fields:
        _require_numeric(rows, field)
    if _vega_type(rows, x) == "temporal":
        rows = _temporal_rows(rows, x)
    # JSON quoting prevents column labels becoming executable Vega expressions.
    color = {
        "condition": {"test": f"datum[{json.dumps(close)}] >= datum[{json.dumps(opening)}]", "value": "#137333"},
        "value": "#c5221f",
    }
    series = _series(input, fields)
    shared: dict[str, Any] = {"x": _encoding(x, _vega_type(rows, x))}
    if series is not None:
        shared["detail"] = _encoding(series, "nominal")
    tip = _tooltip(rows, [x, *([] if series is None else [series]), *y_fields], quantitative=tuple(y_fields))
    body = {
        "encoding": shared,
        "layer": [
            {
                "mark": "rule",
                "encoding": {
                    "color": color,
                    "tooltip": tip,
                    "y": _encoding(low, "quantitative"),
                    "y2": {"field": _field(high)},
                },
            },
            {
                "mark": "bar",
                "encoding": {
                    "color": color,
                    "tooltip": tip,
                    "y": _encoding(opening, "quantitative"),
                    "y2": {"field": _field(close)},
                },
            },
        ],
    }
    return body, rows, x, y_fields, series


def _depth(
    input: BaseModel, rows: list[Row], fields: dict[str, None]
) -> tuple[dict[str, Any], list[Row], str, list[str], str | None]:
    price = _require_field(fields, input.price_field or input.x_field, "price_field", ("price", "PRICE", "px", "PX"))
    size = _require_field(
        fields,
        input.size_field or input.value_field or (input.y_fields[0] if input.y_fields else None),
        "size_field",
        ("size", "SIZE", "quantity", "qty", "volume"),
    )
    side = _require_field(fields, input.side_field or input.series_field, "side_field", ("side", "SIDE", "type"))
    _require_numeric(rows, price)
    _require_numeric(rows, size)
    body = {
        "mark": {"type": "bar", "tooltip": True},
        "encoding": {
            "x": _encoding(price, "quantitative"),
            "y": _encoding(size, "quantitative"),
            "color": _encoding(side, "nominal"),
            "tooltip": _tooltip(rows, [side, price, size], quantitative=(price, size)),
        },
    }
    return body, rows, price, [size], side


def create_chart_spec(input: BaseModel) -> dict[str, Any]:
    """Return an application artifact containing only bounded, inline chart data."""
    rows = input.rows
    if input.max_points is not None and input.max_points < len(rows):
        rows = rows[: input.max_points]
    if not rows:
        raise ValueError("rows must contain at least one chart data row")
    fields = dict.fromkeys(field for row in rows for field in row)
    # Validate all explicit references against the rows actually being displayed.
    for parameter in (
        "x_field",
        "series_field",
        "label_field",
        "value_field",
        "open_field",
        "high_field",
        "low_field",
        "close_field",
        "side_field",
        "price_field",
        "size_field",
    ):
        field = getattr(input, parameter)
        if field is not None and field not in fields:
            raise ValueError(f"Missing {parameter}: {field}")
    for field in input.y_fields or ():
        if field not in fields:
            raise ValueError(f"Missing y field: {field}")
    defaults = {"bdh": "line", "bdib": "candlestick", "holdings": "bar", "depth": "depth", "rows": "line"}
    chart = input.chart or defaults[input.source]
    title = input.title or f"{input.source} {chart}"
    if chart == "candlestick":
        body, rows, x, ys, series = _candlestick(input, rows, fields)
    elif chart == "depth":
        body, rows, x, ys, series = _depth(input, rows, fields)
    elif chart == "bar":
        body, rows, x, ys, series = _bar(input, rows, fields)
    else:
        body, rows, x, ys, series = _generic(input, rows, fields, chart)
    truncated = len(rows) != len(input.rows)
    summary = {
        "chart": chart,
        "inputRows": len(input.rows),
        "renderer": "vega-lite",
        "rowCount": len(rows),
        "source": input.source,
        "title": title,
        "truncatedInput": truncated,
        "xField": x,
        "yFields": ys,
        **({} if series is None else {"seriesField": series}),
    }
    return {
        "kind": "xbbg.visualization",
        "version": 1,
        "component": "xbbg_chart",
        "renderer": "vega-lite",
        "rowCount": len(rows),
        "inputRowCount": len(input.rows),
        "truncatedInput": truncated,
        "source": input.source,
        "chart": chart,
        "summary": summary,
        "spec": {
            "$schema": _SPEC_SCHEMA,
            "data": {"values": rows},
            "description": f"xbbg {chart} chart spec for {input.source}",
            "title": title,
            **body,
        },
        "warnings": [
            f"Chart spec contains first {len(rows)} of {len(input.rows)} rows; narrow the upstream request for a complete visualization."
        ]
        if truncated
        else [],
    }
