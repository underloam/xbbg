from __future__ import annotations
import atexit

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, asdict
from datetime import date, datetime, timezone
import json
from pathlib import Path
import statistics
import time
import tracemalloc
from typing import Any

import pandas as pd

from xbbg import blp
from benchmark_contracts import (
    PYTHON_MEMORY_SCOPE,
    archive_path,
    collect_provenance,
    close_shared_sessions,
    report_date,
    reused_pdblp_connection,
)
from config import (
    BDH_END,
    BDH_START,
    BDIB_DATE,
    BDIB_END_TIME,
    BDIB_INTERVAL,
    BDIB_START_TIME,
    BENCH_TZ,
    FIELDS_MULTI,
    FIELDS_SINGLE,
    TICKERS_MULTI,
    TICKERS_SINGLE,
    intraday_window_utc,
)

ITERATIONS = 5
WARMUP = 1

RESULTS_DIR = Path(__file__).resolve().parent / "results"

BDIB_SECURITY = "IBM US Equity"
BDIB_EVENT = "TRADE"
BDIB_START_NY = f"{BDIB_DATE}T{BDIB_START_TIME}"
BDIB_END_NY = f"{BDIB_DATE}T{BDIB_END_TIME}"
BDIB_START_UTC, BDIB_END_UTC = intraday_window_utc(
    BDIB_DATE,
    BDIB_START_TIME,
    BDIB_END_TIME,
)
BQL_SIMPLE = "get(px_last) for(['IBM US Equity'])"
BQL_MULTI = "get(px_last, px_volume) for(['IBM US Equity', 'AAPL US Equity'])"

NUM_TOL = 1e-9
_OPEN_BQUERY_CONTEXTS: list[Any] = []


def close_benchmark_sessions() -> None:
    while _OPEN_BQUERY_CONTEXTS:
        _OPEN_BQUERY_CONTEXTS.pop().__exit__(None, None, None)
    close_shared_sessions()


atexit.register(close_benchmark_sessions)


@dataclass
class Result:
    package: str
    operation: str
    supported: bool
    equivalent_to_xbbg: bool | None
    equality_detail: str
    lifecycle_scope: str = ""
    warm_first_ms: float | None = None
    warm_mean_ms: float | None = None
    warm_median_ms: float | None = None
    warm_std_ms: float | None = None
    warm_max_ms: float | None = None
    warm_sample_count: int = 0
    python_tracemalloc_peak_mb: float | None = None
    memory_sample_count: int = 0
    memory_scope: str | None = None
    shape: list[int] | None = None
    error: str | None = None


def to_pandas(obj: Any) -> pd.DataFrame:
    if isinstance(obj, pd.DataFrame):
        return obj.copy()
    if hasattr(obj, "to_pandas"):
        return obj.to_pandas()
    if hasattr(obj, "combine"):
        combined = obj.combine()
        if hasattr(combined, "to_pandas"):
            return combined.to_pandas()
        if isinstance(combined, pd.DataFrame):
            return combined.copy()
    return pd.DataFrame(obj)


def scalar(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float):
        return float(value)
    if isinstance(value, int):
        return int(value)
    return str(value)


def scalar_date(value: Any) -> Any:
    value = scalar(value)
    if isinstance(value, str):
        return value.replace(" ", "T").split("T", 1)[0]
    return value


def lower_columns(df: pd.DataFrame) -> dict[str, str]:
    return {str(c).lower(): c for c in df.columns}


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [tuple(str(x) for x in col) for col in out.columns]
    return out


def normalize_bdp(obj: Any, securities: list[str], fields: list[str]) -> list[dict[str, Any]]:
    df = flatten_columns(to_pandas(obj))
    cols = lower_columns(df)
    records: list[dict[str, Any]] = []

    if {"field", "value"}.issubset(cols):
        sec_col = cols.get("ticker") or cols.get("security")
        for _, row in df.iterrows():
            records.append(
                {
                    "security": scalar(row[sec_col]) if sec_col else securities[0],
                    "field": str(row[cols["field"]]).upper(),
                    "value": scalar(row[cols["value"]]),
                }
            )
        return sorted_records(records)

    sec_col = cols.get("ticker") or cols.get("security")
    work = df.reset_index() if sec_col is None else df
    cols = lower_columns(work)
    sec_col = cols.get("ticker") or cols.get("security") or cols.get("index")
    for _, row in work.iterrows():
        security = scalar(row[sec_col]) if sec_col else securities[0]
        for field in fields:
            col = field if field in work.columns else field.lower()
            if col in work.columns:
                records.append({"security": security, "field": field.upper(), "value": scalar(row[col])})
    return sorted_records(records)


def normalize_bdh(obj: Any, securities: list[str], fields: list[str]) -> list[dict[str, Any]]:
    df = flatten_columns(to_pandas(obj))
    records: list[dict[str, Any]] = []
    cols = lower_columns(df)

    if {"date", "field", "value"}.issubset(cols):
        sec_col = cols.get("ticker") or cols.get("security")
        for _, row in df.iterrows():
            records.append(
                {
                    "security": scalar(row[sec_col]) if sec_col else securities[0],
                    "date": scalar_date(row[cols["date"]]),
                    "field": str(row[cols["field"]]).upper(),
                    "value": scalar(row[cols["value"]]),
                }
            )
        return sorted_records(records)

    # Wide historical frames often use date as index and either field columns or
    # MultiIndex columns (security, field).
    work = df.copy()
    if "date" not in lower_columns(work):
        work = work.reset_index()
    cols = lower_columns(work)
    date_col = cols.get("date") or cols.get("index")
    sec_col = cols.get("ticker") or cols.get("security")

    for _, row in work.iterrows():
        row_date = scalar_date(row[date_col]) if date_col else None
        if sec_col:
            row_securities = [scalar(row[sec_col])]
        else:
            row_securities = securities
        for col in work.columns:
            if col in (date_col, sec_col):
                continue
            if isinstance(col, tuple):
                parts = [p for p in col if p and p != "None"]
                if len(parts) >= 2:
                    security, field = parts[0], parts[-1]
                elif len(parts) == 1:
                    security, field = row_securities[0], parts[0]
                else:
                    continue
            else:
                field = str(col)
                security = row_securities[0]
            if field.upper() in {f.upper() for f in fields}:
                records.append(
                    {
                        "security": security,
                        "date": row_date,
                        "field": field.upper(),
                        "value": scalar(row[col]),
                    }
                )
    return sorted_records(records)


def normalize_bdib(obj: Any) -> list[dict[str, Any]]:
    df = to_pandas(obj)
    if df.empty:
        return []
    cols = lower_columns(df)
    sec_col = cols.get("ticker") or cols.get("security")
    out: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rec = {}
        if sec_col:
            rec["security"] = scalar(row[sec_col])
        for name in ["time", "open", "high", "low", "close", "volume", "numevents", "num_events", "value"]:
            col = cols.get(name)
            if col is not None:
                canonical = "numEvents" if name in {"numevents", "num_events"} else name
                rec[canonical] = scalar(row[col])
        out.append(rec)
    return sorted_records(out)


def normalize_bql(obj: Any) -> list[dict[str, Any]]:
    df = to_pandas(obj)
    out: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        rec = {("id" if str(col).lower() == "ticker" else str(col).lower()): scalar(row[col]) for col in df.columns}
        out.append(rec)
    return sorted_records(out)


def sorted_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(records, key=lambda r: json.dumps(r, sort_keys=True, default=str))


def equal_values(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return abs(float(a) - float(b)) <= NUM_TOL
    if isinstance(a, (int, float)) and isinstance(b, str):
        try:
            return abs(float(a) - float(b)) <= NUM_TOL
        except ValueError:
            pass
    if isinstance(a, str) and isinstance(b, (int, float)):
        try:
            return abs(float(a) - float(b)) <= NUM_TOL
        except ValueError:
            pass
    if isinstance(a, str) and isinstance(b, str):
        left = a.replace(" ", "T").rstrip("Z")
        right = b.replace(" ", "T").rstrip("Z")
        if left == right:
            return True
        if left.endswith("T00:00:00") and left.split("T", 1)[0] == right:
            return True
        if right.endswith("T00:00:00") and right.split("T", 1)[0] == left:
            return True
    return a == b


def compare_records(expected: list[dict[str, Any]], actual: list[dict[str, Any]]) -> tuple[bool, str]:
    """Compare normalized rows; the detail names where they differ, never the values.

    Details are saved in shared reports, and the values are Bloomberg data.
    """
    if len(expected) != len(actual):
        return False, f"row count {len(actual)} != {len(expected)}"
    for idx, (left, right) in enumerate(zip(expected, actual, strict=True)):
        if set(left) != set(right):
            return False, f"row {idx} keys {sorted(right)} != {sorted(left)}"
        for key in left:
            if not equal_values(left[key], right[key]):
                return False, f"row {idx} {key} differs"
    return True, "matched"


def shape_of(obj: Any) -> list[int]:
    try:
        df = to_pandas(obj)
        return [int(df.shape[0]), int(df.shape[1])]
    except Exception:
        try:
            return [len(obj)]
        except Exception:
            return [1]


def measure(
    package: str,
    operation: str,
    call: Callable[[], Any],
    normalize: Callable[[Any], list[dict[str, Any]]],
    expected: list[dict[str, Any]] | None,
) -> tuple[Result, list[dict[str, Any]] | None]:
    try:
        for _ in range(WARMUP):
            call()
        times: list[float] = []
        last: Any = None
        for _ in range(ITERATIONS):
            start = time.perf_counter()
            last = call()
            times.append((time.perf_counter() - start) * 1000)

        tracemalloc.start()
        try:
            memory_result = call()
            _current, python_peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        actual = normalize(last)
        if normalize(memory_result) != actual:
            raise RuntimeError("separate allocation run changed the normalized result")
        if expected is None:
            equivalent = True if actual else None
            detail = "baseline" if actual else "empty baseline; not comparable"
        else:
            equivalent, detail = compare_records(expected, actual)
        return (
            Result(
                package=package,
                operation=operation,
                supported=True,
                equivalent_to_xbbg=equivalent,
                equality_detail=detail,
                lifecycle_scope=lifecycle_scope(package),
                warm_first_ms=times[0],
                warm_mean_ms=statistics.mean(times),
                warm_median_ms=statistics.median(times),
                warm_std_ms=statistics.stdev(times) if len(times) > 1 else 0.0,
                warm_max_ms=max(times),
                warm_sample_count=len(times),
                python_tracemalloc_peak_mb=python_peak / 1024 / 1024,
                memory_scope=PYTHON_MEMORY_SCOPE,
                memory_sample_count=1,
                shape=shape_of(last),
            ),
            actual,
        )
    except Exception as exc:
        if tracemalloc.is_tracing():
            tracemalloc.stop()
        # Bloomberg error text can carry the login and firm number; only the
        # exception type is saved, the message goes to this console alone.
        print(f"  {package} {operation} failed: {type(exc).__name__}: {exc}")
        return (
            Result(
                package=package,
                operation=operation,
                supported=False,
                equivalent_to_xbbg=None,
                equality_detail="not run",
                error=type(exc).__name__,
            ),
            None,
        )


def lifecycle_scope(package: str) -> str:
    if package == "xbbg-latest":
        return "warm calls after discarded warmup on xbbg's process-global native engine"
    if package == "pdblp":
        return "warm calls on one explicitly reused pdblp BCon"
    if package == "polars-bloomberg":
        return "warm calls inside one explicitly reused BQuery context"
    return "warm calls through package API; session ownership is package-managed and not guaranteed equivalent"


def run_async(coro: Any) -> Any:
    """Run xbbg async endpoints so the benchmark uses the native async path."""
    return asyncio.run(coro)


def xbbg_calls() -> dict[str, tuple[Callable[[], Any], Callable[[Any], list[dict[str, Any]]]]]:
    return {
        "bdp_single": (
            lambda: run_async(blp.abdp(TICKERS_SINGLE[0], FIELDS_SINGLE[0], backend="Pandas")),
            lambda obj: normalize_bdp(obj, TICKERS_SINGLE, FIELDS_SINGLE),
        ),
        "bdp_multi": (
            lambda: run_async(blp.abdp(TICKERS_MULTI, FIELDS_MULTI, backend="Pandas")),
            lambda obj: normalize_bdp(obj, TICKERS_MULTI, FIELDS_MULTI),
        ),
        "bdh_single": (
            lambda: run_async(blp.abdh(TICKERS_SINGLE[0], FIELDS_SINGLE[0], BDH_START, BDH_END, backend="Pandas")),
            lambda obj: normalize_bdh(obj, TICKERS_SINGLE, FIELDS_SINGLE),
        ),
        "bdh_multi": (
            lambda: run_async(blp.abdh(TICKERS_MULTI, FIELDS_MULTI, BDH_START, BDH_END, backend="Pandas")),
            lambda obj: normalize_bdh(obj, TICKERS_MULTI, FIELDS_MULTI),
        ),
        "bdib": (
            lambda: run_async(
                blp.abdib(
                    BDIB_SECURITY,
                    typ=BDIB_EVENT,
                    start_datetime=BDIB_START_NY,
                    end_datetime=BDIB_END_NY,
                    interval=BDIB_INTERVAL,
                    request_tz="America/New_York",
                    output_tz="UTC",
                    backend="Pandas",
                )
            ),
            normalize_bdib,
        ),
        "bql_simple": (
            lambda: run_async(blp.abql(BQL_SIMPLE, backend="Pandas")),
            normalize_bql,
        ),
        "bql_multi": (
            lambda: run_async(blp.abql(BQL_MULTI, backend="Pandas")),
            normalize_bql,
        ),
    }


def competitor_calls() -> dict[str, Any]:
    competitors: dict[str, Any] = {}

    try:
        import pdblp  # noqa: F401

        def pdblp_ref(securities: list[str], fields: list[str]):
            return reused_pdblp_connection().ref(securities, fields)

        def pdblp_bdh(securities: list[str], fields: list[str]):
            return reused_pdblp_connection().bdh(
                securities,
                fields,
                BDH_START.replace("-", ""),
                BDH_END.replace("-", ""),
            )

        def pdblp_bdib():
            try:
                return reused_pdblp_connection().bdib(
                    BDIB_SECURITY,
                    BDIB_START_UTC,
                    BDIB_END_UTC,
                    BDIB_EVENT,
                    BDIB_INTERVAL,
                )
            except KeyError as exc:
                if "time" in str(exc):
                    return pd.DataFrame()
                raise

        competitors["pdblp"] = {
            "bdp_single": (
                lambda: pdblp_ref(TICKERS_SINGLE, FIELDS_SINGLE),
                lambda obj: normalize_bdp(obj, TICKERS_SINGLE, FIELDS_SINGLE),
            ),
            "bdp_multi": (
                lambda: pdblp_ref(TICKERS_MULTI, FIELDS_MULTI),
                lambda obj: normalize_bdp(obj, TICKERS_MULTI, FIELDS_MULTI),
            ),
            "bdh_single": (
                lambda: pdblp_bdh(TICKERS_SINGLE, FIELDS_SINGLE),
                lambda obj: normalize_bdh(obj, TICKERS_SINGLE, FIELDS_SINGLE),
            ),
            "bdib": (pdblp_bdib, normalize_bdib),
        }
    except Exception:
        pass

    try:
        import bbg_fetch

        competitors["bbg-fetch"] = {
            "bdp_single": (
                lambda: bbg_fetch.bdp(TICKERS_SINGLE[0], FIELDS_SINGLE[0]),
                lambda obj: normalize_bdp(obj, TICKERS_SINGLE, FIELDS_SINGLE),
            ),
            "bdp_multi": (
                lambda: bbg_fetch.bdp(TICKERS_MULTI, FIELDS_MULTI),
                lambda obj: normalize_bdp(obj, TICKERS_MULTI, FIELDS_MULTI),
            ),
            "bdh_single": (
                lambda: bbg_fetch.bdh(TICKERS_SINGLE[0], FIELDS_SINGLE[0], BDH_START, BDH_END),
                lambda obj: normalize_bdh(obj, TICKERS_SINGLE, FIELDS_SINGLE),
            ),
            "bdh_multi": (
                lambda: bbg_fetch.bdh(TICKERS_MULTI, FIELDS_MULTI, BDH_START, BDH_END),
                lambda obj: normalize_bdh(obj, TICKERS_MULTI, FIELDS_MULTI),
            ),
        }
    except Exception:
        pass

    try:
        from polars_bloomberg import BQuery

        bdib_start = BDIB_START_UTC.replace(tzinfo=timezone.utc)
        bdib_end = BDIB_END_UTC.replace(tzinfo=timezone.utc)
        bquery_context = BQuery(timeout=5000)
        bquery = bquery_context.__enter__()
        _OPEN_BQUERY_CONTEXTS.append(bquery_context)

        def with_bquery(fn: Callable[[Any], Any]) -> Any:
            return fn(bquery)

        competitors["polars-bloomberg"] = {
            "bdp_single": (
                lambda: with_bquery(lambda bq: bq.bdp(TICKERS_SINGLE, FIELDS_SINGLE)),
                lambda obj: normalize_bdp(obj, TICKERS_SINGLE, FIELDS_SINGLE),
            ),
            "bdp_multi": (
                lambda: with_bquery(lambda bq: bq.bdp(TICKERS_MULTI, FIELDS_MULTI)),
                lambda obj: normalize_bdp(obj, TICKERS_MULTI, FIELDS_MULTI),
            ),
            "bdh_single": (
                lambda: with_bquery(
                    lambda bq: bq.bdh(
                        TICKERS_SINGLE, FIELDS_SINGLE, date.fromisoformat(BDH_START), date.fromisoformat(BDH_END)
                    )
                ),
                lambda obj: normalize_bdh(obj, TICKERS_SINGLE, FIELDS_SINGLE),
            ),
            "bdh_multi": (
                lambda: with_bquery(
                    lambda bq: bq.bdh(
                        TICKERS_MULTI, FIELDS_MULTI, date.fromisoformat(BDH_START), date.fromisoformat(BDH_END)
                    )
                ),
                lambda obj: normalize_bdh(obj, TICKERS_MULTI, FIELDS_MULTI),
            ),
            "bdib": (
                lambda: with_bquery(lambda bq: bq.bdib(BDIB_SECURITY, BDIB_EVENT, BDIB_INTERVAL, bdib_start, bdib_end)),
                normalize_bdib,
            ),
            "bql_simple": (lambda: with_bquery(lambda bq: bq.bql(BQL_SIMPLE).combine()), normalize_bql),
            "bql_multi": (lambda: with_bquery(lambda bq: bq.bql(BQL_MULTI).combine()), normalize_bql),
        }
    except Exception:
        pass

    return competitors


def main() -> int:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    xbbg_version = getattr(__import__("xbbg"), "__version__", "unknown")
    operations = xbbg_calls()
    baselines: dict[str, list[dict[str, Any]]] = {}
    results: list[Result] = []

    print(f"xbbg latest: {xbbg_version}")
    print(f"iterations={ITERATIONS}, warmup={WARMUP}")

    for operation, (call, normalize) in operations.items():
        print(f"baseline xbbg {operation}...", flush=True)
        baseline_result, baseline_records = measure("xbbg-latest", operation, call, normalize, None)
        results.append(baseline_result)
        if baseline_result.error:
            print(f"  ERROR {baseline_result.error}")
            continue
        if not baseline_records:
            print(f"  NOT COMPARABLE shape={baseline_result.shape} {baseline_result.equality_detail}")
            continue
        baselines[operation] = baseline_records
        print(f"  {baseline_result.warm_mean_ms:.2f}ms shape={baseline_result.shape} rows={len(baseline_records)}")

    for package, calls in competitor_calls().items():
        for operation, (call, normalize) in calls.items():
            if operation not in baselines:
                continue
            print(f"{package} {operation}...", flush=True)
            result, _actual = measure(package, operation, call, normalize, baselines[operation])
            results.append(result)
            if result.error:
                print(f"  ERROR {result.error}")
            else:
                marker = (
                    "NOT COMPARABLE"
                    if result.equivalent_to_xbbg is None
                    else ("OK" if result.equivalent_to_xbbg else "MISMATCH")
                )
                print(f"  {result.warm_mean_ms:.2f}ms shape={result.shape} equality={marker} {result.equality_detail}")

    output = {
        "schema_version": 3,
        "xbbg_version": xbbg_version,
        "date": report_date(),
        "iterations": ITERATIONS,
        "warmup": WARMUP,
        "timing_scope": (
            "uninstrumented warm requests after one discarded call; pdblp and polars-bloomberg "
            "use explicit reused sessions, while package-managed lifecycles are labelled per result"
        ),
        "coverage": "live Bloomberg SDK/network requests with normalized result equivalence",
        "provenance": collect_provenance(
            inputs={
                "operations": sorted(operations),
                "tickers": TICKERS_MULTI,
                "fields": FIELDS_MULTI,
                "bdh": [BDH_START, BDH_END],
                "bdib": {
                    "date": BDIB_DATE,
                    "start_ny": BDIB_START_NY,
                    "end_ny": BDIB_END_NY,
                    "timezone": BENCH_TZ,
                },
            },
            benchmark_file=Path(__file__).name,
        ),
        "results": [asdict(result) for result in results],
    }
    json_path = archive_path(RESULTS_DIR, "latest_competitor_equivalence", ".json")
    md_path = json_path.with_suffix(".md")
    json_path.write_text(json.dumps(output, indent=2), encoding="utf-8")
    write_markdown(md_path, output)
    print(f"wrote {json_path}")
    close_benchmark_sessions()
    print(f"wrote {md_path}")
    return 0


def fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def write_markdown(path: Path, output: dict[str, Any]) -> None:
    rows = output["results"]
    lines = [
        "# Latest xbbg Competitor Benchmark with Equivalence Checks",
        "",
        f"**xbbg version:** {output['xbbg_version']}",
        f"**Date (UTC):** {output['date']}",
        f"**Iterations:** {output['iterations']} measured + {output['warmup']} warmup",
        "",
        "| Operation | Package | Equal to xbbg | Lifecycle scope | Warm mean ms | Warm max ms | Warm n | CPython tracemalloc MB (untimed) | Shape | Detail |",
        "|---|---|---:|---|---:|---:|---:|---:|---|---|",
    ]
    for row in rows:
        eq = "baseline" if row["package"] == "xbbg-latest" else ("yes" if row["equivalent_to_xbbg"] else "no")
        if row.get("error"):
            eq = "error"
        lines.append(
            "| {operation} | {package} | {eq} | {lifecycle} | {warm} | {maximum} | {samples} | {mem} | {shape} | {detail} |".format(
                operation=row["operation"],
                package=row["package"],
                eq=eq,
                lifecycle=row.get("lifecycle_scope", "").replace("|", "\\|"),
                warm=fmt(row.get("warm_mean_ms")),
                maximum=fmt(row.get("warm_max_ms")),
                samples=row.get("warm_sample_count", 0),
                mem=fmt(row.get("python_tracemalloc_peak_mb")),
                shape=row.get("shape") or "n/a",
                detail=(row.get("error") or row.get("equality_detail") or "").replace("|", "\\|"),
            )
        )
    lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
