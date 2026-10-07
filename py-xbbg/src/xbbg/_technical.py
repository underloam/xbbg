"""Technical-analysis vocabulary, requests, and schema-derived IDE stubs.

Study aliases and defaults are collocated with the request construction and stub
generator that consume them. Requests use the lower engine module directly;
this module has no import back into the public facade.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypeAlias
import warnings

from . import _engine, _request_options, backend as _backend
from .services import ExtractorHint, Operation, RequestParams, Service

DataFrameResult: TypeAlias = Any


def _core_arrow_table_class() -> type[Any]:
    from xbbg._core import ArrowTable

    return ArrowTable


# =============================================================================
# Technical Analysis API - Bloomberg Technical Analysis Service
# =============================================================================

# Study type to attribute name mapping
_TA_STUDIES: dict[str, str] = {
    # Moving Averages
    "smavg": "smavgStudyAttributes",
    "sma": "smavgStudyAttributes",
    "emavg": "emavgStudyAttributes",
    "ema": "emavgStudyAttributes",
    "wmavg": "wmavgStudyAttributes",
    "wma": "wmavgStudyAttributes",
    "vmavg": "vmavgStudyAttributes",
    "vma": "vmavgStudyAttributes",
    "tmavg": "tmavgStudyAttributes",
    "tma": "tmavgStudyAttributes",
    "ipmavg": "ipmavgStudyAttributes",
    # Oscillators
    "rsi": "rsiStudyAttributes",
    "macd": "macdStudyAttributes",
    "mao": "maoStudyAttributes",
    "momentum": "momentumStudyAttributes",
    "mom": "momentumStudyAttributes",
    "roc": "rocStudyAttributes",
    # Bands & Channels
    "boll": "bollStudyAttributes",
    "bb": "bollStudyAttributes",
    "kltn": "kltnStudyAttributes",
    "keltner": "kltnStudyAttributes",
    "mae": "maeStudyAttributes",
    "te": "teStudyAttributes",
    "al": "alStudyAttributes",
    # Trend
    "dmi": "dmiStudyAttributes",
    "adx": "dmiStudyAttributes",
    "tas": "tasStudyAttributes",
    "stoch": "tasStudyAttributes",
    "trender": "trenderStudyAttributes",
    "ptps": "ptpsStudyAttributes",
    "parabolic": "ptpsStudyAttributes",
    "sar": "ptpsStudyAttributes",
    # Volume
    "chko": "chkoStudyAttributes",
    "ado": "adoStudyAttributes",
    "vat": "vatStudyAttributes",
    "tvat": "tvatStudyAttributes",
    # Volatility
    "atr": "atrStudyAttributes",
    "hurst": "hurstStudyAttributes",
    # Other
    "fg": "fgStudyAttributes",
    "fear_greed": "fgStudyAttributes",
    "goc": "gocStudyAttributes",
    "ichimoku": "gocStudyAttributes",
    "cmci": "cmciStudyAttributes",
    "wlpr": "wlprStudyAttributes",
    "williams": "wlprStudyAttributes",
    "maxmin": "maxminStudyAttributes",
    "rex": "rexStudyAttributes",
    "etd": "etdStudyAttributes",
    "pd": "pdStudyAttributes",
    "rv": "rvStudyAttributes",
    "pivot": "pivotStudyAttributes",
    "or": "orStudyAttributes",
    "pcr": "pcrStudyAttributes",
    "bs": "bsStudyAttributes",
}

# Default study parameters
_TA_DEFAULTS: dict[str, dict[str, Any]] = {
    "smavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "emavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "wmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "vmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "tmavgStudyAttributes": {"period": 20, "priceSourceClose": "PX_LAST"},
    "rsiStudyAttributes": {"period": 14, "priceSourceClose": "PX_LAST"},
    "macdStudyAttributes": {
        "maPeriod1": 12,
        "maPeriod2": 26,
        "sigPeriod": 9,
        "priceSourceClose": "PX_LAST",
    },
    "bollStudyAttributes": {
        "period": 20,
        "upperBand": 2.0,
        "lowerBand": 2.0,
        "priceSourceClose": "PX_LAST",
    },
    "dmiStudyAttributes": {
        "period": 14,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
    "atrStudyAttributes": {
        "maType": "Simple",
        "period": 14,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
    "tasStudyAttributes": {
        "periodK": 14,
        "periodD": 3,
        "periodDS": 3,
        "periodDSS": 3,
        "priceSourceHigh": "PX_HIGH",
        "priceSourceLow": "PX_LOW",
        "priceSourceClose": "PX_LAST",
    },
}


def _get_study_attr_name(study: str) -> str:
    """Get the Bloomberg attribute name for a study."""
    study_lower = study.lower().replace("-", "_").replace(" ", "_")
    if study_lower in _TA_STUDIES:
        return _TA_STUDIES[study_lower]
    # Try direct match with StudyAttributes suffix
    if study_lower.endswith("studyattributes"):
        return study_lower
    return f"{study_lower}StudyAttributes"


def _build_study_request(
    ticker: str,
    study: str,
    start_date: str | None = None,
    end_date: str | None = None,
    periodicity: str = "DAILY",
    interval: int | None = None,
    **study_params,
) -> list[tuple[str, str]]:
    """Build dotted-path elements for a //blp/tasvc studyRequest.

    Returns a list of (dotted_path, value_str) tuples that the Rust worker
    applies via ``set_nested_str`` / ``set_nested_int`` on the request.

    Args:
        ticker: Security name
        study: Study type
        start_date: Start date (YYYYMMDD)
        end_date: End date (YYYYMMDD)
        periodicity: Data periodicity ('DAILY', 'WEEKLY', 'MONTHLY', or 'INTRADAY')
        interval: Intraday interval in minutes (only used if periodicity is INTRADAY)
        **study_params: Study-specific parameters (e.g., period=20 for SMA period)
    """
    attr_name = _get_study_attr_name(study)

    # Get defaults and merge with user params
    defaults = _TA_DEFAULTS.get(attr_name, {})
    params = {**defaults, **study_params}

    elements: list[tuple[str, str]] = []

    # Normalize dates to YYYYMMDD (Bloomberg tasvc expects this format)
    def _norm_date(d: str | None) -> str | None:
        return d.replace("-", "").replace("/", "") if d else None

    sd = _norm_date(start_date)
    ed = _norm_date(end_date)

    # Price source
    elements.append(("priceSource.securityName", ticker))

    # Data range
    if periodicity.upper() in ("DAILY", "WEEKLY", "MONTHLY"):
        prefix = "priceSource.dataRange.historical"
        if sd:
            elements.append((f"{prefix}.startDate", sd))
        if ed:
            elements.append((f"{prefix}.endDate", ed))
        elements.append((f"{prefix}.periodicitySelection", periodicity.upper()))
    else:
        # Intraday
        prefix = "priceSource.dataRange.intraday"
        if sd:
            elements.append((f"{prefix}.startDate", sd))
        if ed:
            elements.append((f"{prefix}.endDate", ed))
        elements.append((f"{prefix}.eventType", "TRADE"))
        elements.append((f"{prefix}.interval", str(interval or 60)))

    # Study attributes
    sa_prefix = f"studyAttributes.{attr_name}"
    for key, value in params.items():
        elements.append((f"{sa_prefix}.{key}", str(value)))

    return elements


async def abta(
    tickers: str | list[str],
    study: str,
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    periodicity: str = "DAILY",
    interval: int | None = None,
    **study_params,
) -> DataFrameResult:
    """Get technical analysis study data (async).

    Uses Bloomberg //blp/tasvc service to calculate technical indicators.

    Args:
        tickers: Security or list of securities
        study: Study type (e.g., 'sma', 'rsi', 'macd', 'boll', 'atr')
        start_date: Start date. Accepts ISO 8601 / ``YYYYMMDD`` string,
            ``datetime.date``, ``datetime.datetime``, or ``pd.Timestamp``.
        end_date: End date. Same accepted shapes as ``start_date``.
        periodicity: Data periodicity ('DAILY', 'WEEKLY', 'MONTHLY', 'INTRADAY')
        interval: Intraday interval in minutes (only for periodicity='INTRADAY')
        **study_params: Study-specific parameters (e.g., period=20 for SMA period)

    Returns:
        DataFrame with study results

    Available Studies:
        Moving Averages: sma, ema, wma, vma, tma
        Oscillators: rsi, macd, mao, momentum, roc
        Bands: boll (Bollinger), keltner, mae
        Trend: dmi/adx, stoch, trender, parabolic/sar
        Volume: chko, ado, vat
        Volatility: atr, hurst
        Other: ichimoku, pivot, williams

    Example::

        # Simple Moving Average with 20-day period
        df = await xbbg.abta("AAPL US Equity", "sma", period=20)

        # RSI with 14-day period
        df = await xbbg.abta("AAPL US Equity", "rsi", period=14)

        # MACD with custom parameters
        df = await xbbg.abta("AAPL US Equity", "macd", maPeriod1=12, maPeriod2=26, sigPeriod=9)

        # Bollinger Bands with 20-day period and 2 std devs
        df = await xbbg.abta("AAPL US Equity", "boll", period=20, upperBand=2.0, lowerBand=2.0)

        # Intraday RSI with 60-minute bars
        df = await xbbg.abta("AAPL US Equity", "rsi", periodicity="INTRADAY", interval=60)

        # Multiple securities (sends concurrent requests)
        df = await xbbg.abta(["AAPL US Equity", "MSFT US Equity"], "rsi")
    """
    ticker_list = _request_options._normalize_tickers(tickers)
    engine = _engine._get_engine()

    async def fetch_single(ticker: str) -> Any | Exception:
        """Fetch TA data for a single ticker."""
        study_elements = _build_study_request(
            ticker,
            study,
            start_date=start_date,
            end_date=end_date,
            periodicity=periodicity,
            interval=interval,
            **study_params,
        )
        params = RequestParams(
            service=Service.TASVC,
            operation=Operation.STUDY_REQUEST,
            extractor=ExtractorHint.GENERIC,
            elements=study_elements,
        )
        return await engine.request(params.to_dict())

    # tasvc only supports 1 security per request, so send concurrent requests
    results = await asyncio.gather(
        *[fetch_single(t) for t in ticker_list],
        return_exceptions=True,
    )

    # Filter successful results and warn about failures
    batches: list[Any] = []
    for ticker, result in zip(ticker_list, results, strict=True):
        if isinstance(result, Exception):
            warnings.warn(f"Failed to fetch TA data for {ticker}: {result}", stacklevel=2)
        else:
            batches.append(result)

    if not batches:
        raise RuntimeError("All TA requests failed")

    # Combine all batches into a single native Arrow table
    table = _core_arrow_table_class().from_batches(batches)
    return _backend._convert_result_backend(table, None)


def ta_studies() -> list[str]:
    """List available technical analysis study names.

    Returns:
        List of study short names that can be used with bta()/abta()

    Example::

        >>> xbbg.ta_studies()
        ['sma', 'ema', 'rsi', 'macd', 'boll', 'atr', ...]
    """
    # Return unique study short names
    seen = set()
    result = []
    for name in _TA_STUDIES:
        if name not in seen:
            seen.add(name)
            result.append(name)
    return sorted(result)


def ta_study_params(study: str) -> dict[str, Any]:
    """Get default parameters for a technical analysis study.

    Args:
        study: Study name (e.g., 'rsi', 'macd', 'boll')

    Returns:
        Dictionary of parameter names and their default values

    Example::

        >>> xbbg.ta_study_params('rsi')
        {'period': 14, 'priceSourceClose': 'PX_LAST'}

        >>> xbbg.ta_study_params('macd')
        {'maPeriod1': 12, 'maPeriod2': 26, 'sigPeriod': 9, 'priceSourceClose': 'PX_LAST'}

        >>> xbbg.ta_study_params('boll')
        {'period': 20, 'upperBand': 2.0, 'lowerBand': 2.0, 'priceSourceClose': 'PX_LAST'}
    """
    attr_name = _get_study_attr_name(study)
    return _TA_DEFAULTS.get(attr_name, {})


def generate_ta_stubs(output_dir: str | None = None) -> str:
    """Generate Python type stubs for technical analysis studies.

    Creates a .pyi file with TypedDict definitions for all TA study parameters.
    Stubs are generated from the //blp/tasvc schema for IDE autocomplete support.

    Args:
        output_dir: Output directory (default: ~/.xbbg/stubs/)

    Returns:
        Path to the generated stub file.

    Example::

        >>> xbbg.generate_ta_stubs()
        '~/.xbbg/stubs/ta_studies.pyi'

        # Then in your code, IDE will autocomplete:
        >>> from xbbg.stubs.ta_studies import RSIParams
        >>> params: RSIParams = {'period': 14}
    """
    from pathlib import Path

    from .schema import get_schema

    # Get tasvc schema
    schema = get_schema("//blp/tasvc")

    # Find studyRequest operation
    op = schema.get_operation("studyRequest")
    if not op:
        raise RuntimeError("Could not find studyRequest operation in tasvc schema")

    # Find studyAttributes element
    study_attrs = None
    for child in op.request.children:
        if child.name == "studyAttributes":
            study_attrs = child
            break

    if not study_attrs:
        raise RuntimeError("Could not find studyAttributes in schema")

    # Generate stub content
    lines = [
        '"""',
        "Bloomberg Technical Analysis Study Type Stubs",
        "",
        "Auto-generated from //blp/tasvc schema.",
        "DO NOT EDIT - regenerate using xbbg.generate_ta_stubs()",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "import sys",
        "if sys.version_info >= (3, 11):",
        "    from typing import Literal, NotRequired, TypedDict",
        "else:",
        "    from typing import Literal",
        "    from typing_extensions import NotRequired, TypedDict",
        "",
    ]

    # Map of Bloomberg attribute names to friendly names
    attr_to_friendly = {v: k for k, v in _TA_STUDIES.items()}

    # Type mapping
    type_map = {
        "Bool": "bool",
        "Int32": "int",
        "Int64": "int",
        "Float32": "float",
        "Float64": "float",
        "String": "str",
        "Enumeration": "str",
    }

    # Generate TypedDict for each study
    for study_child in study_attrs.children:
        attr_name = study_child.name
        friendly = attr_to_friendly.get(attr_name, attr_name.replace("StudyAttributes", ""))

        # Create class name (e.g., rsiStudyAttributes -> RSIParams)
        class_name = friendly.upper() + "Params"
        if class_name.startswith("_"):
            class_name = class_name[1:]

        lines.append(f"class {class_name}(TypedDict, total=False):")
        lines.append(f'    """Parameters for {friendly} study."""')

        if not study_child.children:
            lines.append("    pass")
        else:
            for param in study_child.children:
                param_name = param.name
                if param.enum_values:
                    values_str = ", ".join(f'"{v}"' for v in param.enum_values)
                    param_type = f"Literal[{values_str}]"
                else:
                    param_type = type_map.get(param.data_type, "str")

                # Add default value comment if we have one
                defaults = _TA_DEFAULTS.get(attr_name, {})
                default_val = defaults.get(param_name)
                if default_val is not None:
                    lines.append(f"    {param_name}: NotRequired[{param_type}]  # default: {default_val}")
                else:
                    lines.append(f"    {param_name}: NotRequired[{param_type}]")

        lines.append("")

    # Add StudyName literal type
    study_names = sorted(set(_TA_STUDIES.keys()))
    lines.append("# All available study names")
    lines.append(f"StudyName = Literal[{', '.join(repr(s) for s in study_names)}]")
    lines.append("")

    # Write files
    output_path = Path.home() / ".xbbg" / "stubs" if output_dir is None else Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    stub_path = output_path / "ta_studies.pyi"
    stub_path.write_text("\n".join(lines))

    # Also write .py for runtime imports
    py_path = output_path / "ta_studies.py"
    py_path.write_text("\n".join(lines))

    # Configure IDE
    from .schema import configure_ide_stubs

    ide_msg = configure_ide_stubs(output_path)
    print(ide_msg)

    return str(stub_path)
