"""Options extension functions.

Convenience wrappers for options analytics and chain queries.
Returns DataFrame in the configured backend format.
Uses high-performance Rust utilities from xbbg._core.

Sync functions (wrap async with asyncio.run):
    - option_info(): Get option contract metadata
    - option_greeks(): Get Greeks and implied volatility
    - option_pricing(): Get option pricing and value decomposition
    - option_chain(): Get option chain via CHAIN_TICKERS overrides
    - option_chain_bql(): Get option chain via BQL with rich filtering
    - option_screen(): Screen multiple options with custom fields

Async functions (primary implementation):
    - aoption_info(): Async option contract metadata
    - aoption_greeks(): Async Greeks and implied volatility
    - aoption_pricing(): Async option pricing
    - aoption_chain(): Async option chain via overrides
    - aoption_chain_bql(): Async option chain via BQL
    - aoption_screen(): Async option screening
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any

from xbbg._dates import DateLike, _fmt_date
from xbbg.ext._utils import _abdp_fields, _abds_field, _syncify

if TYPE_CHECKING:
    from narwhals.typing import IntoDataFrame


# =============================================================================
# Enums
# =============================================================================


class PutCall(StrEnum):
    """Option put/call type."""

    CALL = "C"
    PUT = "P"


class ChainPeriodicity(StrEnum):
    """Option chain periodicity filter."""

    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    YEARLY = "Y"
    ALL = ""


class StrikeRef(StrEnum):
    """Option strike reference point."""

    ATM = "ATM"


class ExerciseType(StrEnum):
    """Option exercise type."""

    AMERICAN = "A"
    EUROPEAN = "E"


class ExpiryMatch(StrEnum):
    """Option expiry matching strategy."""

    EXACT = "E"
    CLOSEST = "C"


# =============================================================================
# Field constants
# =============================================================================

_OPTION_INFO_FIELDS = [
    "OPT_STRIKE_PX",
    "OPT_EXPIRE_DT",
    "OPT_PUT_CALL",
    "OPT_EXER_TYP",
    "OPT_UNDL_TICKER",
    "OPT_UNDL_PX",
    "OPT_CONT_SIZE",
    "OPT_MULTIPLIER",
    "NAME",
    "SECURITY_DES",
]

_OPTION_GREEKS_FIELDS = [
    "DELTA_MID",
    "GAMMA_MID",
    "VEGA_MID",
    "THETA_MID",
    "RHO_MID",
    "DELTA",
    "DELTA_BID",
    "DELTA_ASK",
    "GAMMA",
    "GAMMA_BID",
    "GAMMA_ASK",
    "VEGA",
    "VEGA_BID",
    "VEGA_ASK",
    "THETA_BID",
    "THETA_ASK",
    "RHO_BID",
    "RHO_ASK",
    "IVOL_MID",
    "IVOL_BID",
    "IVOL_ASK",
]

_OPTION_PRICING_FIELDS = [
    "PX_LAST",
    "PX_BID",
    "PX_ASK",
    "OPT_INTRINSIC_VAL",
    "OPT_TRUE_INTRINSIC",
    "OPT_TIME_VAL",
    "OPEN_INT",
    "PX_VOLUME",
    "OPEN_INT_CHANGE",
]

_OPTION_SCREEN_DEFAULT_FIELDS = [
    "NAME",
    "SECURITY_DES",
    "OPT_STRIKE_PX",
    "OPT_EXPIRE_DT",
    "OPT_PUT_CALL",
    "OPT_EXER_TYP",
    "OPT_UNDL_TICKER",
    "OPT_UNDL_PX",
    "PX_LAST",
    "PX_BID",
    "PX_ASK",
    "IVOL_MID",
    "DELTA_MID",
    "GAMMA_MID",
    "VEGA_MID",
    "THETA_MID",
    "RHO_MID",
    "OPEN_INT",
    "PX_VOLUME",
]

# Chain override names
_OVRD_CHAIN_PUT_CALL_TYPE = "CHAIN_PUT_CALL_TYPE_OVRD"
_OVRD_CHAIN_EXP_DT = "CHAIN_EXP_DT_OVRD"
_OVRD_CHAIN_STRIKE_PX = "CHAIN_STRIKE_PX_OVRD"
_OVRD_CHAIN_POINTS = "CHAIN_POINTS_OVRD"
_OVRD_CHAIN_PERIODICITY = "CHAIN_PERIODICITY_OVRD"
_OVRD_CHAIN_EXERCISE_TYPE = "CHAIN_EXERCISE_TYPE_OVRD"
_OVRD_CHAIN_EXP_MATCH = "CHAIN_EXP_MATCH_OVRD"


# =============================================================================
# Async implementations (primary)
# =============================================================================


async def aoption_info(ticker: str, **kwargs) -> IntoDataFrame:
    """Async get option contract metadata.

    Retrieves reference data for an option contract including strike price,
    expiration date, exercise type, and underlying security information.

    Args:
        ticker: Option ticker (e.g., "AAPL US 01/17/25 C200 Equity").
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with option contract metadata. Columns include:
            - OPT_STRIKE_PX: Strike price
            - OPT_EXPIRE_DT: Expiration date
            - OPT_PUT_CALL: Put/call indicator (P or C)
            - OPT_EXER_TYP: Exercise type (American or European)
            - OPT_UNDL_TICKER: Underlying security ticker
            - OPT_UNDL_PX: Underlying security price
            - OPT_CONT_SIZE: Contract size
            - OPT_MULTIPLIER: Price multiplier
            - NAME: Option name
            - SECURITY_DES: Security description

    Example::

        import asyncio
        from xbbg.ext.options import aoption_info


        async def main():
            df = await aoption_info("AAPL US 01/17/25 C200 Equity")
            print(df)


        asyncio.run(main())
    """
    return await _abdp_fields(tickers=ticker, fields=_OPTION_INFO_FIELDS, **kwargs)


async def aoption_greeks(ticker: str, **kwargs) -> IntoDataFrame:
    """Async get option Greeks and implied volatility.

    Retrieves Greeks (delta, gamma, vega, theta, rho) and implied volatility
    for an option contract. Includes both mid-market and bid/ask values.

    Args:
        ticker: Option ticker (e.g., "AAPL US 01/17/25 C200 Equity").
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with Greeks and implied volatility. Columns include:
            - DELTA_MID, DELTA, DELTA_BID, DELTA_ASK: Delta values
            - GAMMA_MID, GAMMA, GAMMA_BID, GAMMA_ASK: Gamma values
            - VEGA_MID, VEGA, VEGA_BID, VEGA_ASK: Vega values
            - THETA_MID, THETA_BID, THETA_ASK: Theta values
            - RHO_MID, RHO_BID, RHO_ASK: Rho values
            - IVOL_MID, IVOL_BID, IVOL_ASK: Implied volatility

    Example::

        import asyncio
        from xbbg.ext.options import aoption_greeks


        async def main():
            df = await aoption_greeks("AAPL US 01/17/25 C200 Equity")
            print(df)


        asyncio.run(main())
    """
    return await _abdp_fields(tickers=ticker, fields=_OPTION_GREEKS_FIELDS, **kwargs)


async def aoption_pricing(ticker: str, **kwargs) -> IntoDataFrame:
    """Async get option pricing and value decomposition.

    Retrieves option pricing data including last price, bid/ask, intrinsic
    value, time value, and open interest.

    Args:
        ticker: Option ticker (e.g., "AAPL US 01/17/25 C200 Equity").
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with option pricing data. Columns include:
            - PX_LAST: Last traded price
            - PX_BID: Bid price
            - PX_ASK: Ask price
            - OPT_INTRINSIC_VAL: Intrinsic value
            - OPT_TRUE_INTRINSIC: True intrinsic value
            - OPT_TIME_VAL: Time value
            - OPEN_INT: Open interest
            - PX_VOLUME: Trading volume
            - OPEN_INT_CHANGE: Change in open interest

    Example::

        import asyncio
        from xbbg.ext.options import aoption_pricing


        async def main():
            df = await aoption_pricing("AAPL US 01/17/25 C200 Equity")
            print(df)


        asyncio.run(main())
    """
    return await _abdp_fields(tickers=ticker, fields=_OPTION_PRICING_FIELDS, **kwargs)


async def aoption_chain(
    underlying: str,
    *,
    put_call: PutCall | str | None = None,
    expiry_dt: DateLike = None,
    strike: StrikeRef | str | float | None = None,
    points: float | None = None,
    periodicity: ChainPeriodicity | str | None = None,
    exercise_type: ExerciseType | str | None = None,
    expiry_match: ExpiryMatch | str | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Async get option chain via CHAIN_TICKERS overrides.

    Retrieves option chain for an underlying security using Bloomberg's
    CHAIN_TICKERS field with override parameters for filtering.

    Args:
        underlying: Underlying security ticker (e.g., "AAPL US Equity").
        put_call: Filter by put/call type. Use PutCall enum or "C"/"P".
        expiry_dt: Filter by expiration date (YYYY-MM-DD format).
        strike: Filter by strike price. Use StrikeRef.ATM or numeric value.
        points: Points around strike for filtering.
        periodicity: Filter by expiry periodicity. Use ChainPeriodicity enum.
        exercise_type: Filter by exercise type. Use ExerciseType enum.
        expiry_match: Expiry matching strategy. Use ExpiryMatch enum.
        **kwargs: Additional arguments passed to abds().

    Returns:
        DataFrame with option chain tickers. Columns include option tickers
        matching the specified filters.

    Example::

        import asyncio
        from xbbg.ext.options import aoption_chain, PutCall, ExpiryMatch


        async def main():
            # Get all call options
            df = await aoption_chain("AAPL US Equity", put_call=PutCall.CALL)

            # Get calls expiring on specific date
            df = await aoption_chain(
                "AAPL US Equity",
                put_call=PutCall.CALL,
                expiry_dt="2025-01-17",
            )

            # Get ATM calls
            df = await aoption_chain(
                "AAPL US Equity",
                put_call=PutCall.CALL,
                strike="ATM",
            )


        asyncio.run(main())
    """
    overrides: dict[str, str] = {}

    if put_call is not None:
        overrides[_OVRD_CHAIN_PUT_CALL_TYPE] = str(put_call)

    if expiry_dt is not None:
        formatted_expiry = _fmt_date(expiry_dt)
        if formatted_expiry is not None:
            overrides[_OVRD_CHAIN_EXP_DT] = formatted_expiry

    if strike is not None:
        if isinstance(strike, StrEnum):
            overrides[_OVRD_CHAIN_STRIKE_PX] = strike.value
        else:
            overrides[_OVRD_CHAIN_STRIKE_PX] = str(strike)

    if points is not None:
        overrides[_OVRD_CHAIN_POINTS] = str(points)

    if periodicity is not None:
        overrides[_OVRD_CHAIN_PERIODICITY] = str(periodicity)

    if exercise_type is not None:
        overrides[_OVRD_CHAIN_EXERCISE_TYPE] = str(exercise_type)

    if expiry_match is not None:
        overrides[_OVRD_CHAIN_EXP_MATCH] = str(expiry_match)

    # Merge with any additional overrides from kwargs
    if "overrides" in kwargs:
        existing = kwargs.pop("overrides")
        if isinstance(existing, dict):
            overrides.update(existing)
        elif isinstance(existing, list):
            for k, v in existing:
                overrides[k] = str(v)

    return await _abds_field(tickers=underlying, field="CHAIN_TICKERS", overrides=overrides, **kwargs)


async def aoption_chain_bql(
    underlying: str,
    *,
    put_call: PutCall | str | None = None,
    expiry_start: str | None = None,
    expiry_end: str | None = None,
    strike_low: float | None = None,
    strike_high: float | None = None,
    delta_low: float | None = None,
    delta_high: float | None = None,
    gamma_low: float | None = None,
    gamma_high: float | None = None,
    vega_low: float | None = None,
    vega_high: float | None = None,
    theta_low: float | None = None,
    theta_high: float | None = None,
    ivol_low: float | None = None,
    ivol_high: float | None = None,
    moneyness_low: float | None = None,
    moneyness_high: float | None = None,
    min_open_int: float | None = None,
    min_volume: float | None = None,
    min_bid: float | None = None,
    max_ask: float | None = None,
    exch_code: str | None = None,
    exercise_type: ExerciseType | str | None = None,
    extra_filters: str | None = None,
    get_fields: list[str] | None = None,
    **kwargs,
) -> Any:
    """Async get option chain via BQL with rich filtering.

    Retrieves option chain using Bloomberg Query Language (BQL) with
    comprehensive filtering on Greeks, volatility, moneyness, and activity.

    Args:
        underlying: Underlying security ticker (e.g., "AAPL US Equity").
        put_call: Filter by put/call type. Use PutCall enum or "C"/"P".
        expiry_start: Start of expiry date range (YYYY-MM-DD format).
        expiry_end: End of expiry date range (YYYY-MM-DD format).
        strike_low: Minimum strike price.
        strike_high: Maximum strike price.
        delta_low: Minimum delta value.
        delta_high: Maximum delta value.
        gamma_low: Minimum gamma value.
        gamma_high: Maximum gamma value.
        vega_low: Minimum vega value.
        vega_high: Maximum vega value.
        theta_low: Minimum theta value.
        theta_high: Maximum theta value.
        ivol_low: Minimum implied volatility.
        ivol_high: Maximum implied volatility.
        moneyness_low: Minimum moneyness ratio.
        moneyness_high: Maximum moneyness ratio.
        min_open_int: Minimum open interest.
        min_volume: Minimum trading volume.
        min_bid: Minimum bid price.
        max_ask: Maximum ask price.
        exch_code: Exchange code filter.
        exercise_type: Filter by exercise type. Use ExerciseType enum.
        extra_filters: Additional BQL filter expressions.
        get_fields: Custom list of fields to retrieve. Default fields:
            - strike_px(): Strike price
            - expire_dt(): Expiration date
            - ivol(): Implied volatility
            - delta(): Delta
            - open_int(): Open interest
            - px_last(): Last price
            - px_bid(): Bid price
            - px_ask(): Ask price
        **kwargs: Additional arguments passed to abql().

    Returns:
        DataFrame with option chain data filtered by specified criteria.

    Example::

        import asyncio
        from xbbg.ext.options import aoption_chain_bql, PutCall


        async def main():
            # Get all calls expiring in January 2025
            df = await aoption_chain_bql(
                "AAPL US Equity",
                put_call=PutCall.CALL,
                expiry_start="2025-01-01",
                expiry_end="2025-01-31",
            )

            # Get ATM calls with high open interest
            df = await aoption_chain_bql(
                "AAPL US Equity",
                put_call=PutCall.CALL,
                delta_low=0.4,
                delta_high=0.6,
                min_open_int=10000,
            )

            # Get puts with custom fields
            df = await aoption_chain_bql(
                "AAPL US Equity",
                put_call=PutCall.PUT,
                get_fields=["id", "strike_px()", "px_last()", "ivol()"],
            )


        asyncio.run(main())
    """
    from xbbg import blp

    # Default get fields
    default_get = [
        "strike_px()",
        "expire_dt()",
        "ivol()",
        "delta()",
        "open_int()",
        "px_last()",
        "px_bid()",
        "px_ask()",
    ]
    get_list = get_fields or default_get
    get_clause = ", ".join(get_list)

    # Build filters
    filters: list[str] = []

    if put_call is not None:
        pc_val = "Call" if str(put_call).upper() in ("C", "CALL") else "Put"
        filters.append(f"put_call=='{pc_val}'")

    if expiry_start is not None:
        filters.append(f"expire_dt()>='{expiry_start}'")

    if expiry_end is not None:
        filters.append(f"expire_dt()<='{expiry_end}'")

    if strike_low is not None and strike_high is not None:
        filters.append(f"between(strike_px(), {strike_low}, {strike_high})")
    elif strike_low is not None:
        filters.append(f"strike_px()>={strike_low}")
    elif strike_high is not None:
        filters.append(f"strike_px()<={strike_high}")

    if delta_low is not None and delta_high is not None:
        filters.append(f"between(delta(), {delta_low}, {delta_high})")
    elif delta_low is not None:
        filters.append(f"delta()>={delta_low}")
    elif delta_high is not None:
        filters.append(f"delta()<={delta_high}")

    if gamma_low is not None and gamma_high is not None:
        filters.append(f"between(gamma(), {gamma_low}, {gamma_high})")
    elif gamma_low is not None:
        filters.append(f"gamma()>={gamma_low}")
    elif gamma_high is not None:
        filters.append(f"gamma()<={gamma_high}")

    if vega_low is not None and vega_high is not None:
        filters.append(f"between(vega(), {vega_low}, {vega_high})")
    elif vega_low is not None:
        filters.append(f"vega()>={vega_low}")
    elif vega_high is not None:
        filters.append(f"vega()<={vega_high}")

    if theta_low is not None and theta_high is not None:
        filters.append(f"between(theta(), {theta_low}, {theta_high})")
    elif theta_low is not None:
        filters.append(f"theta()>={theta_low}")
    elif theta_high is not None:
        filters.append(f"theta()<={theta_high}")

    if ivol_low is not None and ivol_high is not None:
        filters.append(f"between(ivol(), {ivol_low}, {ivol_high})")
    elif ivol_low is not None:
        filters.append(f"ivol()>={ivol_low}")
    elif ivol_high is not None:
        filters.append(f"ivol()<={ivol_high}")

    if moneyness_low is not None and moneyness_high is not None:
        filters.append(f"between(moneyness(), {moneyness_low}, {moneyness_high})")
    elif moneyness_low is not None:
        filters.append(f"moneyness()>={moneyness_low}")
    elif moneyness_high is not None:
        filters.append(f"moneyness()<={moneyness_high}")

    if min_open_int is not None:
        filters.append(f"open_int()>={min_open_int}")

    if min_volume is not None:
        filters.append(f"px_volume()>={min_volume}")

    if min_bid is not None:
        filters.append(f"px_bid()>={min_bid}")

    if max_ask is not None:
        filters.append(f"px_ask()<={max_ask}")

    if exch_code is not None:
        filters.append(f"exch_code()=='{exch_code}'")

    if exercise_type is not None:
        filters.append(f"exercise_type()=='{exercise_type}'")

    if extra_filters is not None:
        filters.append(extra_filters)

    # Build universe and for clause
    universe = f"options('{underlying}')"
    if filters:
        filter_str = " and ".join(filters)
        for_clause = f"filter({universe}, {filter_str})"
    else:
        for_clause = universe

    query = f"get({get_clause}) for({for_clause})"

    return await blp.abql(query, **kwargs)


async def aoption_screen(
    tickers: list[str],
    flds: list[str] | None = None,
    **kwargs,
) -> IntoDataFrame:
    """Async screen multiple options with custom fields.

    Retrieves option data for multiple tickers with customizable field selection.
    Useful for comparing options across different strikes and expirations.

    Args:
        tickers: List of option tickers to screen.
        flds: List of fields to retrieve. Default fields include:
            - NAME: Option name
            - SECURITY_DES: Security description
            - OPT_STRIKE_PX: Strike price
            - OPT_EXPIRE_DT: Expiration date
            - OPT_PUT_CALL: Put/call indicator
            - OPT_EXER_TYP: Exercise type
            - OPT_UNDL_TICKER: Underlying ticker
            - OPT_UNDL_PX: Underlying price
            - PX_LAST: Last price
            - PX_BID: Bid price
            - PX_ASK: Ask price
            - IVOL_MID: Implied volatility (mid)
            - DELTA_MID: Delta (mid)
            - GAMMA_MID: Gamma (mid)
            - VEGA_MID: Vega (mid)
            - THETA_MID: Theta (mid)
            - RHO_MID: Rho (mid)
            - OPEN_INT: Open interest
            - PX_VOLUME: Trading volume
        **kwargs: Additional arguments passed to abdp().

    Returns:
        DataFrame with option screening data. Rows are options, columns are
        the requested fields.

    Example::

        import asyncio
        from xbbg.ext.options import aoption_screen


        async def main():
            # Screen multiple options
            tickers = [
                "AAPL US 01/17/25 C200 Equity",
                "AAPL US 01/17/25 C210 Equity",
                "AAPL US 01/17/25 P190 Equity",
            ]
            df = await aoption_screen(tickers)
            print(df)

            # Screen with custom fields
            df = await aoption_screen(
                tickers,
                flds=["NAME", "OPT_STRIKE_PX", "PX_LAST", "IVOL_MID", "DELTA_MID"],
            )


        asyncio.run(main())
    """
    return await _abdp_fields(
        tickers=tickers,
        fields=flds or _OPTION_SCREEN_DEFAULT_FIELDS,
        **kwargs,
    )


option_info = _syncify(aoption_info)
option_greeks = _syncify(aoption_greeks)
option_pricing = _syncify(aoption_pricing)
option_chain = _syncify(aoption_chain)
option_chain_bql = _syncify(aoption_chain_bql)
option_screen = _syncify(aoption_screen)


__all__ = [
    # Enums
    "PutCall",
    "ChainPeriodicity",
    "StrikeRef",
    "ExerciseType",
    "ExpiryMatch",
    # Async functions
    "aoption_info",
    "aoption_greeks",
    "aoption_pricing",
    "aoption_chain",
    "aoption_chain_bql",
    "aoption_screen",
    # Sync functions
    "option_info",
    "option_greeks",
    "option_pricing",
    "option_chain",
    "option_chain_bql",
    "option_screen",
]
