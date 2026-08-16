"""Funding-interval resolution and the fee-aware entry gate.

Two defects these lock down, both of which silently selected losing trades:

  - interval detection that read a single gap and defaulted to 4h on failure, which
    DOUBLED the reported APR on every 8h symbol (BTC/ETH/SOL/BNB/DOGE/LINK/LTC/XRP);
  - an entry gate comparing gross funding APR against a threshold with no cost term
    anywhere, which is how three "successful" cycles netted $0.00.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from funding_economics import (  # noqa: E402
    FundingIntervalResolver,
    IntervalResolutionError,
    TradeCostModel,
    VenueCosts,
    VERIFIED_TAKER_BPS,
    annualize,
    break_even_apr_pct,
    evaluate_entry,
    recommended_hold_hours,
)
from lighter_aster_hedge import BotConfig, build_cost_model  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HOUR_MS = 3_600_000


def times(interval_h, n=12, start=1_700_000_000_000):
    return [start + i * int(interval_h * HOUR_MS) for i in range(n)]


# ------------------------------------------------------- interval resolution

@pytest.mark.asyncio
@pytest.mark.parametrize("hours", [1.0, 4.0, 8.0])
async def test_api_field_is_preferred_and_cached(hours):
    calls = []

    async def fetch(symbol):
        calls.append(symbol)
        return hours

    resolver = FundingIntervalResolver()
    first = await resolver.resolve_from_api_field("aster", "BTCUSDT", fetch)
    second = await resolver.resolve_from_api_field("aster", "BTCUSDT", fetch)

    assert first.hours == hours
    assert first.source == "api_field"
    assert first.periods_per_day == pytest.approx(24.0 / hours)
    assert len(calls) == 1, "a resolved interval must be cached, not refetched"
    assert second.hours == hours


@pytest.mark.asyncio
async def test_a_failed_lookup_is_never_cached():
    """The original bug: one transient error pinned a symbol at the wrong interval."""
    attempts = []

    async def flaky(symbol):
        attempts.append(symbol)
        if len(attempts) == 1:
            raise RuntimeError("transient 500")
        return 8.0

    resolver = FundingIntervalResolver()
    with pytest.raises(IntervalResolutionError):
        await resolver.resolve_from_api_field("aster", "BTCUSDT", flaky)

    recovered = await resolver.resolve_from_api_field("aster", "BTCUSDT", flaky)
    assert recovered.hours == 8.0


@pytest.mark.asyncio
async def test_api_field_returning_nothing_is_an_error_not_a_default():
    async def empty(symbol):
        return None

    resolver = FundingIntervalResolver()
    with pytest.raises(IntervalResolutionError):
        await resolver.resolve_from_api_field("aster", "BTCUSDT", empty)


@pytest.mark.asyncio
@pytest.mark.parametrize("hours", [1.0, 4.0, 8.0])
async def test_empirical_resolution_recovers_the_true_interval(hours):
    async def fetch(symbol):
        return times(hours)

    resolver = FundingIntervalResolver()
    interval = await resolver.resolve_empirically("aster", "BTCUSDT", fetch)
    assert interval.hours == pytest.approx(hours)
    assert interval.source == "empirical"


@pytest.mark.asyncio
async def test_two_samples_are_refused():
    """One gap is not evidence: a single irregular settlement would set the constant."""
    async def only_two(symbol):
        return times(8.0, n=2)

    resolver = FundingIntervalResolver()
    with pytest.raises(IntervalResolutionError, match="need"):
        await resolver.resolve_empirically("aster", "BTCUSDT", only_two)


@pytest.mark.asyncio
async def test_disagreeing_gaps_refuse_to_guess():
    async def erratic(symbol):
        base = 1_700_000_000_000
        offsets = [0, 1, 3, 4, 9, 11, 12, 20, 21, 30]     # no dominant gap
        return [base + int(o * HOUR_MS) for o in offsets]

    resolver = FundingIntervalResolver()
    with pytest.raises(IntervalResolutionError, match="do not agree"):
        await resolver.resolve_empirically("aster", "BTCUSDT", erratic)


def test_lighter_is_hourly_and_annualises_as_a_decimal():
    interval = FundingIntervalResolver().constant("lighter")
    assert interval.hours == 1.0
    # 0.0001/hour -> 0.01%/hour -> 87.6% APR. The old periods_per_day=3 gave 10.95%,
    # understating by exactly 8x.
    assert annualize(0.0001, interval) == pytest.approx(87.6)


def test_an_8h_symbol_priced_as_4h_doubles_its_apr():
    """Quantifies what the old `periods_per_day = 6` default did to BTC/ETH/SOL/..."""
    resolver = FundingIntervalResolver()
    true_8h = resolver.constant("hyperliquid").__class__(8.0, "test", 0.0)
    wrong_4h = resolver.constant("hyperliquid").__class__(4.0, "test", 0.0)

    assert annualize(0.0001, wrong_4h) == pytest.approx(2 * annualize(0.0001, true_8h))


def test_unknown_venue_has_no_constant():
    with pytest.raises(IntervalResolutionError):
        FundingIntervalResolver().constant("aster")      # per-symbol, never a constant


# ------------------------------------------------------------- entry gate

def test_break_even_matches_the_documented_figures():
    cost = TradeCostModel(legs=(
        VenueCosts("aster", VERIFIED_TAKER_BPS["aster"]),
        VenueCosts("lighter", VERIFIED_TAKER_BPS["lighter"]),
    ))
    assert cost.roundtrip_pct() == pytest.approx(0.080)
    assert break_even_apr_pct(cost, 8.0) == pytest.approx(87.6)
    assert break_even_apr_pct(cost, 24.0) == pytest.approx(29.2)


def test_break_even_scales_inversely_with_hold_length():
    cost = TradeCostModel(legs=(VenueCosts("aster", 4.0), VenueCosts("lighter", 0.0)))
    assert break_even_apr_pct(cost, 8.0) == pytest.approx(3 * break_even_apr_pct(cost, 24.0))


def test_recommended_hold_inverts_break_even():
    cost = TradeCostModel(legs=(VenueCosts("aster", 4.0), VenueCosts("lighter", 0.0)))
    hold = recommended_hold_hours(cost, 29.2)
    assert hold == pytest.approx(24.0, rel=1e-3)


def test_zero_hold_is_rejected_rather_than_dividing_by_zero():
    cost = TradeCostModel(legs=(VenueCosts("aster", 4.0),))
    with pytest.raises(ValueError):
        break_even_apr_pct(cost, 0.0)


@pytest.mark.parametrize("gross,accept", [
    (20.0, False),      # below break-even outright
    (40.0, False),      # clears break-even gross, but not after the haircut
    (60.0, False),      # the old config floor - still under the 2x margin requirement
    (90.0, True),
    (200.0, True),
])
def test_gate_requires_margin_over_break_even_not_merely_a_positive_spread(gross, accept):
    cost = TradeCostModel(legs=(
        VenueCosts("aster", VERIFIED_TAKER_BPS["aster"]),
        VenueCosts("lighter", VERIFIED_TAKER_BPS["lighter"]),
    ))
    decision = evaluate_entry(
        symbol="BTCUSDT", gross_net_apr_pct=gross, notional_usd=1000.0,
        hold_hours=24.0, cost=cost,
    )
    assert decision.accept is accept
    assert decision.reason


def test_the_old_gate_would_have_accepted_a_structural_loser():
    """5% APR against an 87.6% break-even - the configuration that ran for three cycles."""
    cost = TradeCostModel(legs=(
        VenueCosts("aster", VERIFIED_TAKER_BPS["aster"]),
        VenueCosts("lighter", VERIFIED_TAKER_BPS["lighter"]),
    ))
    decision = evaluate_entry(
        symbol="LTCUSDT", gross_net_apr_pct=5.0, notional_usd=1000.0,
        hold_hours=8.0, cost=cost,
    )
    assert 5.0 >= 5.0                       # the old threshold said yes
    assert not decision.accept              # the cost model says no
    assert decision.expected_net_usd < 0


def test_rejections_still_report_their_economics():
    """The table has to show break-even and margin on rejected rows too."""
    cost = TradeCostModel(legs=(VenueCosts("aster", 4.0), VenueCosts("lighter", 0.0)))
    decision = evaluate_entry(
        symbol="X", gross_net_apr_pct=1.0, notional_usd=1000.0,
        hold_hours=24.0, cost=cost,
    )
    assert not decision.accept
    assert decision.break_even_apr_pct > 0
    assert decision.margin_ratio >= 0
    assert decision.expected_cost_usd > 0


# --------------------------------------------------------- bot integration

def test_bot_cost_model_reproduces_the_historical_round_trip():
    config = BotConfig.load_from_file(os.path.join(REPO_ROOT, "config.json"))
    assert build_cost_model(config).roundtrip_pct() == pytest.approx(0.080)


def test_configured_hold_clears_break_even_by_the_documented_margin():
    config = BotConfig.load_from_file(os.path.join(REPO_ROOT, "config.json"))
    break_even = break_even_apr_pct(build_cost_model(config), config.hold_duration_hours)
    assert break_even == pytest.approx(29.2)
    assert config.min_net_apr_threshold > break_even
