"""Two-leg execution safety, driven entirely with fakes - no network.

These bots cannot be validated with real money, so this is the only verification
available for the failure modes that actually cost money:

  - a rejected leg reported as a fill (Lighter returns None/False, not an exception),
  - a hedge that never landed, leaving the pilot naked,
  - an unwind that could not complete and was not made loud,
  - a resting GOOD_TILL_TIME remainder filling after the bot moved on.
"""
import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from two_leg import (  # noqa: E402
    HaltedError,
    LegSpec,
    LegStatus,
    assert_not_halted,
    classify_submission,
    execute_two_leg,
    unwind_leg,
)

FAST = dict(verify_timeout_s=2.0, verify_poll_interval_s=0.01, unwind_backoff_base_s=0.01)


class FakeVenue:
    """A venue whose position moves only as much as `fill_ratio` of each submission.

    `submit_returns` mimics how each real client signals failure: Lighter returns None
    or False, Aster raises, EdgeX returns a dict with a code.
    """

    def __init__(self, name, side, amount_tick=0.001, fill_ratio=1.0,
                 submit_returns="order-123", submit_raises=None, resting=0):
        self.name = name
        self.side = side
        self.amount_tick = amount_tick
        self.fill_ratio = fill_ratio
        self.submit_returns = submit_returns
        self.submit_raises = submit_raises
        self.position = 0.0
        self.resting = resting
        self.submits = []
        self.cancels = 0
        self.closes = []

    async def submit(self, qty):
        self.submits.append(qty)
        if self.submit_raises is not None:
            raise self.submit_raises
        if self.submit_returns in (None, False):
            return self.submit_returns
        filled = qty * self.fill_ratio
        self.position += filled if self.side == "buy" else -filled
        return self.submit_returns

    async def read_position(self):
        return self.position

    async def close_market(self, qty, side):
        self.closes.append((qty, side))
        delta = qty if side == "buy" else -qty
        self.position += delta
        if abs(self.position) < self.amount_tick / 2:
            self.position = 0.0
        return "closed"

    async def cancel_open(self):
        self.cancels += 1
        n, self.resting = self.resting, 0
        return n

    def spec(self, intent_qty):
        return LegSpec(
            name=self.name, symbol="BTC", side=self.side, intent_qty=intent_qty,
            submit=self.submit, read_position=self.read_position,
            close_market=self.close_market, cancel_open=self.cancel_open,
            amount_tick=self.amount_tick, settle_delay_s=0.0,
        )


# ---------------------------------------------------------------- classify

@pytest.mark.parametrize("raw,expected", [
    (None, LegStatus.REJECTED),                    # Lighter failure return
    (False, LegStatus.REJECTED),                   # Lighter close failure return
    (True, LegStatus.UNKNOWN),                     # accepted != filled
    ("order-1", LegStatus.UNKNOWN),
    ({"code": "FAIL", "msg": "nope"}, LegStatus.REJECTED),
    ({"code": "SUCCESS", "order_id": 7}, LegStatus.UNKNOWN),
    (RuntimeError("rejected"), LegStatus.REJECTED),
    (asyncio.TimeoutError(), LegStatus.UNKNOWN),   # may well be resting
])
def test_submission_never_yields_filled(raw, expected):
    result = classify_submission("V", raw, intent_qty=1.0, symbol="BTC", side="buy")
    assert result.status is expected
    assert result.status is not LegStatus.FILLED


# ---------------------------------------------------------------- happy path

@pytest.mark.asyncio
async def test_both_legs_fill_reports_hedged_quantity():
    pilot = FakeVenue("Lighter", "buy")
    hedge = FakeVenue("Aster", "sell")

    outcome = await execute_two_leg(pilot.spec(1.0), hedge.spec(1.0), **FAST)

    assert outcome.ok
    assert outcome.hedged_qty == pytest.approx(1.0)
    assert pilot.position == pytest.approx(1.0)
    assert hedge.position == pytest.approx(-1.0)


@pytest.mark.asyncio
async def test_hedge_is_sized_from_the_pilots_actual_fill_not_the_intent():
    """The core sizing rule: a partial pilot must not get a full-size hedge."""
    pilot = FakeVenue("Lighter", "buy", fill_ratio=0.6)
    hedge = FakeVenue("Aster", "sell")

    outcome = await execute_two_leg(pilot.spec(1.0), hedge.spec(1.0), **FAST)

    assert outcome.ok
    assert hedge.submits[0] == pytest.approx(0.6)
    assert outcome.hedged_qty == pytest.approx(0.6)
    assert pilot.position + hedge.position == pytest.approx(0.0)


# ---------------------------------------------------------------- failures

@pytest.mark.asyncio
async def test_pilot_rejected_leaves_nothing_live_and_never_hedges():
    pilot = FakeVenue("Lighter", "buy", submit_returns=None)   # Lighter's failure return
    hedge = FakeVenue("Aster", "sell")

    outcome = await execute_two_leg(pilot.spec(1.0), hedge.spec(1.0), **FAST)

    assert not outcome.ok
    assert "nothing live" in outcome.reason
    assert hedge.submits == [], "hedge must never be sent after a rejected pilot"
    assert pilot.position == 0.0


@pytest.mark.asyncio
async def test_rejected_hedge_unwinds_the_pilot_rather_than_holding_it_naked():
    """This is the naked-leg bug: Aster fails, Lighter stays long, bot says 'hedged'."""
    pilot = FakeVenue("Lighter", "buy")
    hedge = FakeVenue("Aster", "sell", submit_raises=RuntimeError("insufficient margin"))

    outcome = await execute_two_leg(pilot.spec(1.0), hedge.spec(1.0), **FAST)

    assert not outcome.ok
    assert "hedge rejected" in outcome.reason
    assert pilot.position == pytest.approx(0.0), "pilot must be flattened"
    assert pilot.closes, "an unwind order must actually have been sent"
    assert not outcome.halted


@pytest.mark.asyncio
async def test_resting_orders_are_cancelled_before_the_position_is_read():
    """A zero read only means zero once nothing can still fill."""
    pilot = FakeVenue("Lighter", "buy", resting=2)
    hedge = FakeVenue("Aster", "sell")

    await execute_two_leg(pilot.spec(1.0), hedge.spec(1.0), **FAST)

    assert pilot.cancels >= 2, "swept pre-open and again before verification"
    assert hedge.cancels >= 1


# ---------------------------------------------------------------- halt

@pytest.mark.asyncio
async def test_failed_unwind_writes_a_halt_sentinel(tmp_path):
    halt_path = str(tmp_path / "halt.json")

    stuck = FakeVenue("Lighter", "buy")
    stuck.position = 1.0                       # starts long

    async def close_that_does_nothing(qty, side):
        return "accepted"                      # never actually reduces
    stuck.close_market = close_that_does_nothing

    ok = await unwind_leg(stuck.spec(1.0), baseline_signed_qty=0.0,
                          attempts=2, halt_path=halt_path, backoff_base_s=0.01)

    assert ok is False
    assert os.path.exists(halt_path)
    payload = json.loads(open(halt_path, encoding="utf-8").read())
    assert payload["venue"] == "Lighter"
    assert payload["residual_qty"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_a_halt_sentinel_blocks_the_next_open(tmp_path):
    halt_path = str(tmp_path / "halt.json")
    with open(halt_path, "w", encoding="utf-8") as fh:
        json.dump({"reason": "unwind failed", "venue": "Lighter"}, fh)

    pilot, hedge = FakeVenue("Lighter", "buy"), FakeVenue("Aster", "sell")
    with pytest.raises(HaltedError):
        await execute_two_leg(pilot.spec(1.0), hedge.spec(1.0),
                              halt_path=halt_path, **FAST)

    assert pilot.submits == [], "nothing may be submitted while halted"


def test_an_unreadable_sentinel_still_counts_as_halted(tmp_path):
    """Failing open here would defeat the entire mechanism."""
    halt_path = str(tmp_path / "halt.json")
    open(halt_path, "w", encoding="utf-8").write("{corrupt")

    with pytest.raises(HaltedError):
        assert_not_halted(halt_path)
