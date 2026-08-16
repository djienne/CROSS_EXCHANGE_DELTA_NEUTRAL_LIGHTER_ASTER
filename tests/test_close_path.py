"""close_delta_neutral_position must RAISE when a leg is not flat.

The regression: it computed `aster_closed` / `lighter_closed`, printed
"⚠️ WARNING: One or more positions not fully closed", and then returned normally. The
caller took that as success, appended the cycle with `status: "success"` and cleared
`current_position` - so a still-open leg was left with no monitoring, no stop-loss, and
no record anywhere that it existed.

unwind_leg's own retry/halt behaviour is covered in test_two_leg.py; these tests pin the
wiring between it and the bot.
"""
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import lighter_aster_hedge as bot  # noqa: E402

ENV = {
    "LIGHTER_BASE_URL": "https://example.invalid",
    "API_KEY_PRIVATE_KEY": "0xdeadbeef",
    "ACCOUNT_INDEX": 0,
    "API_KEY_INDEX": 0,
    "MARGIN_MODE": "cross",
}


class FakeAster:
    def __init__(self, position=1.0):
        self.position = position
        self.closes = []
        self.cancelled = 0

    async def get_perp_symbol_filter(self, symbol, filter_type):
        return {"stepSize": "0.001"}

    async def get_perp_account_info(self):
        return {"positions": [{"symbol": "BTCUSDT", "positionAmt": str(self.position)}]}

    async def close_perp_position(self, symbol, quantity, side):
        self.closes.append((quantity, side))
        return {"orderId": 1}

    async def cancel_all_perp_orders(self, symbol):
        self.cancelled += 1
        return 0


@pytest.fixture
def patched(monkeypatch, tmp_path):
    """Stub every network edge close_delta_neutral_position touches."""
    monkeypatch.chdir(tmp_path)      # any halt.json lands in the tmp dir

    class FakeSigner:
        def __init__(self, **kwargs):
            pass

        def check_client(self):
            return None

        async def close(self):
            return None

    class FakeApiClient:
        def __init__(self, **kwargs):
            pass

        async def close(self):
            return None

    fake_lighter = types.SimpleNamespace(
        ApiClient=lambda **kw: FakeApiClient(),
        Configuration=lambda **kw: object(),
        OrderApi=lambda c: object(),
        AccountApi=lambda c: object(),
        SignerClient=lambda **kw: FakeSigner(),
    )
    monkeypatch.setattr(bot, "lighter", fake_lighter)

    async def market_details(order_api, symbol):
        return (1, 0.01, 0.001)          # market_id, price_tick, amount_tick

    monkeypatch.setattr(bot.lighter_client, "get_lighter_market_details", market_details)

    async def open_size(*args, **kwargs):
        return -1.0                      # short on Lighter, hedging a long on Aster

    monkeypatch.setattr(bot.lighter_client, "get_lighter_open_size", open_size)
    return monkeypatch


@pytest.mark.asyncio
async def test_close_raises_when_a_leg_cannot_be_flattened(patched):
    """The exact scenario that used to print a warning and report success."""
    async def unwind_fails(leg, **kwargs):
        return False                     # unwind_leg has already written halt.json

    patched.setattr(bot, "unwind_leg", unwind_fails)

    with pytest.raises(RuntimeError, match="Close incomplete"):
        await bot.close_delta_neutral_position(ENV, FakeAster(), "BTCUSDT")


@pytest.mark.asyncio
async def test_close_raises_when_an_unwind_blows_up(patched):
    async def unwind_raises(leg, **kwargs):
        raise ConnectionError("venue unreachable")

    patched.setattr(bot, "unwind_leg", unwind_raises)

    with pytest.raises(RuntimeError, match="Close incomplete"):
        await bot.close_delta_neutral_position(ENV, FakeAster(), "BTCUSDT")


@pytest.mark.asyncio
async def test_close_returns_normally_when_both_legs_flatten(patched):
    """Aster already flat, Lighter short: only the leg with a position is unwound."""
    unwound = []

    async def unwind_ok(leg, **kwargs):
        unwound.append(leg.name)
        return True

    patched.setattr(bot, "unwind_leg", unwind_ok)

    await bot.close_delta_neutral_position(ENV, FakeAster(position=0.0), "BTCUSDT")
    assert unwound == ["Lighter"]


@pytest.mark.asyncio
async def test_nothing_to_close_is_not_an_error(patched):
    async def flat(*args, **kwargs):
        return 0.0

    patched.setattr(bot.lighter_client, "get_lighter_open_size", flat)

    called = []
    patched.setattr(bot, "unwind_leg",
                    lambda leg, **kw: called.append(leg.name))

    await bot.close_delta_neutral_position(ENV, FakeAster(position=0.0), "BTCUSDT")
    assert called == [], "no orders may be sent when both venues are already flat"


@pytest.mark.asyncio
async def test_unreadable_lighter_position_aborts_the_close(patched):
    """A failed read is not a flat position - it must not proceed to 'closed'."""
    async def unreadable(*args, **kwargs):
        raise bot.lighter_client.PositionFetchError("rpc timeout")

    patched.setattr(bot.lighter_client, "get_lighter_open_size", unreadable)

    with pytest.raises(bot.lighter_client.PositionFetchError):
        await bot.close_delta_neutral_position(ENV, FakeAster(), "BTCUSDT")


@pytest.mark.asyncio
async def test_both_legs_are_unwound_when_both_hold_a_position(patched):
    """Lighter -1.0 (fixture) against Aster +1.0: both must be flattened."""
    unwound = []

    async def unwind_ok(leg, **kwargs):
        unwound.append(leg.name)
        return True

    patched.setattr(bot, "unwind_leg", unwind_ok)

    await bot.close_delta_neutral_position(ENV, FakeAster(position=1.0), "BTCUSDT")
    assert sorted(unwound) == ["Aster", "Lighter"]
