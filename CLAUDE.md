# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **cross-exchange delta-neutral trading bot** that implements funding rate arbitrage between two perpetual futures exchanges: **Lighter** and **Aster**. The bot automatically opens market-neutral positions (long on one exchange, short on the other), holds them to collect funding payments, then closes and repeats the cycle.

**Core Strategy**: Profit from funding rate differentials while maintaining zero directional exposure through delta-neutral hedging.

## Running the Bot

### Basic Commands

```bash
# Run the main bot
python lighter_aster_hedge.py

# Run with custom configuration
python lighter_aster_hedge.py --config config.json --state-file bot_state.json

# Check current positions (read-only)
python check_lighter_positions.py
python check_lighter_positions.py --symbol BTC

# Emergency exit (close all positions)
python emergency_exit.py
```

### Docker Commands

```bash
# Build and run with Docker Compose
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the bot
docker-compose down

# Rebuild after code changes
docker-compose up -d --build
```

## Architecture

### Bot State Machine

The main bot (`lighter_aster_hedge.py`) operates as a state machine with these states:

1. **IDLE** → Waiting to start new cycle
2. **ANALYZING** → Fetching funding rates, calculating opportunities
3. **OPENING** → Placing orders on both exchanges
4. **HOLDING** → Monitoring position, waiting for hold duration
5. **CLOSING** → Closing positions on both exchanges
6. **WAITING** → Brief pause between cycles
7. **ERROR** → Recovery mode (attempts to return to IDLE)
8. **SHUTDOWN** → Graceful shutdown in progress

### Key Components

**Main Bot Logic** (`lighter_aster_hedge.py`):
- `main_loop()` - Core state machine loop with capital validation
- `fetch_symbol_funding()` - Fetches current/upcoming funding rates with dynamic interval detection
- `open_delta_neutral_position()` - Opens hedged positions with affordable notional
- `close_delta_neutral_position()` - Closes hedged positions
- `verify_and_recover_position()` - Position recovery on restart
- `update_capital_status()` - Fetches balances and calculates max affordable position
- `calculate_affordable_notional()` - Validates and adjusts position size based on available capital
- `fetch_and_display_funding_rates()` - Displays opportunity analysis table
- `get_aster_balance()` / `get_lighter_balance()` - Balance fetchers for both exchanges

**Exchange Connectors** (avoid modifying; see exception below):
- `lighter_client.py` - Lighter exchange API wrapper
- `aster_api_manager.py` - Aster exchange API wrapper
- `utils.py` - Helper utilities for Aster connector
- `strategy_logic.py` - Required stub for Aster connector

> **Exception.** The two-leg primitive requires capabilities the upstream connectors did
> not have, so these were added here and must be mirrored back to the source projects:
> `aster_api_manager.get_perp_open_orders / cancel_all_perp_orders / get_funding_info /
> get_all_perp_positions`, and `lighter_client.lighter_cancel_open_orders /
> list_lighter_positions`.

**Shared Safety Modules** (byte-identical across every bot in this family — change here
and re-copy, never fork):
- `two_leg.py` - two-leg execution, fill verification, unwind, halt sentinel,
  boot reconciliation
- `funding_economics.py` - funding-interval resolution and the fee-aware entry gate

**Utility Scripts**:
- `check_lighter_positions.py` - Standalone position checker
- `emergency_exit.py` - Emergency position closer with PnL display

### Data Flow

1. **Funding Rate Analysis**:
   - Fetch from Aster: `aster.get_premium_index()` → Current/upcoming rate (not historical)
   - Detect funding interval: prefer `fundingIntervalHours` from `/fapi/v1/fundingInfo`;
     fall back to the mode of `get_funding_rate_history()` timestamp deltas (1h, 4h or 8h)
   - Fetch from Lighter: `lighter_client.get_lighter_funding_rate()` → ⚠️ periodicity and
     units both UNVERIFIED, see "Funding Rate Calculations" below
   - Calculate annualized APR: `rate * periods_per_day * 365 * 100` (periods_per_day detected dynamically for Aster)
   - Determine optimal direction: `max(aster_apr - lighter_apr, lighter_apr - aster_apr)`
   - Filter by spread and APR threshold

2. **Capital Validation**:
   - Fetch balances: `update_capital_status()` calls both exchanges
   - Calculate max affordable: `min(aster_available, lighter_available) * leverage * safety_margin`
   - Adjust position size: `calculate_affordable_notional()` reduces if needed
   - Skip trade if insufficient capital (< minimum threshold)
   - Log warnings when position size is adjusted

3. **Position Opening**:
   - Calculate size from affordable notional: `notional / avg_mid_price`
   - Round to tick sizes: Use `_floor_to_tick()` for both exchanges
   - Set leverage: Call both exchanges concurrently
   - Place orders: `asyncio.gather()` for concurrent execution
   - Verify: Check actual positions via API
   - Store actual vs requested notional in position metadata

4. **Position Monitoring**:
   - Check every `check_interval_seconds` (default: 60s)
   - Display position size in crypto units and USD notional
   - Track unrealized PnL on both legs
   - Check stop-loss conditions if enabled
   - Display funding table every `funding_table_refresh_minutes` (default: 5min)

5. **Position Closing**:
   - Close both legs concurrently at hold duration expiry
   - Use reduce-only orders to prevent accidental position flips
   - Verify closure and update stats

### State Persistence

The bot saves state to `bot_state.json` containing:
- Current state and position details (including actual vs requested notional)
- **Capital status** (populated): Available balances, max position notional, limiting exchange, last updated timestamp
- Completed cycle history (including stop-loss triggers)
- Cumulative statistics (cycle counts, errors)

**Startup Sequence**:
1. Load config — **fatal** on any error (see Configuration below)
2. Reconcile against both venues (`reconcile_positions_at_boot()`), which also checks
   the halt sentinel via `assert_not_halted()`
3. Fetch and display capital status (`update_capital_status()`)
4. Display initial funding rate table (`fetch_and_display_funding_rates()`)
5. Enter main loop

**Important**: reconciliation runs on **every** boot, not only when the state file
mentions a position — a live hedge missing from the state file is exactly the case that
must not be missed, and it is unreachable from a check gated on the state file already
knowing about it.

### Rate Limiting & Error Handling

- **Global semaphore**: `LIGHTER_API_SEMAPHORE` limits concurrent Lighter API calls to 2
- **Retry logic**: `retry_with_backoff()` handles rate limit errors (HTTP 429) with exponential backoff
- **Staggered requests**: Symbol analysis requests are delayed by `stagger_delay` (2.5s) to avoid overwhelming APIs
- **Graceful degradation**: If one symbol fails, others continue processing

## Configuration

### Environment Variables (.env)

**Aster Exchange**:
```bash
ASTER_API_USER=0x...              # Your wallet address
ASTER_API_SIGNER=0x...            # Authorized signer address
ASTER_API_PRIVATE_KEY=0x...       # Private key for signing
ASTER_APIV1_PUBLIC=...            # API public key
ASTER_APIV1_PRIVATE=...           # API private key
```

**Lighter Exchange**:
```bash
LIGHTER_BASE_URL=https://mainnet.zklighter.elliot.ai
LIGHTER_WS_URL=wss://mainnet.zklighter.elliot.ai/stream
API_KEY_PRIVATE_KEY=0x...         # Your Lighter private key
ACCOUNT_INDEX=0                   # Usually 0
API_KEY_INDEX=0                   # Usually 0
```

### Bot Configuration (config.json)

Key parameters:
- `symbols_to_monitor` - List without USDT suffix (e.g., ["BTC", "ETH"])
- `leverage` - 1-5x recommended (higher = more liquidation risk)
- `notional_per_position` - Max requested USD size per trade (auto-adjusted if insufficient capital)
- `capital_safety_margin` - Percentage of available capital to use (0.95 = 95%, keeps 5% buffer for fees/slippage)
- `hold_duration_hours` - **24h**. Break-even scales as `roundtrip_pct x (8760/hold)`,
  so 8h needs 87.6% APR to break even while 24h needs 29.2%. Longer holds are the lever
  that works; raising the threshold alone is not.
- `min_net_apr_threshold` - a *floor* applied on top of the fee-aware gate, not the gate
  itself (60.0 = 60% annualized)
- `slippage_bps_per_leg` - per-leg one-way slippage added to the venue taker fee.
  Defaults to 0.0, which reproduces the historical fee-only round trip (0.080%) and is
  almost certainly optimistic. Calibrate from realised fills.

> **Config loading is fatal on error.** An unreadable, unknown-key, or out-of-range
> config raises `ConfigError` and the bot exits(2) rather than starting on built-in
> defaults. This is not paranoia: the previous filter dropped only keys starting with
> `comment`, so the `_comment_*` keys in `config.json` reached `BotConfig(**data)`,
> raised `TypeError`, and were swallowed into an all-defaults fallback — the bot silently
> ran at 3x leverage, an 8h hold and a 5% gate while `config.json` asked for 1x, 24h and
> 60%. Keys containing "comment" are ignored; anything else unrecognised is an error, so
> a typo in a risk parameter can never silently take a default.

### Entry Gate (`funding_economics.evaluate_entry`)

Candidates are no longer filtered on `net_apr >= threshold`, where "net" only ever meant
net of the *other leg's funding* and never of trading cost. Each candidate is now
evaluated at the **actual affordable notional** (so the capital check runs before the
gate, not after) and must clear all of:

- expected net USD > 0, where expected APR applies a 30% haircut for forward-funding decay,
- `margin_ratio >= 2.0` against the break-even implied by the round trip and hold length,
- `min_net_apr_threshold` as a floor.

At the configured 24h hold and 0.080% round trip this accepts from roughly **83% gross
APR** upward — noticeably stricter than the bare 60% floor, and deliberately so. The
opportunity table prints expected APR, break-even, margin and expected net dollars per
row, plus the rejection reason, so a quiet bot explains itself.
- `max_spread_pct` - Maximum cross-exchange price difference (0.15 = 0.15%)
- `enable_stop_loss` - Enable automatic stop-loss execution (true/false)
- `funding_table_refresh_minutes` - How often to refresh opportunity table while holding (5.0 = every 5 minutes)

## Important Implementation Details

### Tick Size Handling

Both exchanges have precision requirements:
- **Lighter**: `price_tick` and `amount_tick` from market details
- **Aster**: `stepSize` from LOT_SIZE filter

The bot uses `_floor_to_tick()` to ensure sizes match on both exchanges (lines 759-766). This prevents partial hedges due to rounding differences.

### Leverage Configuration

Leverage MUST be set on both exchanges before opening positions:
- **Lighter**: Uses `lighter_set_leverage()` - applies to next order
- **Aster**: Uses `set_perp_leverage()` - can be verified immediately

The bot verifies Aster leverage after setting but cannot verify Lighter (only applies on next order).

### Order Types

**Opening**:
- Lighter: aggressive limit orders that cross the spread by `cross_ticks`, sent
  **GOOD_TILL_TIME** — *not* IOC, as this file previously claimed. That distinction is
  the whole reason Lighter is the pilot leg: a GTT remainder rests on the book and can
  fill minutes later, so it must be cancelled and verified before the hedge is committed.
- Aster: market orders

**Closing**:
- Lighter: reduce-only aggressive limit orders (also GOOD_TILL_TIME)
- Aster: reduce-only market orders

### Two-Leg Execution (`two_leg.py`)

Opening and closing both run through the shared primitive rather than
`asyncio.gather` + "assume it worked":

- **Sequential, not parallel.** `execute_two_leg` submits the pilot (Lighter), cancels
  its remainder, reads the position back, and only then sizes and submits the hedge
  (Aster) **from what actually filled**. Parallel submission maximises the window where
  both legs are live and unverified.
- **Submission never means filled.** Only a position read can promote a leg to FILLED.
- **Failure unwinds.** If the hedge does not complete, the pilot is unwound. If an
  unwind cannot complete, a `halt.json` sentinel is written.
- `HedgeVenues` in `lighter_aster_hedge.py` builds the `LegSpec`s. All venue coupling
  lives there; `two_leg.py` imports no exchange client.

### Halt Sentinel — operator procedure

`halt.json` in the working directory means the bot found real, unhedged, unmanaged
exposure and has stopped trading. It is written when:

- an unwind failed after its retries,
- a close could not flatten a leg,
- boot reconciliation found a one-legged, same-side, or multi-symbol position.

While it exists the bot **stays running and idle**, logging the reason every 5 minutes.
It deliberately does not exit: `restart: unless-stopped` would otherwise crash-loop it
and bury the message.

To clear it:

1. Read `halt.json` — it records the symbol, venue and residual quantity.
2. Check **both** venues by hand (`python check_lighter_positions.py`, and the Aster UI).
3. Flatten or hedge whatever is left, e.g. `python emergency_exit.py`.
4. Delete `halt.json`.
5. Restart the bot.

Never delete it without checking the venues — it exists precisely because the bot could
not confirm what it left behind.

### Position Verification

After opening, the bot verifies positions via:
- **Aster**: `get_perp_account_info()` → parse `positions` array
- **Lighter**: `get_lighter_open_size()` → fetch signed position size

The verification is non-blocking (continues even if verification fails).

### Funding Rate Calculations

> **Implementation note.** Interval resolution goes through
> `funding_economics.FundingIntervalResolver`, not ad-hoc code in the bot:
> `resolve_from_api_field('aster', symbol, ...)` reads `fundingIntervalHours` from
> `AsterApiManager.get_funding_info()`, falling back to `resolve_empirically` over 50
> history records (needs ≥8 samples and ≥75% agreement). **A symbol whose interval
> cannot be resolved is skipped, never defaulted.** The old code read two records, took
> the single gap between them, and fell back to `periods_per_day = 6` — which doubled
> the reported APR on every 8h symbol, i.e. most of the configured universe.

**Aster**: Funding interval varies by symbol (detected dynamically). Verified from raw
`nextFundingTime` values in this repo's own logs — **three intervals run simultaneously**:
- **1 hour (24x/day)**: e.g. ATUSDT
- **4 hours (6x/day)**: ASTERUSDT, FARTCOINUSDT, PUMPUSDT, XPLUSDT
- **8 hours (3x/day)**: BTC, ETH, BNB, DOGE, LINK, LTC, SOL, XRP
- **Detection**: prefer `fundingIntervalHours` from `/fapi/v1/fundingInfo`; fall back to
  the mode of consecutive `fundingTime` deltas. Two history records is not enough — one
  irregular gap gives a wrong answer.
- **Rate source**: Premium index endpoint (`/fapi/v1/premiumIndex`) for current/upcoming rate
```python
# Detected dynamically per symbol — never hardcode
aster_apr = aster_rate * aster_periods_per_day * 365 * 100
```

**Lighter**: Funding every **1 hour (24x/day)**, rate returned as a **decimal**.
✅ **RESOLVED EMPIRICALLY 2026-08-16** (previously two open questions, both now settled).

`/api/v1/funding-rates` is a **cross-venue** endpoint: a single response carries
binance, bybit, hyperliquid and lighter rows together. Comparing Lighter against
Hyperliquid — whose convention is established beyond doubt as hourly settlement with a
decimal rate — across **98 same-sign common symbols** gave a **median ratio of 0.9600**,
with dozens of pairs at exactly `0.000096` (Lighter) vs `0.00010` (Hyperliquid).

Matching order of magnitude across ~100 symbols is only possible if BOTH the unit and
the period agree. Hence: hourly, decimal.

This settled two contradictory claims that had been live in the code:
- `lighter_aster_hedge.py` passed `periods_per_day=3`, **understating Lighter APR by
  exactly 8x** and mis-ranking every cross-venue spread the bot evaluated.
- `examples/hedge_cli.py` (sibling EdgeX repo) asserted "rate already as percentage"
  and omitted the `*100`, **understating by 100x**. That comment was wrong.

```python
lighter_apr = lighter_rate * 24 * 365 * 100  # hourly decimal -> APR %
```

**Do not "simplify" the `*100`** — the rate is a decimal, so it belongs.

**Net APR** is the difference between receiving and paying rates. The bot chooses the direction that maximizes net APR.

### Capital Management

The bot validates capital before each position:

1. **Fetches balances**: Calls `update_capital_status()` to get available funds on both exchanges
2. **Calculates max affordable**: `min(aster_available, lighter_available) * leverage * capital_safety_margin`
3. **Adjusts position size**: If `notional_per_position` > affordable, reduces to affordable amount
4. **Logs adjustment**: Displays warning with requested vs affordable notional and limiting exchange
5. **Skips trade**: If affordable <= 0, waits 5 minutes and retries

**Capital Status Display**:
```
════════════════════════════════════════════════════════════════════════════════
CAPITAL STATUS
════════════════════════════════════════════════════════════════════════════════
  Aster:   Total: $X,XXX.XX | Available: $X,XXX.XX
  Lighter:  Total: $X,XXX.XX | Available: $X,XXX.XX
  Combined: Total: $X,XXX.XX | Available: $X,XXX.XX
  Max Position Notional: $X,XXX.XX (limited by Aster/Lighter)
  Configured Notional: $X,XXX.XX
════════════════════════════════════════════════════════════════════════════════
```

### Price Formatting

The `format_price()` helper formats prices with precision based on magnitude:
- >= $100: 2 decimals (e.g., $114,817.15)
- >= $1: 4 decimals (e.g., $1.4900)
- < $1: 6 decimals (e.g., $0.210000)

This ensures meaningful precision across all price ranges in the funding table.

## Common Development Tasks

### Adding New Symbols

1. Verify the symbol exists on both Lighter and Aster
2. Add to `symbols_to_monitor` in `config.json` (without USDT suffix)
3. Ensure minimum notional requirements are met on both exchanges

### Modifying Hold Duration

Edit `hold_duration_hours` in `config.json`. Note:
- **Payment counts depend on each symbol's actual interval** — an 8h hold collects 1 Aster
  payment on an 8h symbol, 2 on a 4h symbol, 8 on a 1h symbol. Lighter's count is unknown
  until its periodicity is resolved (see "Funding Rate Calculations").
- **Longer holds are strictly better for profitability.** Break-even APR scales as
  `roundtrip_fee_pct x (8760 / hold_hours)`. At the verified fee rates for this pair
  (Aster taker 0.040% x2 legs entry+exit, Lighter 0.000%), the round trip costs 0.080%,
  so an 8h hold needs **87.6% APR** just to break even, while a 24h hold needs **29.2%**.
- Shorter durations reduce exposure to funding rate changes but increase trading frequency
  — and each rotation pays the full round trip again.

### Adjusting Position Size

Edit `notional_per_position` in `config.json`. Consider:
- This is the **maximum requested** size - bot will auto-reduce if insufficient capital
- Minimum size requirements vary by symbol (typically $10-20 per exchange)
- Size must be >= `tick_size * 10` on both exchanges
- Bot validates available capital before each trade
- Use `capital_safety_margin` to adjust how much of available capital to use (default: 0.95 = 95%)

### Changing APR Threshold

Edit `min_net_apr_threshold` in `config.json`:
- Higher threshold = fewer trades, better opportunities
- Lower threshold = more trades, potentially marginal opportunities
- Consider funding payment frequency and fees when setting

## Testing & Debugging

### Checking Positions Without Running Bot

```bash
# Check all open positions
python check_lighter_positions.py

# Check specific symbol
python check_lighter_positions.py --symbol BTC
```

### Emergency Position Closure

```bash
# Close all delta-neutral positions with confirmation
python emergency_exit.py
```

The emergency exit script:
1. Scans both exchanges for positions
2. Matches delta-neutral pairs (opposite positions on same symbol)
3. Displays PnL for each leg
4. Waits for ENTER confirmation
5. Closes all matched positions
6. Verifies closure

### Log Levels

- **Console**: INFO level (high-level status)
- **File** (`logs/lighter_aster_hedge.log`): DEBUG level (detailed API calls, calculations)

To debug rate limit issues, check the log file for:
- `"Rate limit error detected"`
- `"retry_with_backoff:"`
- `"429"` or `"too many requests"`

### Manual State Recovery

Mostly unnecessary now — **venue truth outranks the state file**. On every boot
`reconcile_positions_at_boot` queries account-level position listings from both venues
and reconciles against them, so the state file is no longer the thing that decides what
is open. It contributes metadata (`opened_at`, `capital_at_open`) and nothing else.

What the reconciler does with what it finds:

| Found on venues | Action |
|---|---|
| Nothing | clears any stale `current_position`, starts fresh |
| Hedged, matches state | resumes HOLDING, corrects `size_base` to venue truth |
| Hedged, absent from state | **adopts** it (hold window restarts now) rather than opening a second position beside it |
| One-legged | **halts** — that is live directional exposure the bot did not intend |
| Same side on both venues | **halts** — doubled exposure, not a hedge |
| More than one hedged symbol | **halts** — this bot manages one position at a time |
| A venue listing fails | retries 5x, then halts — a failed read is *not* "flat" |

So: **do not delete `bot_state.json` to fix a stuck position.** It no longer helps (the
bot rediscovers positions from the venues) and it destroys the `capital_at_open`
baseline that realised-PnL measurement depends on. If something is genuinely wrong, read
`halt.json`, check both venues, and use `python emergency_exit.py`.

## Exchange Connector Notes

The exchange connectors (`lighter_client.py` and `aster_api_manager.py`) are copied from external projects and should NOT be modified in this repository. Changes should be made in the source projects.

**Dependencies**:
- `aster_api_manager.py` requires `utils.py` and `strategy_logic.py` (included)
- Both connectors have their own rate limiting and retry logic
- WebSocket connections are used for Lighter balance checks only

## Docker Deployment

The bot includes Docker support for production deployment:

**Key Docker Features**:
- Python 3.11 slim base image
- Non-root user for security
- Volume mounts for:
  - `bot_state.json` - persistent state
  - `logs/` - log files
  - `config.json` - read-only configuration
- Environment loaded from `.env` file
- Graceful shutdown (30s grace period)
- Health check based on state file freshness
- Resource limits (512MB RAM, 1 CPU)

**Health Check Logic**:
```python
# Checks if bot_state.json was updated in last 10 minutes
os.path.exists('bot_state.json') and
time.time() - os.path.getmtime('bot_state.json') < 600
```

## Safety Considerations

**NEVER commit**:
- `.env` file (contains private keys)
- `bot_state.json` (may contain sensitive position data)
- Any files with API keys or private keys

**Position safety**:
- Delta-neutral ≠ risk-free (funding can flip, spreads can widen)
- High leverage increases liquidation risk
- Partial fills break delta-neutral hedge
- Exchange downtime can prevent position closure

**Code safety**:
- Always test with small positions first
- Verify both exchanges support the symbols
- Check minimum size requirements before adding symbols
- Monitor positions actively, especially initially
