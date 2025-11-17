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

**Exchange Connectors** (DO NOT MODIFY):
- `lighter_client.py` - Lighter exchange API wrapper
- `aster_api_manager.py` - Aster exchange API wrapper
- `utils.py` - Helper utilities for Aster connector
- `strategy_logic.py` - Required stub for Aster connector

**Utility Scripts**:
- `check_lighter_positions.py` - Standalone position checker
- `emergency_exit.py` - Emergency position closer with PnL display

### Data Flow

1. **Funding Rate Analysis**:
   - **Caching**: Check 5-minute cache first to reduce API calls (`get_cached_funding()`)
   - Fetch from Aster: `aster.get_premium_index()` → Current/upcoming rate (forward-looking, not historical)
   - Detect funding interval: Analyze `get_funding_rate_history()` timestamps (4h or 8h)
   - Fetch from Lighter: `lighter_client.get_lighter_funding_rate()` → Current/upcoming rate (forward-looking)
   - **Lighter funding**: Effective 8-hour intervals (3x/day) despite hourly sampling in calculation
   - Calculate annualized APR: `rate * periods_per_day * 365 * 100` (periods_per_day=3 for Lighter, detected dynamically for Aster)
   - **Cache results**: Store rates for 5 minutes (`set_cached_funding()`) to prevent redundant API calls
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
1. Load state and recover position if exists (`verify_and_recover_position()`)
2. Fetch and display capital status (`update_capital_status()`)
3. Display initial funding rate table (`fetch_and_display_funding_rates()`)
4. Enter main loop

**Important**: The bot can recover from crashes by loading this state file. If a position exists in state, the bot calls `verify_and_recover_position()` on startup to validate the position still exists on both exchanges.

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
- `hold_duration_hours` - 8h recommended (1 full Lighter funding cycle)
- `min_net_apr_threshold` - Minimum APR to open (5.0 = 5% annualized)
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
- Lighter: Aggressive limit orders (IOC) that cross the spread by `cross_ticks`
- Aster: Market orders

**Closing**:
- Lighter: Reduce-only aggressive limit orders
- Aster: Reduce-only market orders

### Position Verification

After opening, the bot verifies positions via:
- **Aster**: `get_perp_account_info()` → parse `positions` array
- **Lighter**: `get_lighter_open_size()` → fetch signed position size

The verification is non-blocking (continues even if verification fails).

### Funding Rate Calculations

**Aster**: Funding interval varies by symbol (detected dynamically)
- **Most symbols**: Every 8 hours (3x/day)
- **Some symbols** (e.g., XPL, ASTER): Every 4 hours (6x/day)
- **Detection**: Bot fetches 2 history records and calculates time difference
- **Rate source**: Premium index endpoint (`/fapi/v1/premiumIndex`) for current/upcoming rate (forward-looking)
- **Rate format**: Forward-looking (represents upcoming funding, not historical)
```python
# Detected dynamically per symbol
aster_apr = aster_rate * aster_periods_per_day * 365 * 100
```

**Lighter**: Funding effectively every 8 hours (3x/day)
- **Rate source**: `/api/v1/funding-rates` endpoint (forward-looking)
- **Calculation methodology**: Lighter samples premiums every hour and divides by 8, but the effective funding interval is 8 hours
- **Rate format**: Forward-looking (represents upcoming funding, not historical)
- **API returns**: 8-hour equivalent rate (comparable to other 8-hour exchanges like Binance)
```python
lighter_apr = lighter_rate * 3 * 365 * 100  # 3 periods per day (8-hour intervals)
```

**Important**: Lighter's documentation mentions "funding payments occur at each hour mark" which refers to their *sampling methodology*, not the actual payment frequency. The rate returned by their API is an 8-hour equivalent rate (divided by 8), making it directly comparable to other exchanges with 8-hour funding intervals.

**Net APR** is the difference between receiving and paying rates. The bot chooses the direction that maximizes net APR.

### Funding Rate Caching

The bot implements a 5-minute cache for funding rates to:
- Reduce API calls and avoid rate limiting
- Improve performance during multi-symbol analysis
- Minimize network latency

Cache implementation:
- **TTL**: 300 seconds (5 minutes)
- **Key**: `(symbol, quote, exchange)` tuple
- **Storage**: In-memory dictionary with timestamps
- **Auto-expiry**: Expired entries removed on next access
- **Functions**: `get_cached_funding()`, `set_cached_funding()`, `clear_funding_cache()`

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
- 8 hours = 1 Lighter funding payment + 2 Aster payments
- 4 hours = 0 Lighter payments + 1 Aster payment
- Shorter durations reduce exposure to funding rate changes but increase trading frequency

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

If the bot's state becomes corrupted:

1. Backup `bot_state.json`
2. Check actual positions: `python check_lighter_positions.py`
3. If positions exist but bot doesn't know about them:
   - Option A: Close manually via exchange UIs
   - Option B: Delete `bot_state.json` and let bot start fresh (WARNING: bot won't track these positions)
4. If bot shows position but none exist:
   - Delete `bot_state.json` to reset

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
