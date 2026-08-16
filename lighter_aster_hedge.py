#!/usr/bin/env python3
"""
lighter_aster_hedge.py
----------------------
Automated cross-exchange delta-neutral position rotation bot between Lighter and Aster perpetual markets.

This bot continuously:
1. Analyzes funding rates across multiple symbols on both Lighter and Aster
2. Opens the best delta-neutral position (long on one exchange, short on the other)
3. Holds for 8 hours collecting funding
4. Closes the position
5. Waits briefly and repeats

Features:
- Persistent state across restarts
- Automatic recovery from crashes
- Comprehensive PnL tracking (trading, funding, fees)
- Health monitoring during hold period
- Graceful shutdown handling

Usage:
    python lighter_aster_hedge.py
    python lighter_aster_hedge.py --state-file custom_state.json --config config.json
"""

import asyncio
import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict, fields

from dotenv import load_dotenv
import lighter

import lighter_client
from aster_api_manager import AsterApiManager
from two_leg import (
    HALT_FILENAME,
    BootReconcileError,
    HaltedError,
    LegSpec,
    assert_not_halted,
    boot_reconcile,
    execute_two_leg,
    unwind_leg,
    write_halt,
)
from funding_economics import (
    FundingInterval,
    FundingIntervalResolver,
    IntervalResolutionError,
    TradeCostModel,
    VenueCosts,
    VERIFIED_TAKER_BPS,
    annualize,
    break_even_apr_pct,
    evaluate_entry,
)

# ANSI color codes for console output
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    GRAY = '\033[90m'

class BalanceFetchError(Exception):
    """Raised when balance retrieval fails."""
    pass


class RateLimitError(Exception):
    """Raised when API rate limit is hit."""
    pass


# ==================== Rate Limit Handling ====================

def is_rate_limit_error(exc: Exception) -> bool:
    """Check if an exception is a rate limit error (HTTP 429)."""
    error_str = str(exc).lower()
    logger.debug(f"Checking if error is rate limit: {error_str[:200]}")
    is_rate_limit = (
        "429" in error_str or
        "too many requests" in error_str or
        "23000" in error_str or
        "rate limit" in error_str or
        "ratelimit" in error_str
    )
    if is_rate_limit:
        logger.warning(f"Rate limit error detected: {error_str[:300]}")
    return is_rate_limit


async def retry_with_backoff(
    func,
    max_retries: int = 3,
    initial_delay: float = 1.0,
    backoff_factor: float = 2.0,
    max_delay: float = 30.0,
    jitter: bool = True
):
    """
    Retry an async function with exponential backoff on rate limit errors.
    """
    import random

    func_name = getattr(func, '__name__', 'unknown')
    logger.debug(f"retry_with_backoff: Starting for {func_name}, max_retries={max_retries}")

    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            logger.debug(f"retry_with_backoff: Attempt {attempt + 1}/{max_retries + 1} for {func_name}")
            result = await func()
            logger.debug(f"retry_with_backoff: Success on attempt {attempt + 1} for {func_name}")
            return result
        except Exception as exc:
            last_exception = exc
            logger.debug(f"retry_with_backoff: Exception on attempt {attempt + 1} for {func_name}: {type(exc).__name__}")

            if not is_rate_limit_error(exc):
                logger.debug(f"retry_with_backoff: Not a rate limit error, re-raising for {func_name}")
                raise

            if attempt >= max_retries:
                logger.error(f"Rate limit retry exhausted after {max_retries} attempts for {func_name}")
                raise RateLimitError(f"Rate limit exceeded after {max_retries} retries: {exc}") from exc

            delay = min(initial_delay * (backoff_factor ** attempt), max_delay)

            if jitter:
                jitter_range = delay * 0.25
                delay = delay + random.uniform(-jitter_range, jitter_range)

            logger.warning(
                f"Rate limit hit (attempt {attempt + 1}/{max_retries}) for {func_name}, "
                f"retrying in {delay:.1f}s... Error: {str(exc)[:100]}"
            )
            logger.debug(f"retry_with_backoff: Sleeping {delay:.1f}s before retry for {func_name}")

            await asyncio.sleep(delay)

    if last_exception:
        logger.error(f"retry_with_backoff: All retries exhausted for {func_name}, raising last exception")
        raise last_exception


# ==================== Global Rate Limiting ====================

# Global semaphore to limit concurrent Lighter API calls
LIGHTER_API_SEMAPHORE = asyncio.Semaphore(2)  # Max 2 concurrent Lighter API calls

# Shared per-symbol funding-interval cache. Successes are cached with a TTL; failures
# are NEVER cached, so one transient API error cannot pin a symbol at a wrong interval
# for the life of the process.
FUNDING_RESOLVER = FundingIntervalResolver()

# ==================== Logging Setup ====================

os.makedirs('logs', exist_ok=True)

# File handler - DEBUG level
# APPEND + rotate, never mode='w'. This bot can exit with a position still open and be
# restarted by `restart: unless-stopped`; truncating on start destroyed exactly the log
# that explained the failure. Rotation keeps growth bounded (10MB x 3 = 30MB max).
from logging.handlers import RotatingFileHandler

file_handler = RotatingFileHandler(
    'logs/lighter_aster_hedge.log', mode='a', maxBytes=10 * 1024 * 1024, backupCount=3
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))

# Console handler - INFO level
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

# Root logger
logging.basicConfig(level=logging.DEBUG, handlers=[file_handler, console_handler], force=True)
logger = logging.getLogger(__name__)

# Silence noisy third-party loggers
logging.getLogger('websockets').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('lighter').setLevel(logging.WARNING)
logging.getLogger('lighter_client').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)

# ==================== Constants & Environment ====================

DEFAULT_SYMBOLS: List[str] = [
    "BTC",
    "ETH",
    "SOL",
    "BNB",
    "ASTER",
    "DOGE",
    "XRP",
    "LINK",
    "LTC",
]


def load_env() -> dict:
    """Load required environment variables for both exchanges."""
    load_dotenv()
    env: Dict[str, object] = {}

    # Aster credentials
    env["ASTER_API_USER"] = os.getenv("ASTER_API_USER")
    env["ASTER_API_SIGNER"] = os.getenv("ASTER_API_SIGNER")
    env["ASTER_API_PRIVATE_KEY"] = os.getenv("ASTER_API_PRIVATE_KEY")
    env["ASTER_APIV1_PUBLIC"] = os.getenv("ASTER_APIV1_PUBLIC")
    env["ASTER_APIV1_PRIVATE"] = os.getenv("ASTER_APIV1_PRIVATE")

    # Lighter credentials
    env["LIGHTER_BASE_URL"] = os.getenv("LIGHTER_BASE_URL", os.getenv("BASE_URL", "https://mainnet.zklighter.elliot.ai"))
    env["LIGHTER_WS_URL"] = os.getenv("LIGHTER_WS_URL", os.getenv("WEBSOCKET_URL", "wss://mainnet.zklighter.elliot.ai/stream"))
    env["API_KEY_PRIVATE_KEY"] = os.getenv("API_KEY_PRIVATE_KEY") or os.getenv("LIGHTER_PRIVATE_KEY")
    env["ACCOUNT_INDEX"] = int(os.getenv("ACCOUNT_INDEX", os.getenv("LIGHTER_ACCOUNT_INDEX", "0")))
    env["API_KEY_INDEX"] = int(os.getenv("API_KEY_INDEX", os.getenv("LIGHTER_API_KEY_INDEX", "0")))
    env["MARGIN_MODE"] = "cross"

    missing = [key for key in ("ASTER_API_USER", "ASTER_API_SIGNER", "ASTER_API_PRIVATE_KEY", "ASTER_APIV1_PUBLIC", "ASTER_APIV1_PRIVATE", "API_KEY_PRIVATE_KEY") if not env.get(key)]
    if missing:
        logger.warning("Missing env vars: %s. Trading may fail.", missing)

    return env

# ==================== State Management ====================

class ConfigError(RuntimeError):
    """Raised when the configuration cannot be trusted.

    Deliberately fatal. The previous behaviour was to log the error and return
    `BotConfig(symbols_to_monitor=DEFAULT_SYMBOLS)`, which silently substituted the
    hardcoded defaults for every tuned parameter. That is not a degraded mode, it is
    a different strategy: config.json asks for 1x leverage, a 24h hold and a 60% APR
    gate; the defaults are 3x, 8h and 5%. The 5% gate sits far below the 87.6%
    break-even of an 8h hold, so the fallback traded at a structural loss while
    reporting that it had loaded fine.

    A trading bot that cannot read its own risk parameters must not trade.
    """


class BotState:
    """State machine for the rotation bot."""
    IDLE = "IDLE"
    ANALYZING = "ANALYZING"
    OPENING = "OPENING"
    HOLDING = "HOLDING"
    CLOSING = "CLOSING"
    WAITING = "WAITING"
    ERROR = "ERROR"
    SHUTDOWN = "SHUTDOWN"


@dataclass
class BotConfig:
    """Bot configuration parameters."""
    symbols_to_monitor: List[str]
    quote: str = "USDT"
    leverage: int = 3
    notional_per_position: float = 100.0
    hold_duration_hours: float = 8.0
    wait_between_cycles_minutes: float = 5.0
    check_interval_seconds: int = 60
    min_net_apr_threshold: float = 5.0
    max_spread_pct: float = 0.15
    enable_stop_loss: bool = True
    funding_table_refresh_minutes: float = 5.0
    capital_safety_margin: float = 0.95
    # Per-leg, one-way slippage charged on top of the venue taker fee, in basis
    # points. 0.0 reproduces the fee-only round trip this bot has always assumed
    # (Aster taker 4bps x 2 legs = 0.080%). It is almost certainly optimistic: the
    # bot crosses the spread on both venues, twice per cycle. Calibrate it from
    # realised fills - the difference between avg_mid at open and the actual entry
    # price - rather than leaving it at zero.
    slippage_bps_per_leg: float = 0.0

    @classmethod
    def _clean(cls, data: dict, source: str) -> Dict[str, object]:
        """Drop documentation keys and reject anything that is not a real field.

        Whitelisting against the dataclass field set is the point. The old filter was
        `not k.startswith('comment')`, which let `_comment_hold_duration_hours` through
        into `BotConfig(**data)` -> TypeError -> silent all-defaults fallback. Matching
        on the field set means no comment convention can reach the constructor, and a
        misspelled key is an error rather than a silent default on a risk parameter.
        """
        known = {f.name for f in fields(cls)}
        cleaned: Dict[str, object] = {}
        unknown: List[str] = []

        for key, value in data.items():
            if "comment" in key.lower():
                continue
            if key in known:
                cleaned[key] = value
            else:
                unknown.append(key)

        if unknown:
            raise ConfigError(
                f"{source}: unknown configuration key(s) {sorted(unknown)}. "
                f"Valid keys are {sorted(known)}. A misspelled key would otherwise "
                f"fall back to a default silently, so this refuses to guess."
            )
        if not cleaned.get("symbols_to_monitor"):
            raise ConfigError(f"{source}: 'symbols_to_monitor' is required and must be non-empty")

        defaults_by_name = {f.name: f.default for f in fields(cls)}
        for name in sorted(known - set(cleaned)):
            logger.info("%s: %s not set, using default %r", source, name,
                        defaults_by_name.get(name))
        return cleaned

    def validate(self, source: str = "config") -> None:
        """Reject values that are out of range before any of them reach an order.

        Cheap, and it catches the kind of edit that produces a nonsense trade rather
        than a crash: `hold_duration_hours: 0` divides by zero inside
        `funding_economics.break_even_apr_pct`, and a negative safety margin would
        invert the affordable-notional calculation.
        """
        problems: List[str] = []

        if not self.symbols_to_monitor:
            problems.append("symbols_to_monitor is empty")
        if not isinstance(self.leverage, int) or self.leverage < 1:
            problems.append(f"leverage must be an integer >= 1 (got {self.leverage!r})")
        if self.notional_per_position <= 0:
            problems.append(f"notional_per_position must be > 0 (got {self.notional_per_position!r})")
        if self.hold_duration_hours <= 0:
            problems.append(f"hold_duration_hours must be > 0 (got {self.hold_duration_hours!r})")
        if self.wait_between_cycles_minutes < 0:
            problems.append(f"wait_between_cycles_minutes must be >= 0 (got {self.wait_between_cycles_minutes!r})")
        if self.check_interval_seconds <= 0:
            problems.append(f"check_interval_seconds must be > 0 (got {self.check_interval_seconds!r})")
        if self.min_net_apr_threshold < 0:
            problems.append(f"min_net_apr_threshold must be >= 0 (got {self.min_net_apr_threshold!r})")
        if self.max_spread_pct <= 0:
            problems.append(f"max_spread_pct must be > 0 (got {self.max_spread_pct!r})")
        if self.funding_table_refresh_minutes <= 0:
            problems.append(f"funding_table_refresh_minutes must be > 0 (got {self.funding_table_refresh_minutes!r})")
        if not 0 < self.capital_safety_margin <= 1:
            problems.append(f"capital_safety_margin must be in (0, 1] (got {self.capital_safety_margin!r})")

        if problems:
            raise ConfigError(f"{source}: invalid configuration - " + "; ".join(problems))

    @classmethod
    def load_from_file(cls, config_file: str) -> 'BotConfig':
        """Load configuration from JSON file.

        Raises ConfigError on anything missing, unreadable, unknown or out of range.
        See ConfigError for why this is fatal rather than quietly defaulted.
        """
        try:
            with open(config_file, 'r') as f:
                data = json.load(f)
        except FileNotFoundError as exc:
            raise ConfigError(
                f"Config file {config_file} not found. Refusing to start on built-in "
                f"defaults (3x leverage, 8h hold, 5% APR gate) - they are not the "
                f"configured strategy."
            ) from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfigError(f"Config file {config_file} could not be read: {exc}") from exc

        if not isinstance(data, dict):
            raise ConfigError(f"{config_file}: top level must be a JSON object")

        config = cls(**cls._clean(data, config_file))
        config.validate(config_file)
        return config

    def reload(self, config_file: str) -> bool:
        """Reload configuration in place. Returns True if the file was applied.

        Non-fatal by design, unlike the initial load: the bot may be holding a live
        position, and refusing to continue because someone saved a half-edited config
        is worse than running on the last known-good values.

        The update is all-or-nothing. The candidate is fully built and validated
        before a single attribute is touched, so a bad edit can never leave the bot
        running on a half-applied mixture of old and new parameters.
        """
        try:
            with open(config_file, 'r') as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ConfigError(f"{config_file}: top level must be a JSON object")
            candidate = BotConfig(**self._clean(data, config_file))
            candidate.validate(config_file)
        except (OSError, json.JSONDecodeError, ConfigError, TypeError, ValueError) as exc:
            logger.error(
                "Config reload failed (%s). Continuing on the previously loaded "
                "values, which are unchanged.", exc
            )
            return False

        changed_params = []
        for field_def in fields(self):
            old_value = getattr(self, field_def.name)
            new_value = getattr(candidate, field_def.name)
            if old_value != new_value:
                setattr(self, field_def.name, new_value)
                changed_params.append(f"{field_def.name}: {old_value} → {new_value}")

        if changed_params:
            logger.info("Configuration reloaded with changes:")
            for change in changed_params:
                logger.info(f"  - {change}")
        else:
            logger.debug("Configuration reloaded (no changes detected)")

        return True


# ==================== Helper Functions ====================

def _round_to_tick(value: float, tick: float) -> float:
    """Round value to nearest tick."""
    if not tick or tick <= 0:
        return value
    d_value = Decimal(str(value))
    d_tick = Decimal(str(tick))
    return float((d_value / d_tick).quantize(Decimal('1'), rounding=ROUND_HALF_UP) * d_tick)


def _ceil_to_tick(value: float, tick: float) -> float:
    """Round value up to nearest tick."""
    if not tick or tick <= 0:
        return value
    d_value = Decimal(str(value))
    d_tick = Decimal(str(tick))
    return float((d_value / d_tick).quantize(Decimal('1'), rounding=ROUND_UP) * d_tick)


def _floor_to_tick(value: float, tick: float) -> float:
    """Round value down to nearest tick."""
    if not tick or tick <= 0:
        return value
    d_value = Decimal(str(value))
    d_tick = Decimal(str(tick))
    return float((d_value / d_tick).quantize(Decimal('1'), rounding=ROUND_DOWN) * d_tick)


def compute_base_size_from_quote(avg_mid: float, size_quote: float) -> float:
    """Convert quote notional into base size using the average mid price."""
    if avg_mid <= 0:
        raise ValueError("Invalid mid price to compute base size.")
    return size_quote / avg_mid


def get_avg_mid(
    lighter_bid: Optional[float],
    lighter_ask: Optional[float],
    aster_bid: Optional[float],
    aster_ask: Optional[float],
) -> float:
    """Average mid price between both exchanges, falling back gracefully."""
    mids: List[float] = []
    if lighter_bid and lighter_ask:
        mids.append((lighter_bid + lighter_ask) / 2.0)
    if aster_bid and aster_ask:
        mids.append((aster_bid + aster_ask) / 2.0)

    if mids:
        return sum(mids) / len(mids)

    if lighter_bid and lighter_ask:
        return (lighter_bid + lighter_ask) / 2.0
    if aster_bid and aster_ask:
        return (aster_bid + aster_ask) / 2.0
    if lighter_bid and aster_ask:
        return (lighter_bid + aster_ask) / 2.0
    if aster_bid and lighter_ask:
        return (aster_bid + lighter_ask) / 2.0

    raise RuntimeError("No usable prices from either venue.")


# NOTE on auditing the Lighter interval: funding_economics.audit_constant_interval()
# exists to re-check a hardcoded cadence against observed settlement timestamps, but the
# installed Lighter SDK exposes only FundingApi.funding_rates() - current rates, with no
# history endpoint - so there are no timestamps to audit against from here. The 1h
# constant rests on the cross-venue comparison documented in
# funding_economics.CONSTANT_INTERVALS. If Lighter ever ships a funding-history
# endpoint, wire audit_constant_interval() into startup.
#
# `_calculate_apr` was removed: annualisation now goes through
# funding_economics.annualize(rate, FundingInterval), so a period count can no longer be
# passed as a bare literal at the call site.


def utc_now() -> datetime:
    """Return a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    """Return an ISO 8601 timestamp suffixed with Z for UTC."""
    return utc_now().isoformat().replace("+00:00", "Z")


def to_iso_z(dt_obj: datetime) -> str:
    """Convert datetime to ISO string with Z suffix, adding UTC if naive."""
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=timezone.utc)
    return dt_obj.isoformat().replace("+00:00", "Z")


def from_iso_z(iso_string: str) -> datetime:
    """Parse ISO timestamp with Z or +00:00 suffix."""
    cleaned = iso_string.rstrip('Z')
    if cleaned.count('+00:00') > 1:
        parts = cleaned.split('+00:00')
        cleaned = parts[0] + '+00:00'
    elif not ('+' in cleaned or '-' in cleaned[-6:]):
        cleaned = cleaned + '+00:00'

    return datetime.fromisoformat(cleaned)


def calculate_stop_loss_percentage(leverage: float) -> float:
    """
    Calculate stop-loss percentage based on leverage.

    For cross margin:
    - Liquidation occurs at approximately (100 / leverage)%
    - Stop-loss is set at 75% of liquidation threshold for safety

    Example: 3x leverage → ~33% liquidation, ~25% stop-loss

    Args:
        leverage: Leverage multiplier (e.g., 3 for 3x)

    Returns:
        Stop-loss percentage (e.g., 25.0 for 25%)
    """
    if leverage <= 0:
        return 0.0

    liquidation_threshold = 100.0 / leverage
    stop_loss_pct = liquidation_threshold * 0.75  # 75% of liquidation threshold
    return stop_loss_pct


async def get_position_pnls(
    env: dict,
    aster: AsterApiManager,
    symbol: str
) -> Tuple[Optional[float], Optional[float]]:
    """
    Fetch unrealized PnL from both exchanges for a given symbol.

    Args:
        env: Environment variables
        aster: Aster API manager instance
        symbol: Trading symbol (e.g., "BTCUSDT")

    Returns:
        Tuple of (aster_pnl, lighter_pnl)
        Either value may be None if fetching fails
    """
    aster_pnl: Optional[float] = None
    lighter_pnl: Optional[float] = None

    # Fetch Aster PnL
    try:
        aster_account = await aster.get_perp_account_info()
        aster_positions = aster_account.get('positions', [])
        for pos in aster_positions:
            if pos.get('symbol') == symbol:
                aster_pnl = float(pos.get('unrealizedProfit', 0))
                break
    except Exception as e:
        # WARNING, not DEBUG. A None here disables the stop-loss for this leg (the
        # monitor only evaluates it when BOTH PnLs are known), so this line is the
        # only signal that risk management just stopped running. There is no
        # alerting layer in this deployment - the log is all there is.
        logger.warning(f"STOP-LOSS DEGRADED: could not fetch Aster PnL for {symbol}: {e}")

    # Fetch Lighter PnL
    api_client = None
    try:
        symbol_clean = symbol.replace("USDT", "")
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
        order_api = lighter.OrderApi(api_client)
        account_api = lighter.AccountApi(api_client)

        # Get market ID
        l_market_id, _, _ = await lighter_client.get_lighter_market_details(order_api, symbol_clean)

        # Get position details with PnL
        details = await lighter_client.get_lighter_position_details(
            account_api,
            env["ACCOUNT_INDEX"],
            l_market_id
        )

        if details:
            lighter_pnl = details.get('unrealized_pnl', 0.0)
    except Exception as e:
        # See the Aster branch above: None disables the stop-loss for this cycle.
        logger.warning(f"STOP-LOSS DEGRADED: could not fetch Lighter PnL for {symbol}: {e}")
    finally:
        if api_client:
            try:
                await api_client.close()
            except Exception:
                pass

    return aster_pnl, lighter_pnl


async def configure_leverage(
    leverage: float,
    env: dict,
    aster: AsterApiManager,
    lighter_signer: lighter.SignerClient,
    aster_symbol: str,
    lighter_market_id: int,
    verify: bool = True,
) -> Tuple[bool, bool]:
    """
    Configure leverage on both exchanges. Returns tuple of booleans
    indicating success for (Aster, Lighter).
    """
    aster_success = False
    lighter_success = False

    logger.info("Setting leverage to %sx on both exchanges...", leverage)
    print(f"\n{Colors.CYAN}Setting leverage to {leverage}x on both exchanges...{Colors.RESET}")

    try:
        await lighter_client.lighter_set_leverage(
            lighter_signer,
            lighter_market_id,
            int(leverage),
            env.get("MARGIN_MODE", "cross"),
        )
        lighter_success = True
        print(f"  {Colors.GREEN}✓ Lighter: Set to {leverage}x ({env.get('MARGIN_MODE', 'cross')} margin){Colors.RESET}")
    except Exception as exc:
        print(f"  {Colors.RED}✗ Lighter: Failed to set leverage - {exc}{Colors.RESET}")
        logger.error("Lighter leverage set failed: %s", exc)

    try:
        await aster.set_perp_leverage(aster_symbol, int(leverage))
        aster_success = True
        print(f"  {Colors.GREEN}✓ Aster: Set to {leverage}x{Colors.RESET}")
    except Exception as exc:
        print(f"  {Colors.RED}✗ Aster: Failed to set leverage - {exc}{Colors.RESET}")
        logger.error("Aster leverage set failed: %s", exc)

    if verify and aster_success:
        try:
            current_leverage = await aster.get_perp_leverage(aster_symbol)
            if current_leverage:
                if abs(current_leverage - leverage) < 0.1:
                    print(f"  {Colors.GREEN}✓ Aster: Verified at {current_leverage}x{Colors.RESET}")
                else:
                    print(f"  {Colors.YELLOW}⚠ Aster: Set to {leverage}x but reads as {current_leverage}x{Colors.RESET}")
            else:
                print(f"  {Colors.YELLOW}⚠ Aster: Could not verify leverage{Colors.RESET}")
        except Exception as exc:
            logger.debug("Could not verify Aster leverage: %s", exc)

    if lighter_success and verify:
        print(f"  {Colors.BLUE}ℹ Lighter: Verification not available (applies on next order){Colors.RESET}")

    if not (aster_success and lighter_success):
        print(f"\n{Colors.RED}{Colors.BOLD}⚠️  WARNING: Leverage setting failed on one or more exchanges!{Colors.RESET}")
        print(f"  {Colors.YELLOW}This may result in unexpected margin usage.{Colors.RESET}")
        return aster_success, lighter_success

    print(f"{Colors.GREEN}✓ Leverage configured on both exchanges{Colors.RESET}\n")
    return aster_success, lighter_success


async def fetch_symbol_spread(symbol: str, env: dict, aster: AsterApiManager) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Fetch mid prices from both exchanges and calculate cross-exchange spread percentage.

    Returns:
        Tuple of (spread_pct, aster_mid, lighter_mid)
        - spread_pct: Spread percentage (e.g., 0.15 for 0.15%), or None if unavailable
        - aster_mid: Aster mid price, or None if unavailable
        - lighter_mid: Lighter mid price, or None if unavailable
    """
    logger.debug(f"fetch_symbol_spread: Starting for {symbol}")

    async def fetch_aster_mid() -> Optional[float]:
        logger.debug(f"fetch_aster_mid: Starting for {symbol}")
        try:
            ticker = await aster.get_perp_book_ticker(symbol)
            bid = float(ticker.get('bidPrice', 0))
            ask = float(ticker.get('askPrice', 0))

            if bid and ask:
                mid = (bid + ask) / 2
                logger.debug(f"fetch_aster_mid: Got mid price {mid} for {symbol}")
                return mid
            elif bid or ask:
                fallback = bid if bid else ask
                logger.debug(f"fetch_aster_mid: Using fallback price {fallback} for {symbol}")
                return fallback
            logger.warning(f"fetch_aster_mid: No prices available for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error fetching Aster mid price for {symbol}: {e}", exc_info=True)
            return None

    async def fetch_lighter_mid() -> Optional[float]:
        logger.debug(f"fetch_lighter_mid: Starting for {symbol}")
        api_client = None
        try:
            logger.debug(f"fetch_lighter_mid: Waiting for semaphore for {symbol}")
            async with LIGHTER_API_SEMAPHORE:
                logger.debug(f"fetch_lighter_mid: Semaphore acquired for {symbol}")
                api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
                order_api = lighter.OrderApi(api_client)
                symbol_clean = symbol.replace("USDT", "")

                logger.debug(f"fetch_lighter_mid: Getting market details for {symbol_clean}")
                market_id, _, _ = await lighter_client.get_lighter_market_details(order_api, symbol_clean)
                logger.debug(f"fetch_lighter_mid: Market ID for {symbol_clean}: {market_id}")

                logger.debug(f"fetch_lighter_mid: Getting best bid/ask for {symbol_clean}")
                best_bid, best_ask = await lighter_client.get_lighter_best_bid_ask(order_api, symbol_clean, market_id, timeout=10.0)
                logger.debug(f"fetch_lighter_mid: Bid={best_bid}, Ask={best_ask} for {symbol_clean}")

                if best_bid and best_ask:
                    mid = (best_bid + best_ask) / 2
                    logger.debug(f"fetch_lighter_mid: Mid price={mid} for {symbol_clean}")
                    return mid
                elif best_bid or best_ask:
                    fallback = best_bid if best_bid else best_ask
                    logger.debug(f"fetch_lighter_mid: Using fallback price={fallback} for {symbol_clean}")
                    return fallback
                logger.warning(f"fetch_lighter_mid: No prices available for {symbol_clean}")
                return None
        except Exception as e:
            logger.error(f"Error fetching Lighter mid price for {symbol}: {e}", exc_info=True)
            return None
        finally:
            if api_client:
                try:
                    await api_client.close()
                    logger.debug(f"fetch_lighter_mid: API client closed for {symbol}")
                except Exception as close_err:
                    logger.debug(f"fetch_lighter_mid: Error closing API client for {symbol}: {close_err}")

    logger.debug(f"fetch_symbol_spread: Gathering mid prices from both exchanges for {symbol}")
    aster_mid, lighter_mid = await asyncio.gather(fetch_aster_mid(), fetch_lighter_mid())
    logger.debug(f"fetch_symbol_spread: Received aster_mid={aster_mid}, lighter_mid={lighter_mid} for {symbol}")

    if aster_mid is None or lighter_mid is None:
        logger.warning(f"fetch_symbol_spread: Missing mid price for {symbol} (aster={aster_mid}, lighter={lighter_mid})")
        return None, aster_mid, lighter_mid

    # Calculate cross-exchange spread percentage
    price_diff = abs(aster_mid - lighter_mid)
    avg_mid = (aster_mid + lighter_mid) / 2
    spread_pct = (price_diff / avg_mid) * 100

    logger.debug(f"fetch_symbol_spread: Calculated spread for {symbol}: {spread_pct:.4f}% (aster={aster_mid}, lighter={lighter_mid})")
    return spread_pct, aster_mid, lighter_mid


async def fetch_symbol_funding(symbol: str, env: dict, aster: AsterApiManager, check_volume: bool = False, max_spread_pct: float = 0.15) -> dict:
    """
    Fetch funding rates for a single symbol across both venues.

    Args:
        symbol: Trading symbol (e.g., "BTCUSDT")
        env: Environment variables
        aster: Aster API manager instance
        check_volume: Whether to check volume threshold (not implemented for now)
        max_spread_pct: Maximum cross-exchange spread percentage (default: 0.15%)
    """
    logger.info("Checking funding for %s...", symbol)
    logger.debug(f"fetch_symbol_funding: Starting for {symbol}, max_spread_pct={max_spread_pct}")

    aster_rate_decimal: Optional[float] = None
    lighter_rate_decimal: Optional[float] = None
    aster_apr: Optional[float] = None
    lighter_apr: Optional[float] = None

    async def _aster_interval_hours(sym: str) -> Optional[float]:
        """Authoritative source: the venue publishes its own cadence."""
        for row in await aster.get_funding_info():
            if row.get('symbol') == sym:
                value = row.get('fundingIntervalHours')
                return float(value) if value else None
        return None

    async def _aster_funding_times(sym: str) -> List[int]:
        history = await aster.get_funding_rate_history(sym, limit=50)
        return [int(h['fundingTime']) for h in (history or []) if h.get('fundingTime')]

    async def fetch_aster_rate() -> Optional[Tuple[float, FundingInterval]]:
        """Fetch the Aster funding rate and RESOLVE its interval.

        Returns (rate_decimal, FundingInterval), or None if either is unavailable.

        The previous implementation read two history records, took the single gap
        between them as the interval, and fell back to `periods_per_day = 6` on any
        problem. Both halves of that were wrong in the direction that costs money:

          - Aster runs 1h, 4h and 8h cadences simultaneously across symbols. One gap
            is enough to be fooled by a single irregular settlement.
          - Defaulting to 6 periods/day on the 8h symbols - BTC, ETH, SOL, BNB, DOGE,
            LINK, LTC, XRP, i.e. most of the configured universe - DOUBLES the
            reported Aster APR, so the worst-understood symbols sort to the top of
            the opportunity table.

        A symbol whose interval cannot be established is now skipped. Refusing to
        trade it is strictly better than ranking it on a guess.
        """
        logger.debug(f"fetch_aster_rate: Starting for {symbol}")

        try:
            rate: Optional[float] = None
            try:
                premium_data = await aster.get_premium_index(symbol)
                rate = float(premium_data.get('lastFundingRate', 0))
                logger.debug(f"fetch_aster_rate: current rate {rate} for {symbol} from premium index")
            except Exception as premium_err:
                logger.debug(f"fetch_aster_rate: premium index failed for {symbol}, "
                             f"falling back to history: {premium_err}")
                history = await aster.get_funding_rate_history(symbol, limit=1)
                if not history:
                    logger.warning(f"fetch_aster_rate: no funding data at all for {symbol}")
                    return None
                rate = float(history[0].get('fundingRate', 0))

            try:
                interval = await FUNDING_RESOLVER.resolve_from_api_field(
                    "aster", symbol, _aster_interval_hours
                )
            except IntervalResolutionError as field_err:
                logger.debug("fetch_aster_rate: fundingInfo unusable for %s (%s); "
                             "falling back to empirical resolution", symbol, field_err)
                interval = await FUNDING_RESOLVER.resolve_empirically(
                    "aster", symbol, _aster_funding_times
                )

            logger.debug("fetch_aster_rate: %s interval %.4gh (%s)",
                         symbol, interval.hours, interval.source)
            return (rate, interval)

        except IntervalResolutionError as exc:
            logger.warning("Skipping %s: Aster funding interval unresolved (%s)", symbol, exc)
            return None
        except Exception as exc:
            logger.error("Error fetching Aster funding for %s: %s", symbol, exc, exc_info=True)
            return None

    async def fetch_lighter_rate() -> Optional[float]:
        logger.debug(f"fetch_lighter_rate: Starting for {symbol}")

        async def _fetch_with_semaphore():
            api_client = None
            try:
                logger.debug(f"fetch_lighter_rate: Waiting for semaphore for {symbol}")
                async with LIGHTER_API_SEMAPHORE:
                    logger.debug(f"fetch_lighter_rate: Semaphore acquired for {symbol}")
                    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
                    order_api = lighter.OrderApi(api_client)
                    symbol_clean = symbol.replace("USDT", "")

                    logger.debug(f"fetch_lighter_rate: Getting market details for {symbol_clean}")
                    market_id, _, _ = await lighter_client.get_lighter_market_details(order_api, symbol_clean)
                    logger.debug(f"fetch_lighter_rate: Market ID for {symbol_clean}: {market_id}")

                    funding_api = lighter.FundingApi(api_client)
                    logger.debug(f"fetch_lighter_rate: Fetching funding rate for {symbol_clean} (market {market_id})")
                    rate = await lighter_client.get_lighter_funding_rate(funding_api, market_id)
                    logger.debug(f"fetch_lighter_rate: Got rate {rate} for {symbol_clean}")

                    await api_client.close()
                    logger.debug(f"fetch_lighter_rate: API client closed for {symbol}")
                    return rate
            except Exception as e:
                logger.debug(f"fetch_lighter_rate: Exception in _fetch_with_semaphore for {symbol}: {e}")
                if api_client:
                    try:
                        await api_client.close()
                    except Exception:
                        pass
                raise

        try:
            logger.debug(f"fetch_lighter_rate: Starting retry_with_backoff for {symbol}")
            result = await retry_with_backoff(_fetch_with_semaphore, max_retries=3, initial_delay=2.0)
            logger.debug(f"fetch_lighter_rate: Success for {symbol}, rate={result}")
            return result
        except RateLimitError as exc:
            logger.error("Lighter rate limit exceeded for %s after retries: %s", symbol, exc)
            return None
        except Exception as exc:
            logger.error("Error fetching Lighter funding for %s: %s", symbol, exc)
            return None

    # Fetch funding rates and spread
    logger.debug(f"fetch_symbol_funding: Gathering data from both exchanges for {symbol}")
    aster_result, lighter_rate_decimal, spread_data = await asyncio.gather(
        fetch_aster_rate(),
        fetch_lighter_rate(),
        fetch_symbol_spread(symbol, env, aster)
    )
    spread_pct, aster_mid, lighter_mid = spread_data

    # Unpack Aster result (rate, resolved FundingInterval). A None here means either
    # the rate or the interval could not be established - both are disqualifying.
    aster_rate_decimal = None
    aster_interval: Optional[FundingInterval] = None
    if aster_result is not None:
        aster_rate_decimal, aster_interval = aster_result

    logger.debug(
        f"fetch_symbol_funding: Received aster_rate={aster_rate_decimal} "
        f"(interval={aster_interval.hours if aster_interval else None}h), "
        f"lighter_rate={lighter_rate_decimal}, spread={spread_pct} for {symbol}"
    )

    if aster_rate_decimal is None or lighter_rate_decimal is None:
        missing = []
        if aster_rate_decimal is None:
            missing.append("Aster")
        if lighter_rate_decimal is None:
            missing.append("Lighter")
        logger.warning(f"fetch_symbol_funding: Missing data for {symbol}: {missing}")
        return {
            "symbol": symbol,
            "available": False,
            "missing_on": missing or ["Data unavailable"],
            "spread_pct": spread_pct,
            "aster_mid": aster_mid,
            "lighter_mid": lighter_mid,
        }

    # Check spread threshold
    if spread_pct is not None and spread_pct > max_spread_pct:
        logger.info(f"{symbol}: Spread {spread_pct:.3f}% exceeds {max_spread_pct:.2f}% threshold")
        logger.debug(f"fetch_symbol_funding: Rejecting {symbol} due to spread")
        return {
            "symbol": symbol,
            "available": False,
            "excluded_reason": "spread",
            "missing_on": [f"Spread too wide: {spread_pct:.3f}% > {max_spread_pct:.2f}%"],
            "spread_pct": spread_pct,
            "aster_mid": aster_mid,
            "lighter_mid": lighter_mid,
            "aster_rate": aster_rate_decimal * 100 if aster_rate_decimal is not None else None,
            "lighter_rate": lighter_rate_decimal * 100 if lighter_rate_decimal is not None else None,
        }

    # Both APRs now come from a resolved FundingInterval rather than a literal.
    #
    # Lighter settles HOURLY. RESOLVED EMPIRICALLY (not from docs):
    # /api/v1/funding-rates is a cross-venue endpoint returning binance/bybit/
    # hyperliquid/lighter side by side. Comparing Lighter against Hyperliquid - whose
    # convention is established beyond doubt as hourly-decimal - across 98 same-sign
    # common symbols gave a median ratio of 0.9600, with dozens of pairs at exactly
    # 0.000096 vs 0.00010. Same units, same period. So the rate is a DECIMAL and the
    # cadence is HOURLY; the old `periods_per_day=3` understated every Lighter APR by
    # exactly 8x and mis-ranked every cross-venue spread the bot evaluated.
    #
    # That constant now lives in funding_economics.CONSTANT_INTERVALS, where
    # audit_constant_interval() re-checks it against observed settlements at startup.
    lighter_interval = FUNDING_RESOLVER.constant("lighter")
    aster_apr = annualize(aster_rate_decimal, aster_interval)
    lighter_apr = annualize(lighter_rate_decimal, lighter_interval)

    long_aster_short_lighter = lighter_apr - aster_apr
    long_lighter_short_aster = aster_apr - lighter_apr

    if long_aster_short_lighter >= long_lighter_short_aster:
        long_exch = "Aster"
        short_exch = "Lighter"
        net_apr = long_aster_short_lighter
    else:
        long_exch = "Lighter"
        short_exch = "Aster"
        net_apr = long_lighter_short_aster

    result = {
        "symbol": symbol,
        "available": True,
        "aster_rate": aster_rate_decimal * 100 if aster_rate_decimal is not None else None,
        "aster_apr": aster_apr,
        "aster_interval_hours": aster_interval.hours,
        "aster_interval_source": aster_interval.source,
        "aster_periods_per_day": aster_interval.periods_per_day,
        "lighter_rate": lighter_rate_decimal * 100 if lighter_rate_decimal is not None else None,
        "lighter_apr": lighter_apr,
        "lighter_interval_hours": lighter_interval.hours,
        "long_exch": long_exch,
        "short_exch": short_exch,
        "net_apr": net_apr,
        "spread_pct": spread_pct,
        "aster_mid": aster_mid,
        "lighter_mid": lighter_mid,
    }
    logger.debug(
        f"fetch_symbol_funding: Success for {symbol}: net_apr={net_apr:.2f}%, "
        f"long={long_exch}, short={short_exch}, "
        f"aster_interval={aster_interval.hours}h ({aster_interval.source})"
    )
    return result


class HedgeVenues:
    """Builds the two `LegSpec`s that `two_leg.py` drives, for one symbol.

    Open and close both go through this so they cannot drift apart. The close path has
    to cancel, retry and halt with exactly the same semantics the open path unwinds
    with; the previous code hand-rolled two copies of that logic and they did not
    agree - the open path aborted on a rejected leg while the close path printed a
    warning and returned success.

    All venue coupling lives here. `two_leg.py` imports no exchange client at all.
    """

    def __init__(
        self,
        env: dict,
        aster: AsterApiManager,
        signer: lighter.SignerClient,
        order_api,
        account_api,
        symbol: str,
        l_market_id: int,
        l_price_tick: float,
        l_amount_tick: float,
        aster_step_size: float,
        cross_ticks: int = 100,
    ):
        self.env = env
        self.aster = aster
        self.signer = signer
        self.order_api = order_api
        self.account_api = account_api
        self.symbol = symbol
        self.symbol_clean = symbol.replace("USDT", "")
        self.l_market_id = l_market_id
        self.l_price_tick = l_price_tick
        self.l_amount_tick = l_amount_tick
        self.aster_step_size = aster_step_size
        self.cross_ticks = cross_ticks

    # ---- Lighter -------------------------------------------------------
    async def _fresh_lighter_ref(self, side: str) -> Optional[float]:
        """Best bid/ask at the moment of use.

        Re-fetched rather than captured once: an unwind can run many seconds and
        several retries after the opening quotes were taken, and crossing the spread
        against a stale reference is how a "close" order rests instead of filling.
        """
        bid, ask = await lighter_client.get_lighter_best_bid_ask(
            self.order_api, self.symbol_clean, self.l_market_id
        )
        return (ask or bid) if side == "buy" else (bid or ask)

    def lighter_leg(self, side: str, intent_qty: float) -> LegSpec:
        async def submit(qty: float):
            ref = await self._fresh_lighter_ref(side)
            if ref is None:
                raise RuntimeError(
                    f"Lighter: no reference price for {side} on {self.symbol_clean}"
                )
            return await lighter_client.lighter_place_aggressive_order(
                self.signer, self.l_market_id, self.l_price_tick, self.l_amount_tick,
                side, _floor_to_tick(qty, self.l_amount_tick), ref,
                cross_ticks=self.cross_ticks,
            )

        async def read_position() -> float:
            return await lighter_client.get_lighter_open_size(
                self.account_api, self.env["ACCOUNT_INDEX"], self.l_market_id,
                symbol=self.symbol_clean,
            )

        async def close_market(qty: float, close_side: str):
            ref = await self._fresh_lighter_ref(close_side)
            if ref is None:
                raise RuntimeError(f"Lighter: no reference price to close {self.symbol_clean}")
            # Ceil, not floor: the order is reduce-only, so rounding up closes the
            # whole residual, while rounding down leaves sub-tick dust the unwind loop
            # can never clear - driving it to a spurious halt.
            return await lighter_client.lighter_close_position(
                self.signer, self.l_market_id, self.l_price_tick, self.l_amount_tick,
                close_side, _ceil_to_tick(qty, self.l_amount_tick), ref,
                cross_ticks=self.cross_ticks,
            )

        async def cancel_open() -> int:
            return await lighter_client.lighter_cancel_open_orders(
                self.signer, self.order_api, self.env["ACCOUNT_INDEX"], self.l_market_id
            )

        return LegSpec(
            name="Lighter", symbol=self.symbol_clean, side=side, intent_qty=intent_qty,
            submit=submit, read_position=read_position, close_market=close_market,
            cancel_open=cancel_open, amount_tick=self.l_amount_tick,
            # zk batch inclusion takes ~3s; reading sooner reports a false zero and
            # makes verify_fill call a good leg REJECTED.
            settle_delay_s=3.0,
        )

    # ---- Aster ---------------------------------------------------------
    def aster_leg(self, side: str, intent_qty: float) -> LegSpec:
        async def submit(qty: float):
            return await self.aster.place_perp_market_order(
                self.symbol, str(_floor_to_tick(qty, self.aster_step_size)),
                'BUY' if side == "buy" else 'SELL',
            )

        async def read_position() -> float:
            account = await self.aster.get_perp_account_info()
            for pos in account.get('positions', []) or []:
                if pos.get('symbol') == self.symbol:
                    return float(pos.get('positionAmt', 0) or 0)
            return 0.0

        async def close_market(qty: float, close_side: str):
            return await self.aster.close_perp_position(
                self.symbol, str(_ceil_to_tick(qty, self.aster_step_size)),
                'BUY' if close_side == "buy" else 'SELL',
            )

        async def cancel_open() -> int:
            return await self.aster.cancel_all_perp_orders(self.symbol)

        return LegSpec(
            name="Aster", symbol=self.symbol, side=side, intent_qty=intent_qty,
            submit=submit, read_position=read_position, close_market=close_market,
            cancel_open=cancel_open, amount_tick=self.aster_step_size,
            settle_delay_s=1.0,
        )


async def open_delta_neutral_position(
    env: dict,
    aster: AsterApiManager,
    symbol: str,
    long_exchange: str,
    short_exchange: str,
    leverage: float,
    notional_quote: float,
    cross_ticks: int = 100,
) -> Dict[str, object]:
    """
    Open a delta-neutral position across Lighter and Aster perpetual markets.
    Returns metadata describing the trade.
    """
    # Build Lighter client
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
    order_api = lighter.OrderApi(api_client)
    signer = lighter.SignerClient(
        url=env["LIGHTER_BASE_URL"],
        private_key=env["API_KEY_PRIVATE_KEY"],
        account_index=env["ACCOUNT_INDEX"],
        api_key_index=env["API_KEY_INDEX"],
    )
    err = signer.check_client()
    if err:
        await api_client.close()
        raise RuntimeError(f"Lighter check_client error: {err}")

    try:
        # Get Lighter market details
        symbol_clean = symbol.replace("USDT", "")
        l_market_id, l_price_tick, l_amount_tick = await lighter_client.get_lighter_market_details(order_api, symbol_clean)
        lighter_bid, lighter_ask = await lighter_client.get_lighter_best_bid_ask(order_api, symbol_clean, l_market_id)

        # Get Aster market details
        aster_ticker = await aster.get_perp_book_ticker(symbol)
        aster_bid = float(aster_ticker.get('bidPrice', 0)) if aster_ticker.get('bidPrice') else None
        aster_ask = float(aster_ticker.get('askPrice', 0)) if aster_ticker.get('askPrice') else None

        # Get Aster LOT_SIZE filter for precision
        aster_lot_size_filter = await aster.get_perp_symbol_filter(symbol, 'LOT_SIZE')
        if aster_lot_size_filter:
            aster_step_size = float(aster_lot_size_filter.get('stepSize', 0.001))
        else:
            aster_step_size = 0.001

        if not any([lighter_bid, lighter_ask, aster_bid, aster_ask]):
            raise RuntimeError("Could not fetch quotes from either venue.")
        if not (lighter_bid or lighter_ask):
            raise RuntimeError(
                f"Could not fetch any prices from Lighter for {symbol_clean}. "
                "The order book may be empty or the market inactive."
            )
        if not (aster_bid or aster_ask):
            raise RuntimeError(f"Could not fetch any prices from Aster for {symbol}")
    except Exception:
        await signer.close()
        await api_client.close()
        raise

    # Configure leverage on BOTH venues, and refuse to trade if either failed.
    #
    # The return value used to be discarded. Opening anyway means the margin actually
    # applied is whatever each venue happened to have set from a previous cycle, so the
    # liquidation distance the stop-loss is calculated against (100/leverage) is not the
    # liquidation distance in force. Sizing is derived from `leverage` too, so a failed
    # set on one side silently produces an asymmetric hedge.
    aster_lev_ok, lighter_lev_ok = await configure_leverage(
        leverage, env, aster, signer, symbol, l_market_id, verify=True
    )
    if not (aster_lev_ok and lighter_lev_ok):
        failed = [name for name, ok in (("Aster", aster_lev_ok), ("Lighter", lighter_lev_ok))
                  if not ok]
        await signer.close()
        await api_client.close()
        raise RuntimeError(
            f"Leverage configuration failed on {', '.join(failed)}. Refusing to open: "
            f"the applied margin would not match the {leverage}x this position is sized "
            f"and stop-lossed for."
        )

    avg_mid = get_avg_mid(lighter_bid, lighter_ask, aster_bid, aster_ask)
    size_base = compute_base_size_from_quote(avg_mid, float(notional_quote))

    coarser_tick = max(l_amount_tick, aster_step_size)
    size_base = _floor_to_tick(size_base, coarser_tick)

    lighter_rounded = _round_to_tick(size_base, l_amount_tick)
    aster_rounded = _round_to_tick(size_base, aster_step_size)
    if abs(lighter_rounded - aster_rounded) > min(l_amount_tick, aster_step_size):
        size_base = _floor_to_tick(size_base, coarser_tick)
        logger.warning("Adjusted size to %s to ensure same size on both exchanges", size_base)

    if size_base <= 0:
        await signer.close()
        await api_client.close()
        raise RuntimeError("Computed size rounds to zero. Increase notional.")

    # Check minimum size requirements
    lighter_min_size = l_amount_tick * 10
    aster_min_size = aster_step_size * 10
    min_errors: List[str] = []

    if size_base < lighter_min_size:
        lighter_min_usd = lighter_min_size * avg_mid
        min_errors.append(f"Lighter estimated minimum: {lighter_min_size} {symbol_clean} (${lighter_min_usd:.2f} USD)")

    if size_base < aster_min_size:
        aster_min_usd = aster_min_size * avg_mid
        min_errors.append(f"Aster estimated minimum: {aster_min_size} {symbol_clean} (${aster_min_usd:.2f} USD)")

    if min_errors:
        await signer.close()
        await api_client.close()
        raise RuntimeError(
            "Order size too small. Requirements:\n" + "\n".join(f"  - {err}" for err in min_errors)
        )

    long_leg = long_exchange.lower()
    short_leg = short_exchange.lower()
    if long_leg == short_leg:
        await signer.close()
        await api_client.close()
        raise RuntimeError("Long and short exchanges cannot be identical.")
    if {long_leg, short_leg} != {"lighter", "aster"}:
        await signer.close()
        await api_client.close()
        raise RuntimeError(
            f"Unsupported venue pair (long={long_exchange}, short={short_exchange}). "
            f"This bot trades Lighter against Aster only."
        )

    account_api = lighter.AccountApi(api_client)
    lighter_side = "buy" if long_leg == "lighter" else "sell"
    aster_side = "sell" if lighter_side == "buy" else "buy"

    venues = HedgeVenues(
        env, aster, signer, order_api, account_api, symbol,
        l_market_id, l_price_tick, l_amount_tick, aster_step_size,
        cross_ticks=cross_ticks,
    )

    # Lighter is the PILOT because its orders are GOOD_TILL_TIME and can rest silently
    # on the book; Aster's are market orders that resolve immediately. Submit the leg
    # that can leave a surprise first, cancel it, verify it, and only then commit the
    # other side - sized from what actually filled, never from the original intent.
    pilot = venues.lighter_leg(lighter_side, size_base)
    hedge = venues.aster_leg(aster_side, size_base)

    print(f"\n{Colors.CYAN}Opening hedge sequentially: "
          f"{Colors.BOLD}Lighter {lighter_side.upper()}{Colors.RESET}{Colors.CYAN} (pilot) "
          f"then {Colors.BOLD}Aster {aster_side.upper()}{Colors.RESET}{Colors.CYAN} "
          f"(hedge), {size_base:.6f} {symbol_clean}{Colors.RESET}")

    try:
        outcome = await execute_two_leg(
            pilot, hedge,
            min_notional_qty=max(l_amount_tick, aster_step_size) * 10,
        )
    finally:
        await signer.close()
        await api_client.close()

    for note in outcome.notes:
        logger.info("two-leg: %s", note)
        print(f"  {Colors.GRAY}- {note}{Colors.RESET}")

    if not outcome.ok:
        print(f"\n{Colors.RED}{Colors.BOLD}❌ Hedge NOT opened: {outcome.reason}{Colors.RESET}")
        for leg in (outcome.pilot, outcome.hedge):
            if leg is not None:
                print(f"   {Colors.GRAY}{leg.venue}: status={leg.status.value} "
                      f"filled={leg.filled_qty:.8g} error={leg.error}{Colors.RESET}")
        if outcome.halted:
            print(f"\n{Colors.RED}{Colors.BOLD}⚠️  HALTED: an unwind did not complete. "
                  f"Real unhedged exposure may exist. Check BOTH venues manually, then "
                  f"remove halt.json to resume.{Colors.RESET}\n")
        raise RuntimeError(f"Delta-neutral open failed: {outcome.reason}")

    # hedged_qty is the size CONFIRMED on both venues. Everything downstream - the
    # monitor's position value, the stop-loss percentage, the close - keys off this,
    # so it must never be the requested size.
    filled_base = outcome.hedged_qty
    filled_notional = filled_base * avg_mid

    print(
        f"\n{Colors.GREEN}{Colors.BOLD}✓ Hedge opened and verified on both venues{Colors.RESET}\n"
        f"  {Colors.CYAN}Confirmed size: {Colors.BOLD}{filled_base:.6f} {symbol_clean}"
        f"{Colors.RESET}{Colors.CYAN} (~${filled_notional:,.2f}) on each exchange{Colors.RESET}\n"
        f"  {Colors.CYAN}Delta-neutral: {Colors.BOLD}LONG {long_exchange.capitalize()}"
        f"{Colors.RESET}{Colors.CYAN}, {Colors.BOLD}SHORT {short_exchange.capitalize()}"
        f"{Colors.RESET}\n"
    )
    if abs(filled_base - size_base) > 1e-12:
        logger.warning(
            "Hedge filled %.10g of a requested %.10g %s (%.2f%%); downstream sizing "
            "uses the filled amount.",
            filled_base, size_base, symbol_clean,
            (filled_base / size_base * 100.0) if size_base else 0.0,
        )
    logger.info("Opened hedge: confirmed %.10g %s on both venues", filled_base, symbol_clean)

    return {
        "lighter_market_id": l_market_id,
        "lighter_price_tick": l_price_tick,
        "lighter_amount_tick": l_amount_tick,
        "aster_step_size": aster_step_size,
        "aster_bid": aster_bid,
        "aster_ask": aster_ask,
        "lighter_bid": lighter_bid,
        "lighter_ask": lighter_ask,
        "size_base": filled_base,
        "requested_size_base": size_base,
        "filled_notional": filled_notional,
        "avg_mid": avg_mid,
        "lighter_side": lighter_side,
        "aster_side": aster_side,
    }


async def close_delta_neutral_position(
    env: dict,
    aster: AsterApiManager,
    symbol: str,
    cross_ticks: int = 100,
) -> None:
    """Close positions on both exchanges for the specified symbol."""
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
    order_api = lighter.OrderApi(api_client)
    account_api = lighter.AccountApi(api_client)
    signer = lighter.SignerClient(
        url=env["LIGHTER_BASE_URL"],
        private_key=env["API_KEY_PRIVATE_KEY"],
        account_index=env["ACCOUNT_INDEX"],
        api_key_index=env["API_KEY_INDEX"],
    )
    err = signer.check_client()
    if err:
        await api_client.close()
        raise RuntimeError(f"Lighter check_client error: {err}")

    symbol_clean = symbol.replace("USDT", "")
    l_market_id, l_price_tick, l_amount_tick = await lighter_client.get_lighter_market_details(order_api, symbol_clean)

    # No bid/ask fetch here any more: HedgeVenues re-reads the book at the moment each
    # close order is actually sent, which matters because an unwind can retry several
    # times over tens of seconds.
    aster_lot_size_filter = await aster.get_perp_symbol_filter(symbol, 'LOT_SIZE')
    aster_step_size = float(aster_lot_size_filter.get('stepSize', 0.001)) if aster_lot_size_filter else 0.001

    print(f"\n{Colors.RED}{Colors.BOLD}┌{'─' * 66}┐{Colors.RESET}")
    print(f"{Colors.RED}{Colors.BOLD}│{'Closing Delta-Neutral Hedge':^66}│{Colors.RESET}")
    print(f"{Colors.RED}{Colors.BOLD}├{'─' * 66}┤{Colors.RESET}")
    print(f"{Colors.RED}{Colors.BOLD}│{Colors.RESET}  Symbol: {Colors.BOLD}{symbol}{Colors.RESET}                                                  {Colors.RED}{Colors.BOLD}│{Colors.RESET}")
    print(f"{Colors.RED}{Colors.BOLD}└{'─' * 66}┘{Colors.RESET}\n")

    print(f"{Colors.CYAN}Checking current positions...{Colors.RESET}")

    # Get Aster position
    aster_account = await aster.get_perp_account_info()
    aster_positions = aster_account.get('positions', [])
    aster_size = 0.0
    for pos in aster_positions:
        if pos.get('symbol') == symbol:
            aster_size = float(pos.get('positionAmt', 0))
            break
    aster_color = Colors.YELLOW if abs(aster_size) > 0.0001 else Colors.GRAY
    print(f"  {aster_color}Aster position:  {aster_size:+.6f} {symbol_clean}{Colors.RESET}")

    # If this read fails it now RAISES (PositionFetchError) rather than returning
    # 0.0. That is deliberate and must not be "helpfully" caught here: a false zero
    # means the close below sends no Lighter order at all, and the verification at
    # the end then reads 0.0 again and declares success over a still-open leg.
    try:
        lighter_size = await lighter_client.get_lighter_open_size(account_api, env["ACCOUNT_INDEX"], l_market_id)
    except lighter_client.PositionFetchError as e:
        print(f"\n{Colors.RED}{Colors.BOLD}ABORTING CLOSE: cannot read the Lighter position "
              f"({e}).{Colors.RESET}")
        print(f"  {Colors.YELLOW}Refusing to proceed - closing on a position size we could "
              f"not verify risks leaving a leg open while reporting success. "
              f"Check both venues manually.{Colors.RESET}\n")
        raise
    lighter_color = Colors.YELLOW if abs(lighter_size) > l_amount_tick else Colors.GRAY
    print(f"  {lighter_color}Lighter position: {lighter_size:+.6f} {symbol_clean}{Colors.RESET}")

    print(f"\n{Colors.CYAN}Closing positions on both exchanges...{Colors.RESET}")

    # Close through the same unwind primitive the open path uses when it has to back
    # out, instead of a hand-rolled send-and-hope.
    #
    # unwind_leg cancels resting orders FIRST, re-reads the position, closes whatever
    # residual is actually there, and repeats before writing a halt sentinel. That
    # ordering is the point: the Lighter close is a GOOD_TILL_TIME reduce-only order,
    # so a single unverified send can sit on the book and fill much later - after the
    # cycle has been recorded complete and the position forgotten.
    venues = HedgeVenues(
        env, aster, signer, order_api, account_api, symbol,
        l_market_id, l_price_tick, l_amount_tick, aster_step_size,
        cross_ticks=cross_ticks,
    )
    lighter_leg = venues.lighter_leg(
        "buy" if lighter_size < 0 else "sell", max(abs(lighter_size), l_amount_tick)
    )
    aster_leg = venues.aster_leg(
        "buy" if aster_size < 0 else "sell", max(abs(aster_size), aster_step_size)
    )

    to_unwind: List[LegSpec] = []
    if abs(lighter_size) > l_amount_tick:
        to_unwind.append(lighter_leg)
    else:
        print(f"  {Colors.GRAY}Lighter: already flat (below one amount tick).{Colors.RESET}")
    if abs(aster_size) > 0.0001:
        to_unwind.append(aster_leg)
    else:
        print(f"  {Colors.GRAY}Aster: already flat.{Colors.RESET}")

    try:
        if not to_unwind:
            print(f"{Colors.GREEN}✓ Nothing to close - both venues already flat.{Colors.RESET}")
            return

        # Unwound concurrently: closing both together keeps the hedge balanced for as
        # long as possible. Each leg writes its own halt sentinel on exhaustion.
        results = await asyncio.gather(
            *(unwind_leg(leg, baseline_signed_qty=0.0) for leg in to_unwind),
            return_exceptions=True,
        )

        failures: List[str] = []
        for leg, result in zip(to_unwind, results):
            if isinstance(result, BaseException):
                failures.append(f"{leg.name}: {type(result).__name__}: {result}")
            elif result is not True:
                failures.append(f"{leg.name}: residual position could not be flattened")

        if failures:
            # A close that did not complete must NOT return normally. It used to print
            # a warning and fall through, after which the caller recorded the cycle as
            # "success" and cleared current_position - leaving a live leg with no
            # monitoring, no stop-loss, and no record anywhere that it existed.
            print(f"\n{Colors.RED}{Colors.BOLD}❌ CLOSE INCOMPLETE{Colors.RESET}")
            for failure in failures:
                print(f"   {Colors.RED}- {failure}{Colors.RESET}")
            print(f"\n{Colors.YELLOW}A halt sentinel has been written. The bot will "
                  f"refuse to trade until BOTH venues are checked manually and "
                  f"halt.json is removed.{Colors.RESET}\n")
            raise RuntimeError(f"Close incomplete for {symbol}: " + "; ".join(failures))

        # Final explicit read, for the operator record rather than for the decision -
        # unwind_leg has already established flatness within tolerance.
        print(f"\n{Colors.CYAN}Verifying closure...{Colors.RESET}")
        aster_size_after = await aster_leg.read_position()
        print(f"  {Colors.GREEN if abs(aster_size_after) < 0.0001 else Colors.RED}"
              f"Aster position:   {aster_size_after:+.6f} {symbol_clean}{Colors.RESET}")
        try:
            lighter_size_after = await lighter_leg.read_position()
        except lighter_client.PositionFetchError as exc:
            print(f"\n{Colors.YELLOW}Closure was confirmed by the unwind, but the final "
                  f"Lighter read failed ({exc}). Treating the unwind result as "
                  f"authoritative.{Colors.RESET}")
        else:
            print(f"  {Colors.GREEN if abs(lighter_size_after) < l_amount_tick else Colors.RED}"
                  f"Lighter position: {lighter_size_after:+.6f} {symbol_clean}{Colors.RESET}")

        print(f"\n{Colors.GREEN}{Colors.BOLD}✓ Hedge closed and verified flat on both "
              f"exchanges{Colors.RESET}")
    finally:
        await signer.close()
        await api_client.close()


class StateManager:
    """Manages bot state persistence and recovery."""

    def __init__(self, state_file: str = "bot_state.json"):
        self.state_file = state_file
        self.state = {
            "version": "1.0",
            "state": BotState.IDLE,
            "current_cycle": 0,
            "current_position": None,
            "capital_status": {
                "aster_total": 0.0,
                "aster_available": 0.0,
                "lighter_total": 0.0,
                "lighter_available": 0.0,
                "total_capital": 0.0,
                "total_available": 0.0,
                "max_position_notional": 0.0,
                "limiting_exchange": None,
                "last_updated": None,
                "initial_total_capital": None
            },
            "completed_cycles": [],
            "cumulative_stats": {
                "total_cycles": 0,
                "successful_cycles": 0,
                "failed_cycles": 0,
                "total_realized_pnl": 0.0,
                "total_trading_pnl": 0.0,
                "total_funding_pnl": 0.0,
                "total_fees_paid": 0.0,
                "best_cycle_pnl": 0.0,
                "worst_cycle_pnl": 0.0,
                "total_volume_traded": 0.0,
                "total_hold_time_hours": 0.0,
                "by_symbol": {},
                "last_error": None,
                "last_error_at": None
            },
            "config": None,
            "last_updated": utc_now_iso()
        }

    def load(self) -> bool:
        """Load state from file. Returns True if loaded successfully."""
        if not os.path.exists(self.state_file):
            logger.info(f"No state file found at {self.state_file}, starting fresh")
            return False

        try:
            with open(self.state_file, 'r') as f:
                content = f.read().strip()

            if not content:
                logger.info(f"State file {self.state_file} is empty, starting fresh")
                return False

            loaded_state = json.loads(content)
            self.state.update(loaded_state)

            # Ensure capital_status exists
            if "capital_status" not in self.state:
                self.state["capital_status"] = {
                    "aster_total": 0.0,
                    "aster_available": 0.0,
                    "lighter_total": 0.0,
                    "lighter_available": 0.0,
                    "total_capital": 0.0,
                    "total_available": 0.0,
                    "max_position_notional": 0.0,
                    "limiting_exchange": None,
                    "last_updated": None,
                    "initial_total_capital": None
                }

            if "initial_total_capital" not in self.state["capital_status"]:
                self.state["capital_status"]["initial_total_capital"] = None

            logger.info(f"Loaded state from {self.state_file}")
            logger.info(f"Current state: {self.state['state']}")
            return True
        except json.JSONDecodeError as e:
            logger.warning(f"State file {self.state_file} is corrupted or invalid JSON: {e}")
            logger.info("Starting fresh with new state")
            return False
        except Exception as e:
            logger.warning(f"Could not load state file: {e}")
            logger.info("Starting fresh with new state")
            return False

    def save(self):
        """Save current state to file."""
        import time
        self.state["last_updated"] = utc_now_iso()

        max_retries = 3
        for attempt in range(max_retries):
            try:
                temp_file = self.state_file + ".tmp"
                with open(temp_file, 'w') as f:
                    json.dump(self.state, f, indent=2)
                os.replace(temp_file, self.state_file)
                logger.debug(f"Saved state to {self.state_file}")
                return
            except OSError as e:
                if e.errno == 16 and attempt < max_retries - 1:
                    time.sleep(0.1 * (attempt + 1))
                    continue
                elif attempt == max_retries - 1:
                    logger.debug(f"Failed to save state after {max_retries} attempts: {e}")
                else:
                    logger.error(f"Failed to save state: {e}")
                    break
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                break

    def set_state(self, new_state: str):
        """Update bot state."""
        logger.info(f"State transition: {self.state['state']} → {new_state}")
        self.state["state"] = new_state
        self.save()

    def get_state(self) -> str:
        """Get current bot state."""
        return self.state["state"]

    def set_config(self, config: BotConfig):
        """Set bot configuration."""
        self.state["config"] = asdict(config)
        self.save()

    def get_config(self) -> Optional[BotConfig]:
        """Get bot configuration."""
        if self.state["config"]:
            return BotConfig(**self.state["config"])
        return None


# ==================== Balance Helpers ====================

async def get_aster_balance(aster: AsterApiManager) -> Tuple[float, float]:
    """Get Aster total and available USD balance."""
    try:
        perp_account = await aster.get_perp_account_info()
        perp_assets = perp_account.get('assets', [])

        total = 0.0
        available = 0.0

        for asset in perp_assets:
            if asset.get('asset') == 'USDT':
                total = float(asset.get('walletBalance', 0))
                available = float(asset.get('availableBalance', 0))
                break

        logger.info("Aster balance: total=%s, available=%s", total, available)
        return total, available
    except Exception as exc:
        logger.error("Error fetching Aster balance: %s", exc, exc_info=True)
        raise BalanceFetchError(f"Aster balance fetch failed: {exc}") from exc


async def get_lighter_balance(env: dict) -> Tuple[float, float]:
    """Get Lighter total and available USD balance via WebSocket."""
    try:
        account_index = int(env.get("ACCOUNT_INDEX", "0"))
        ws_url = env["LIGHTER_WS_URL"]
        available, portfolio_value = await lighter_client.get_lighter_balance(ws_url, account_index, timeout=10.0)
        if available is None or portfolio_value is None:
            raise BalanceFetchError("Lighter WebSocket returned None values")
        logger.info("Lighter balance: total=%s, available=%s", portfolio_value, available)
        return portfolio_value, available
    except BalanceFetchError:
        raise
    except Exception as exc:
        logger.error("Error fetching Lighter balance: %s: %s", type(exc).__name__, exc, exc_info=True)
        raise BalanceFetchError(f"Lighter balance fetch failed: {type(exc).__name__}: {exc}") from exc


async def update_capital_status(
    env: dict,
    aster: AsterApiManager,
    state_mgr: 'StateManager',
    config: BotConfig
) -> bool:
    """
    Fetch balances from both exchanges and update capital_status in state.

    Returns:
        True if successful, False if any balance fetch failed
    """
    try:
        logger.info("Fetching balances from both exchanges...")

        # Fetch balances from both exchanges
        aster_total, aster_available = await get_aster_balance(aster)
        lighter_total, lighter_available = await get_lighter_balance(env)

        # Calculate totals
        total_capital = aster_total + lighter_total
        total_available = aster_available + lighter_available

        # Calculate max position notional considering leverage
        # We need capital on BOTH exchanges, so use the minimum
        max_per_exchange = min(aster_available, lighter_available)

        # With leverage, the position notional can be larger than available capital
        # But we still need enough margin on both sides
        max_position_notional = max_per_exchange * config.leverage

        # Determine limiting exchange
        limiting_exchange = "Aster" if aster_available <= lighter_available else "Lighter"

        # Store initial capital on first fetch
        if state_mgr.state["capital_status"]["initial_total_capital"] is None:
            state_mgr.state["capital_status"]["initial_total_capital"] = total_capital

        # Update capital status
        state_mgr.state["capital_status"].update({
            "aster_total": aster_total,
            "aster_available": aster_available,
            "lighter_total": lighter_total,
            "lighter_available": lighter_available,
            "total_capital": total_capital,
            "total_available": total_available,
            "max_position_notional": max_position_notional,
            "limiting_exchange": limiting_exchange,
            "last_updated": utc_now_iso()
        })
        state_mgr.save()

        # Log capital status with colors
        logger.info(
            f"\n{Colors.BOLD}{'═' * 80}\n"
            f"CAPITAL STATUS\n"
            f"{'═' * 80}{Colors.RESET}\n"
            f"  {Colors.CYAN}Aster:{Colors.RESET}   Total: ${aster_total:,.2f} | Available: {Colors.GREEN}${aster_available:,.2f}{Colors.RESET}\n"
            f"  {Colors.CYAN}Lighter:{Colors.RESET}  Total: ${lighter_total:,.2f} | Available: {Colors.GREEN}${lighter_available:,.2f}{Colors.RESET}\n"
            f"  {Colors.BOLD}Combined:{Colors.RESET} Total: ${total_capital:,.2f} | Available: {Colors.GREEN}${total_available:,.2f}{Colors.RESET}\n"
            f"  {Colors.YELLOW}Max Position Notional:{Colors.RESET} ${max_position_notional:,.2f} (limited by {Colors.CYAN}{limiting_exchange}{Colors.RESET})\n"
            f"  {Colors.YELLOW}Configured Notional:{Colors.RESET} ${config.notional_per_position:,.2f}\n"
            f"{Colors.BOLD}{'═' * 80}{Colors.RESET}"
        )

        return True

    except BalanceFetchError as e:
        logger.error(f"Failed to fetch balances: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error updating capital status: {e}", exc_info=True)
        return False


def calculate_affordable_notional(
    state_mgr: 'StateManager',
    config: BotConfig,
    requested_notional: Optional[float] = None
) -> Tuple[float, bool]:
    """
    Calculate the affordable position notional based on available capital.

    Args:
        state_mgr: State manager containing capital_status
        config: Bot configuration
        requested_notional: Requested notional (defaults to config.notional_per_position)

    Returns:
        Tuple of (affordable_notional, was_adjusted)
        - affordable_notional: The actual notional to use for the position
        - was_adjusted: True if the notional was reduced from requested amount
    """
    if requested_notional is None:
        requested_notional = config.notional_per_position

    capital_status = state_mgr.state.get("capital_status", {})
    max_position_notional = capital_status.get("max_position_notional", 0.0)

    # Apply safety margin
    safe_max_notional = max_position_notional * config.capital_safety_margin

    if requested_notional <= safe_max_notional:
        # We can afford the full requested amount
        return requested_notional, False
    else:
        # Need to reduce to what we can afford
        affordable = safe_max_notional

        # Log the adjustment
        limiting_exchange = capital_status.get("limiting_exchange", "Unknown")
        aster_available = capital_status.get("aster_available", 0.0)
        lighter_available = capital_status.get("lighter_available", 0.0)

        logger.warning(
            f"\n{Colors.YELLOW}{Colors.BOLD}⚠️  POSITION SIZE ADJUSTED{Colors.RESET}\n"
            f"  {Colors.YELLOW}Requested:{Colors.RESET} ${requested_notional:,.2f}\n"
            f"  {Colors.GREEN}Affordable:{Colors.RESET} ${affordable:,.2f} (with {config.capital_safety_margin*100:.0f}% safety margin)\n"
            f"  {Colors.CYAN}Limited by:{Colors.RESET} {limiting_exchange} (${aster_available:,.2f} Aster, ${lighter_available:,.2f} Lighter available)\n"
            f"  {Colors.GRAY}Safety margin reduces max from ${max_position_notional:,.2f} to ${safe_max_notional:,.2f}{Colors.RESET}"
        )

        return affordable, True


def build_cost_model(config: BotConfig) -> TradeCostModel:
    """Round-trip cost model for one Aster/Lighter cycle.

    Taker fees come from `funding_economics.VERIFIED_TAKER_BPS` so there is exactly one
    place where a fee number lives: Aster 4.0bps, Lighter genuinely 0.0 on the standard
    account. With `slippage_bps_per_leg = 0` this reproduces the 0.080% round trip the
    bot has always assumed, so nothing changes until slippage is calibrated.
    """
    slip = float(config.slippage_bps_per_leg)
    return TradeCostModel(legs=(
        VenueCosts("aster", VERIFIED_TAKER_BPS["aster"], slip, source="verified"),
        VenueCosts("lighter", VERIFIED_TAKER_BPS["lighter"], slip, source="verified"),
    ))


async def record_cycle_result(
    state_mgr: 'StateManager',
    env: dict,
    aster: AsterApiManager,
    config: BotConfig,
    position: dict,
    status: str,
    extra: Optional[Dict[str, object]] = None,
) -> Optional[float]:
    """Append a finished cycle and fold its realised PnL into cumulative stats.

    Shared by the hold-expiry close and the stop-loss close. This measurement used to
    exist only on the hold-expiry path, so every stop-loss cycle - the ones that lose
    money more or less by definition - incremented `failed_cycles` but contributed
    nothing to `total_realized_pnl`. The "cumulative realised PnL is NEGATIVE" warning
    was therefore evaluated over a sample with the losses filtered out, which is the
    one bias that makes such a warning useless.

    Realised PnL is measured as total capital after minus total capital before. That
    captures funding, fees and slippage together, with no cost model to be wrong about.
    """
    stats = state_mgr.state["cumulative_stats"]
    cap_before = position.get("capital_at_open")
    cycle_pnl: Optional[float] = None

    try:
        if await update_capital_status(env, aster, state_mgr, config):
            cap_now = state_mgr.state["capital_status"].get("total_capital")
            if cap_before is not None and cap_now:
                cycle_pnl = float(cap_now) - float(cap_before)
    except Exception as exc:                                    # noqa: BLE001
        logger.warning("Could not measure realised cycle PnL: %s", exc)

    notional = float(position.get("actual_notional", 0.0) or 0.0)
    est_fees = notional * build_cost_model(config).roundtrip_pct() / 100.0

    hold_hours = 0.0
    try:
        opened = from_iso_z(position["opened_at"])
        hold_hours = max(0.0, (utc_now() - opened).total_seconds() / 3600.0)
    except Exception:                                           # noqa: BLE001
        pass

    entry: Dict[str, object] = {
        "symbol": position.get("symbol"),
        "opened_at": position.get("opened_at"),
        "closed_at": utc_now_iso(),
        "expected_net_apr": position.get("expected_net_apr", 0.0),
        "notional": notional,
        "hold_hours": round(hold_hours, 4),
        "realized_pnl": cycle_pnl,
        "estimated_fees": est_fees,
        "status": status,
    }
    if extra:
        entry.update(extra)
    state_mgr.state["completed_cycles"].append(entry)

    stats["total_cycles"] = stats.get("total_cycles", 0) + 1
    if status == "success":
        stats["successful_cycles"] = stats.get("successful_cycles", 0) + 1
    else:
        stats["failed_cycles"] = stats.get("failed_cycles", 0) + 1
    stats["total_fees_paid"] = stats.get("total_fees_paid", 0.0) + est_fees
    stats["total_volume_traded"] = stats.get("total_volume_traded", 0.0) + notional
    stats["total_hold_time_hours"] = stats.get("total_hold_time_hours", 0.0) + hold_hours

    symbol = str(position.get("symbol") or "UNKNOWN")
    by_symbol = stats.setdefault("by_symbol", {})
    sym_stats = by_symbol.setdefault(symbol, {"cycles": 0, "realized_pnl": 0.0})
    sym_stats["cycles"] += 1

    if cycle_pnl is not None:
        stats["total_realized_pnl"] = stats.get("total_realized_pnl", 0.0) + cycle_pnl
        stats["best_cycle_pnl"] = max(stats.get("best_cycle_pnl", 0.0), cycle_pnl)
        stats["worst_cycle_pnl"] = min(stats.get("worst_cycle_pnl", 0.0), cycle_pnl)
        sym_stats["realized_pnl"] = sym_stats.get("realized_pnl", 0.0) + cycle_pnl

        pnl_color = Colors.GREEN if cycle_pnl >= 0 else Colors.RED
        logger.info(
            f"Cycle realised PnL ({status}): {pnl_color}${cycle_pnl:+.4f}{Colors.RESET} "
            f"| cumulative: ${stats['total_realized_pnl']:+.4f} over "
            f"{stats['total_cycles']} cycle(s)"
        )
        if stats["total_realized_pnl"] < 0 and stats["total_cycles"] >= 3:
            logger.warning(
                f"{Colors.YELLOW}Cumulative realised PnL is NEGATIVE "
                f"(${stats['total_realized_pnl']:+.4f} over {stats['total_cycles']} "
                f"cycles). The configured threshold may not be clearing round-trip "
                f"costs.{Colors.RESET}"
            )
    else:
        logger.warning("Cycle PnL unmeasured - capital snapshot unavailable.")

    state_mgr.save()
    return cycle_pnl


def format_price(price: Optional[float]) -> str:
    """
    Format price with appropriate precision based on magnitude.

    - >= $100: 2 decimals (e.g., $114,817.15)
    - >= $10: 4 decimals (e.g., $97.9600)
    - >= $1: 4 decimals (e.g., $1.4900)
    - < $1: 6 decimals (e.g., $0.210000)
    """
    if price is None:
        return "N/A"

    if price >= 100:
        return f"${price:,.2f}"
    elif price >= 1:
        return f"${price:,.4f}"
    else:
        return f"${price:,.6f}"


def display_funding_table(available: List[dict], unavailable: List[dict], current_symbol: Optional[str] = None, limit: int = 10):
    """Display formatted funding rates table."""
    print(f"\n{Colors.BOLD}{'═' * 150}")
    print(f"{'FUNDING RATE ANALYSIS':^150}")
    print(f"{'═' * 150}{Colors.RESET}\n")

    if available:
        print(f"{Colors.GREEN}Available Opportunities (Top {min(limit, len(available))} by Net APR):{Colors.RESET}\n")
        # Break-even and margin are shown per row so a rejected opportunity explains
        # itself. A bare gross APR cannot: whether 45% is good depends entirely on the
        # hold length and the round trip, and those are what the operator needs to see
        # when the bot declines to trade for hours at a time.
        print(f"{'Symbol':<10} {'Net APR':<10} {'Exp APR':<10} {'BrkEven':<9} {'Margin':<8} "
              f"{'Net $':<10} {'Long':<8} {'Short':<8} {'Ast APR':<10} {'Lgt APR':<10} "
              f"{'Ivl':<6} {'Spread':<9} {'Verdict':<8}")
        print(f"{'-' * 150}")

        for r in available[:limit]:
            marker = f"{Colors.CYAN}→{Colors.RESET}" if r['symbol'] == current_symbol else " "
            color = Colors.GREEN if r['net_apr'] >= 10 else Colors.YELLOW if r['net_apr'] >= 5 else Colors.RESET
            spread_str = f"{r['spread_pct']:.3f}%" if r.get('spread_pct') is not None else "N/A"
            interval_str = (f"{r['aster_interval_hours']:.0f}h"
                            if r.get('aster_interval_hours') else "?")

            decision = r.get('decision')
            if decision is not None:
                verdict = (f"{Colors.GREEN}ACCEPT{Colors.RESET}" if decision.accept
                           else f"{Colors.GRAY}reject{Colors.RESET}")
                exp_str = f"{decision.expected_apr_pct:>8.2f}%"
                be_str = f"{decision.break_even_apr_pct:>7.2f}%"
                margin_str = f"{decision.margin_ratio:>6.2f}x"
                net_str = f"${decision.expected_net_usd:>+8.2f}"
            else:
                verdict, exp_str, be_str, margin_str, net_str = "-", " " * 9, " " * 8, " " * 7, " " * 9

            print(f"{marker} {r['symbol']:<8} {color}{r['net_apr']:>8.2f}%{Colors.RESET} "
                  f"{exp_str:<10} {be_str:<9} {margin_str:<8} {net_str:<10} "
                  f"{r['long_exch']:<8} {r['short_exch']:<8} "
                  f"{r['aster_apr']:>8.2f}% {r['lighter_apr']:>8.2f}% "
                  f"{interval_str:<6} {spread_str:<9} {verdict:<8}")

        rejected_rows = [r for r in available[:limit]
                         if r.get('decision') is not None and not r['decision'].accept]
        if rejected_rows:
            print(f"\n{Colors.GRAY}Why rejected:{Colors.RESET}")
            for r in rejected_rows[:5]:
                print(f"  {Colors.GRAY}{r['symbol']:<10} {r['decision'].reason}{Colors.RESET}")

    # Separate spread-excluded from other unavailable
    spread_excluded = [r for r in unavailable if r.get('excluded_reason') == 'spread']
    other_unavailable = [r for r in unavailable if r.get('excluded_reason') != 'spread']

    if spread_excluded:
        print(f"\n{Colors.YELLOW}Excluded due to High Spread:{Colors.RESET}\n")
        print(f"{'Symbol':<10} {'Spread':<10} {'Aster Mid':<15} {'Lighter Mid':<15} {'Aster Rate':<12} {'Lighter Rate':<14}")
        print(f"{'-' * 150}")

        for r in spread_excluded[:limit]:
            spread_str = f"{r['spread_pct']:.3f}%" if r.get('spread_pct') is not None else "N/A"
            aster_mid_str = format_price(r.get('aster_mid'))
            lighter_mid_str = format_price(r.get('lighter_mid'))
            aster_rate_str = f"{r['aster_rate']:.4f}%" if r.get('aster_rate') is not None else "N/A"
            lighter_rate_str = f"{r['lighter_rate']:.4f}%" if r.get('lighter_rate') is not None else "N/A"

            print(f"  {r['symbol']:<8} {Colors.YELLOW}{spread_str:<10}{Colors.RESET} "
                  f"{aster_mid_str:<15} {lighter_mid_str:<15} {aster_rate_str:<12} {lighter_rate_str:<14}")

    if other_unavailable:
        print(f"\n{Colors.GRAY}Excluded Symbols (Missing Data):{Colors.RESET}\n")
        print(f"{'Symbol':<12} {'Reason':<80}")
        print(f"{'-' * 150}")

        for r in other_unavailable[:limit]:
            reasons = r.get('missing_on', ['Unknown'])
            reason_str = ', '.join(reasons)
            print(f"  {r['symbol']:<10} {reason_str:<80}")

    print(f"\n{Colors.BOLD}{'═' * 150}{Colors.RESET}\n")


# ==================== Position Recovery ====================

async def reconcile_positions_at_boot(
    state_mgr: StateManager,
    env: dict,
    aster: AsterApiManager,
    config: BotConfig,
) -> bool:
    """Establish ground truth from both venues before trusting the state file.

    Replaces verify_and_recover_position, which had two defects pointing the same way -
    towards forgetting live money:

    1. It cleared `current_position` from inside a bare `except Exception`, so a
       transient API error at boot erased the bot's only record of a live hedge. That
       also silently defeated the PositionFetchError raise in lighter_client: the
       exception added there precisely so a failed read could NOT be mistaken for
       "flat" was caught here and converted straight back into "flat".
    2. It only ever inspected the ONE symbol named in the state file, so a live
       position in any other symbol was invisible - including one in a symbol since
       removed from `symbols_to_monitor`.

    boot_reconcile queries account-level listings from both venues instead, and venue
    truth outranks the state file unconditionally. The state file contributes metadata
    (opened_at, capital_at_open) and nothing else.

    Returns True when a valid hedged position is being held.
    """
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
    account_api = lighter.AccountApi(api_client)

    def _clean(sym: str) -> str:
        return str(sym).upper().replace("USDT", "")

    async def aster_positions() -> Dict[str, float]:
        return {_clean(s): q for s, q in (await aster.get_all_perp_positions()).items()}

    async def lighter_positions() -> Dict[str, float]:
        raw = await lighter_client.list_lighter_positions(account_api, env["ACCOUNT_INDEX"])
        return {_clean(s): q for s, q in raw.items()}

    position = state_mgr.state.get("current_position") or {}
    state_symbol = _clean(position["symbol"]) if position.get("symbol") else None

    try:
        decision = await boot_reconcile(
            [("Aster", aster_positions), ("Lighter", lighter_positions)],
            state_symbols=[state_symbol] if state_symbol else [],
            configured_symbols=[_clean(s) for s in config.symbols_to_monitor],
        )
    finally:
        await api_client.close()

    logger.info("Boot reconciliation: %s", decision.reason)
    print(f"\n{Colors.CYAN}Boot reconciliation: {decision.reason}{Colors.RESET}")
    for venue, positions in decision.positions.items():
        if positions:
            for sym, qty in sorted(positions.items()):
                print(f"  {Colors.YELLOW}{venue:<8} {sym:<10} {qty:+.6f}{Colors.RESET}")
        else:
            print(f"  {Colors.GRAY}{venue:<8} flat{Colors.RESET}")

    def _halt_and_raise(reason: str, symbol: str, detail: str, extra: dict) -> None:
        write_halt(reason, symbol=symbol, venue="both",
                   residual_qty=float("nan"), extra=extra)
        print(f"\n{Colors.RED}{Colors.BOLD}{reason.upper()}: {detail}{Colors.RESET}")
        print(f"  {Colors.YELLOW}A halt sentinel has been written. Resolve this on the "
              f"venues, then remove halt.json to resume.{Colors.RESET}\n")
        raise HaltedError(f"{reason}: {detail}")

    # --- unhedged exposure is not something to trade around ---------------
    one_legged = [c for c in decision.conflicts if c.get("kind") == "one_legged"]
    if one_legged:
        _halt_and_raise(
            "one-legged position at boot",
            one_legged[0]["symbol"],
            "; ".join(f"{c['symbol']}: {c['legs']}" for c in one_legged),
            {"conflicts": one_legged},
        )

    if decision.flat:
        if position:
            print(f"  {Colors.GRAY}State file referenced {state_symbol}, but neither "
                  f"venue holds it. Clearing.{Colors.RESET}")
            logger.info("Clearing stale position state for %s (both venues flat)", state_symbol)
        state_mgr.state["current_position"] = None
        state_mgr.save()
        return False

    # --- something is hedged on both venues -------------------------------
    aster_book = decision.positions.get("Aster", {})
    lighter_book = decision.positions.get("Lighter", {})
    hedged = sorted(set(aster_book) & set(lighter_book))

    if len(hedged) > 1:
        _halt_and_raise(
            "multiple hedged positions at boot", hedged[0],
            f"this bot manages one position at a time, found {hedged}",
            {"symbols": hedged},
        )

    symbol_clean = hedged[0]
    aster_qty = aster_book[symbol_clean]
    lighter_qty = lighter_book[symbol_clean]

    if aster_qty * lighter_qty > 0:
        _halt_and_raise(
            "position is not delta-neutral at boot", symbol_clean,
            f"both venues are on the SAME side (Aster {aster_qty:+.8g}, "
            f"Lighter {lighter_qty:+.8g}) - that is doubled directional exposure, "
            f"not a hedge",
            {"aster": aster_qty, "lighter": lighter_qty},
        )

    actual_size = (abs(aster_qty) + abs(lighter_qty)) / 2.0
    imbalance = abs(abs(aster_qty) - abs(lighter_qty))
    full_symbol = f"{symbol_clean}{config.quote}"

    if state_symbol == symbol_clean:
        # Known position: keep its original clock and economics, correct its size.
        saved_size = (position.get("metadata") or {}).get("size_base", 0.0)
        if abs(actual_size - saved_size) > max(1e-9, 0.001 * max(actual_size, 1e-9)):
            logger.warning(
                "Position size corrected from state %.10g to venue truth %.10g %s",
                saved_size, actual_size, symbol_clean,
            )
            position.setdefault("metadata", {})["size_base"] = actual_size
        print(f"{Colors.GREEN}Resuming tracked {full_symbol} hedge "
              f"({actual_size:.6f} {symbol_clean}){Colors.RESET}")
    else:
        # Hedged on both venues but absent from the state file. Adopting it is safer
        # than ignoring it: an unmanaged hedge has no stop-loss and never gets closed,
        # and the alternative - opening a second position alongside it - is exactly
        # the failure this reconciliation exists to prevent. The hold clock restarts
        # from now, because the real entry time is unknowable from here.
        logger.warning("Adopting untracked hedged position in %s", full_symbol)
        print(f"{Colors.YELLOW}Adopting untracked hedged position in {full_symbol}. "
              f"Hold window restarts now.{Colors.RESET}")
        position = {
            "symbol": full_symbol,
            "long_exchange": "Lighter" if lighter_qty > 0 else "Aster",
            "short_exchange": "Aster" if lighter_qty > 0 else "Lighter",
            "leverage": config.leverage,
            "opened_at": utc_now_iso(),
            "target_close_at": to_iso_z(utc_now() + timedelta(hours=config.hold_duration_hours)),
            "metadata": {"size_base": actual_size, "avg_mid": 0.0},
            "expected_net_apr": 0.0,
            "actual_notional": 0.0,
            "requested_notional": config.notional_per_position,
            "notional_was_adjusted": False,
            "capital_at_open": None,
            "adopted_at_boot": True,
        }
        state_mgr.state["current_position"] = position

    if imbalance > 0:
        logger.warning(
            "Legs are imbalanced by %.10g %s (Aster %.10g vs Lighter %.10g); the "
            "difference is unhedged delta.",
            imbalance, symbol_clean, abs(aster_qty), abs(lighter_qty),
        )
        print(f"  {Colors.YELLOW}Legs imbalanced by {imbalance:.8g} {symbol_clean} "
              f"- that difference is unhedged.{Colors.RESET}")

    try:
        remaining = (from_iso_z(position["target_close_at"]) - utc_now()).total_seconds() / 3600
        print(f"  {Colors.BLUE}Time remaining: {remaining:.2f} hours{Colors.RESET}\n")
    except Exception:                                           # noqa: BLE001
        logger.warning("Position has no usable target_close_at; closing on next check.")
        position["target_close_at"] = utc_now_iso()

    state_mgr.save()
    return True


# ==================== Funding Rate Display ====================

async def fetch_and_display_funding_rates(env: dict, aster: AsterApiManager, config: BotConfig, current_symbol: Optional[str] = None):
    """
    Fetch current funding rates and display opportunity table.

    Args:
        env: Environment variables
        aster: Aster API manager instance
        config: Bot configuration
        current_symbol: Currently held symbol (will be highlighted in table)
    """
    logger.info("Fetching current funding rates...")

    async def fetch_with_timeout(symbol: str, delay: float = 0.0, timeout: float = 30.0):
        """Fetch funding with timeout."""
        if delay > 0:
            await asyncio.sleep(delay)

        try:
            result = await asyncio.wait_for(
                fetch_symbol_funding(symbol + config.quote, env, aster, check_volume=False, max_spread_pct=config.max_spread_pct),
                timeout=timeout
            )
            return result
        except asyncio.TimeoutError:
            return {"symbol": symbol + config.quote, "available": False, "error": "timeout"}
        except Exception as e:
            return {"symbol": symbol + config.quote, "available": False, "error": str(e)[:50]}

    # Stagger requests to avoid rate limits
    stagger_delay = 2.5
    results = await asyncio.gather(*[
        fetch_with_timeout(symbol, delay=idx * stagger_delay)
        for idx, symbol in enumerate(config.symbols_to_monitor)
    ], return_exceptions=True)

    available = [r for r in results if isinstance(r, dict) and r.get("available", False)]
    unavailable = [r for r in results if isinstance(r, dict) and not r.get("available", False)]

    if available:
        available.sort(key=lambda x: x["net_apr"], reverse=True)

    display_funding_table(available, unavailable, current_symbol=current_symbol, limit=10)


# ==================== Main Bot Logic ====================

async def halted_idle(reason: object) -> None:
    """Stay alive, trade nothing, and keep saying why.

    Exiting here would be worse than useless: docker-compose runs this bot with
    `restart: unless-stopped`, so the container would come straight back, hit the same
    sentinel and exit again - a crash loop that buries the one message explaining what
    needs a human. Staying up idle keeps the process visible in `docker ps` and the
    reason legible in `docker logs`.
    """
    logger.critical("HALTED: %s", reason)
    print(f"\n{Colors.RED}{Colors.BOLD}{'=' * 80}\nBOT HALTED - NOT TRADING\n{'=' * 80}{Colors.RESET}")
    print(f"  {Colors.YELLOW}{reason}{Colors.RESET}")
    print(f"  {Colors.YELLOW}Check BOTH venues, then delete {HALT_FILENAME} to "
          f"resume.{Colors.RESET}\n")
    while True:
        logger.critical(
            "Bot is HALTED and will not trade. Check both venues, then remove %s to "
            "resume. Reason: %s", HALT_FILENAME, reason,
        )
        await asyncio.sleep(300)


async def main_loop(state_mgr: StateManager, env: dict, config: BotConfig, config_file: str):
    """Main bot loop."""

    # Initialize Aster API manager
    aster = AsterApiManager(
        api_user=env["ASTER_API_USER"],
        api_signer=env["ASTER_API_SIGNER"],
        api_private_key=env["ASTER_API_PRIVATE_KEY"],
        apiv1_public=env["ASTER_APIV1_PUBLIC"],
        apiv1_private=env["ASTER_APIV1_PRIVATE"]
    )

    # Reconcile against the venues on EVERY boot, not only when the state file happens
    # to mention a position. A live hedge that is missing from the state file is
    # exactly the case that must not be missed - and it is unreachable from a check
    # gated on the state file already knowing about it.
    #
    # boot_reconcile calls assert_not_halted() internally, so this is also the point
    # where a halt sentinel from a previous failure stops the bot.
    logger.info("Reconciling positions against both venues...")
    try:
        holding = None
        for attempt in range(1, 6):
            try:
                holding = await reconcile_positions_at_boot(state_mgr, env, aster, config)
                break
            except BootReconcileError as exc:
                # A venue listing failed. That is NOT "flat" - retry rather than
                # guessing, and never fall through to trading on an unknown state.
                delay = min(60.0, 5.0 * attempt)
                logger.error("Boot reconciliation attempt %d/5 failed: %s. Retrying in %.0fs",
                             attempt, exc, delay)
                await asyncio.sleep(delay)
        if holding is None:
            raise HaltedError(
                "Could not establish position state from the venues after 5 attempts. "
                "Refusing to trade against an unknown account state."
            )
        state_mgr.set_state(BotState.HOLDING if holding else BotState.IDLE)
    except HaltedError as exc:
        await aster.close()
        await halted_idle(exc)
        return

    # Fetch initial capital status
    logger.info("Fetching initial capital status from exchanges...")
    capital_ok = await update_capital_status(env, aster, state_mgr, config)
    if not capital_ok:
        logger.error("Failed to fetch initial capital status. Continuing anyway...")

    # Display initial funding rate table at startup
    logger.info("Fetching initial funding rates...")
    try:
        await fetch_and_display_funding_rates(env, aster, config, current_symbol=None)
    except Exception as e:
        logger.error(f"Failed to fetch initial funding rates: {e}")
        logger.info("Continuing to main loop...")

    try:
        while True:
            current_state = state_mgr.get_state()

            if current_state == BotState.IDLE or current_state == BotState.WAITING:
                # Reload configuration before starting new cycle
                logger.info("Starting new cycle - reloading configuration...")
                config.reload(config_file)

                # Analyze and open position
                logger.info("Analyzing funding rates...")

                # Fetch funding rates
                logger.info(f"Analyzing funding rates for {len(config.symbols_to_monitor)} symbols...")
                state_mgr.set_state(BotState.ANALYZING)

                async def fetch_with_timeout(symbol: str, delay: float = 0.0, timeout: float = 30.0):
                    """Fetch funding with timeout. Reduced from 90s to 30s to avoid long blocks."""
                    logger.debug(f"fetch_with_timeout: Starting {symbol} with delay={delay}s, timeout={timeout}s")
                    if delay > 0:
                        logger.debug(f"fetch_with_timeout: Waiting {delay}s before fetching {symbol}")
                        await asyncio.sleep(delay)

                    logger.debug(f"fetch_with_timeout: Now fetching funding for {symbol}")
                    try:
                        result = await asyncio.wait_for(
                            fetch_symbol_funding(symbol + config.quote, env, aster, check_volume=False, max_spread_pct=config.max_spread_pct),
                            timeout=timeout
                        )
                        logger.debug(f"fetch_with_timeout: Success for {symbol}: available={result.get('available')}")
                        return result
                    except asyncio.TimeoutError:
                        logger.warning(f"{symbol}: Funding rate fetch timed out after {timeout}s")
                        return {"symbol": symbol + config.quote, "available": False, "error": "timeout"}
                    except Exception as e:
                        logger.warning(f"{symbol}: Error fetching funding - {str(e)[:100]}")
                        logger.debug(f"fetch_with_timeout: Exception details for {symbol}", exc_info=True)
                        return {"symbol": symbol + config.quote, "available": False, "error": str(e)[:50]}

                # Stagger requests to avoid overwhelming the APIs and reduce rate limit errors
                stagger_delay = 2.5  # Increased from 1.0 to give more breathing room
                logger.info(f"Staggering {len(config.symbols_to_monitor)} symbol requests with {stagger_delay}s delay")
                results = await asyncio.gather(*[
                    fetch_with_timeout(symbol, delay=idx * stagger_delay)
                    for idx, symbol in enumerate(config.symbols_to_monitor)
                ], return_exceptions=True)
                logger.info(f"Completed gathering funding data for {len(results)} symbols")

                available = [r for r in results if isinstance(r, dict) and r.get("available", False)]
                unavailable = [r for r in results if isinstance(r, dict) and not r.get("available", False)]

                if not available:
                    logger.error("No symbols available on both exchanges!")
                    state_mgr.set_state(BotState.WAITING)
                    await asyncio.sleep(60)
                    continue

                available.sort(key=lambda x: x["net_apr"], reverse=True)

                # Size FIRST, then judge. The entry decision depends on the notional
                # actually being traded (fees and funding both scale with it), so the
                # capital check has to come before the gate rather than after it.
                logger.info("Updating capital status before evaluating candidates...")
                capital_ok = await update_capital_status(env, aster, state_mgr, config)

                if not capital_ok:
                    logger.error("Failed to fetch current capital status. Skipping this cycle...")
                    state_mgr.set_state(BotState.WAITING)
                    await asyncio.sleep(60)
                    continue

                affordable_notional, was_adjusted = calculate_affordable_notional(state_mgr, config)

                if affordable_notional <= 0:
                    logger.error(
                        f"{Colors.RED}{Colors.BOLD}⚠️  INSUFFICIENT CAPITAL{Colors.RESET}\n"
                        f"  No available capital to open positions. Please deposit funds.\n"
                        f"  Aster available: ${state_mgr.state['capital_status']['aster_available']:,.2f}\n"
                        f"  Lighter available: ${state_mgr.state['capital_status']['lighter_available']:,.2f}"
                    )
                    state_mgr.set_state(BotState.WAITING)
                    await asyncio.sleep(300)  # Wait 5 minutes before checking again
                    continue

                # Fee-aware entry gate.
                #
                # This replaces `net_apr >= min_net_apr_threshold`, where "net" only
                # ever meant net of the OTHER LEG'S FUNDING - never net of trading
                # cost. The bot recorded three "successful" cycles at $0.00 PnL while
                # paying a full round trip on each, because nothing anywhere compared
                # the spread it was capturing against the cost of capturing it.
                #
                # evaluate_entry discounts the observed rate (forward funding decays,
                # the spot rate at entry is not the mean realised over a 24h hold),
                # charges the round trip, and requires a margin over break-even. The
                # configured min_net_apr_threshold is kept as an additional floor.
                cost_model = build_cost_model(config)
                break_even = break_even_apr_pct(cost_model, config.hold_duration_hours)
                logger.info(
                    "Entry gate: break-even %.2f%% APR at a %.1fh hold "
                    "(round trip %.4f%%), floor %.2f%%, sizing $%,.2f",
                    break_even, config.hold_duration_hours, cost_model.roundtrip_pct(),
                    config.min_net_apr_threshold, affordable_notional,
                )

                for row in available:
                    row["decision"] = evaluate_entry(
                        symbol=row["symbol"],
                        gross_net_apr_pct=row["net_apr"],
                        notional_usd=affordable_notional,
                        hold_hours=config.hold_duration_hours,
                        cost=cost_model,
                    )

                display_funding_table(available, unavailable, current_symbol=None, limit=10)

                candidates = [
                    r for r in available
                    if r["decision"].accept and r["net_apr"] >= config.min_net_apr_threshold
                ]
                # Rank by expected dollars, not by headline APR: two rows with the same
                # gross spread are not equally good once their break-evens differ.
                candidates.sort(key=lambda r: r["decision"].expected_net_usd, reverse=True)

                if not candidates:
                    best_row = available[0]
                    logger.info(
                        "No candidate clears the fee-aware gate. Best was %s at %.2f%% "
                        "gross (%s). Waiting...",
                        best_row["symbol"], best_row["net_apr"], best_row["decision"].reason,
                    )
                    state_mgr.set_state(BotState.WAITING)
                    await asyncio.sleep(60)
                    continue

                # Refuse to open on top of an unresolved failure. A halt sentinel means
                # a previous unwind left real exposure behind; `restart: unless-stopped`
                # would otherwise bring the container straight back here and stack a
                # fresh position on top of it.
                assert_not_halted()

                best = candidates[0]
                decision = best["decision"]
                logger.info(
                    f"\n{Colors.CYAN}Opening {best['symbol']}: gross {best['net_apr']:.2f}% -> "
                    f"expected {decision.expected_apr_pct:.2f}% vs break-even "
                    f"{decision.break_even_apr_pct:.2f}% ({decision.margin_ratio:.2f}x), "
                    f"expected net ${decision.expected_net_usd:+.2f}{Colors.RESET}"
                )

                state_mgr.set_state(BotState.OPENING)

                try:
                    metadata = await open_delta_neutral_position(
                        env,
                        aster,
                        best['symbol'],
                        best['long_exch'],
                        best['short_exch'],
                        config.leverage,
                        affordable_notional,  # Use affordable notional instead of config value
                        cross_ticks=100
                    )

                    # Store position metadata
                    state_mgr.state["current_position"] = {
                        "symbol": best['symbol'],
                        "long_exchange": best['long_exch'],
                        "short_exchange": best['short_exch'],
                        "leverage": config.leverage,
                        "opened_at": utc_now_iso(),
                        "target_close_at": to_iso_z(utc_now() + timedelta(hours=config.hold_duration_hours)),
                        "metadata": metadata,
                        "expected_net_apr": best['net_apr'],
                        # The economics this trade was accepted on, recorded so a
                        # post-mortem can compare what was expected against the
                        # realised PnL rather than re-deriving it from a changed config.
                        "entry_economics": {
                            "gross_apr_pct": decision.gross_apr_pct,
                            "expected_apr_pct": decision.expected_apr_pct,
                            "break_even_apr_pct": decision.break_even_apr_pct,
                            "margin_ratio": decision.margin_ratio,
                            "expected_funding_usd": decision.expected_funding_usd,
                            "expected_cost_usd": decision.expected_cost_usd,
                            "expected_net_usd": decision.expected_net_usd,
                            "hold_hours": config.hold_duration_hours,
                            "roundtrip_pct": cost_model.roundtrip_pct(),
                            "aster_interval_hours": best.get("aster_interval_hours"),
                            "aster_interval_source": best.get("aster_interval_source"),
                        },
                        "actual_notional": affordable_notional,
                        "requested_notional": config.notional_per_position,
                        "notional_was_adjusted": was_adjusted,
                        # Baseline for measuring realised PnL at close. Total capital
                        # after minus before captures funding, fees and slippage
                        # together, with no cost model that could be wrong.
                        "capital_at_open": state_mgr.state.get("capital_status", {}).get("total_capital"),
                    }
                    state_mgr.set_state(BotState.HOLDING)
                    logger.info("Position opened successfully, now holding...")

                except Exception as e:
                    logger.error(f"Failed to open position: {e}", exc_info=True)
                    state_mgr.set_state(BotState.ERROR)
                    state_mgr.state["cumulative_stats"]["last_error"] = str(e)
                    state_mgr.state["cumulative_stats"]["last_error_at"] = utc_now_iso()
                    state_mgr.save()
                    await asyncio.sleep(300)
                    continue

            elif current_state == BotState.HOLDING:
                # Monitor position and close when time is up
                position = state_mgr.state.get("current_position")
                if not position:
                    logger.warning("No position found in HOLDING state, returning to IDLE")
                    state_mgr.set_state(BotState.IDLE)
                    continue

                target_close = from_iso_z(position["target_close_at"])
                now = utc_now()

                if now >= target_close:
                    logger.info("Hold duration complete, closing position...")
                    state_mgr.set_state(BotState.CLOSING)

                    try:
                        await close_delta_neutral_position(
                            env,
                            aster,
                            position["symbol"],
                            cross_ticks=100
                        )

                        # Record the cycle WITH its realised economics.
                        #
                        # cumulative_stats has carried total_realized_pnl,
                        # total_fees_paid, best/worst_cycle_pnl etc. since day one -
                        # every one of them initialised to 0.0 and NEVER written. So
                        # the bot reported 3 "successful" cycles at $0.00 PnL while
                        # actually paying a round trip on each. A bot that cannot see
                        # its own losses cannot be told it is losing.
                        await record_cycle_result(
                            state_mgr, env, aster, config, position, "success",
                        )

                        state_mgr.state["current_position"] = None
                        state_mgr.set_state(BotState.WAITING)

                        logger.info(f"Cycle complete! Waiting {config.wait_between_cycles_minutes} minutes before next cycle...")
                        await asyncio.sleep(config.wait_between_cycles_minutes * 60)
                        state_mgr.set_state(BotState.IDLE)

                    except Exception as e:
                        logger.error(f"Failed to close position: {e}", exc_info=True)
                        state_mgr.set_state(BotState.ERROR)
                        state_mgr.state["cumulative_stats"]["last_error"] = str(e)
                        state_mgr.state["cumulative_stats"]["last_error_at"] = utc_now_iso()
                        state_mgr.save()
                        await asyncio.sleep(300)
                        continue

                else:
                    # Still holding, check periodically
                    time_remaining = (target_close - now).total_seconds() / 3600

                    # Calculate stop-loss percentage from leverage
                    leverage = position.get('leverage', config.leverage)
                    stop_loss_pct = calculate_stop_loss_percentage(leverage)

                    # Calculate position value for percentage calculation and position size string
                    metadata = position.get('metadata', {})
                    size_base = metadata.get('size_base', 0.0)
                    avg_mid = metadata.get('avg_mid', 0.0)
                    position_value = size_base * avg_mid if size_base and avg_mid else config.notional_per_position

                    # Format position size string
                    base_asset = position['symbol'].replace('USDT', '')
                    size_str = f"{Colors.MAGENTA}{size_base:.4f} {base_asset} / ${position_value:,.2f}{Colors.RESET}" if size_base and position_value else ""

                    # Fetch current PnL from both exchanges
                    try:
                        aster_pnl, lighter_pnl = await get_position_pnls(env, aster, position['symbol'])

                        # Calculate worst PnL (most negative) and determine exchange
                        worst_pnl = None
                        worst_exchange = None
                        total_pnl = None

                        if aster_pnl is not None and lighter_pnl is not None:
                            if aster_pnl <= lighter_pnl:
                                worst_pnl = aster_pnl
                                worst_exchange = "Aster"
                            else:
                                worst_pnl = lighter_pnl
                                worst_exchange = "Lighter"
                            total_pnl = aster_pnl + lighter_pnl
                        elif aster_pnl is not None:
                            worst_pnl = aster_pnl
                            worst_exchange = "Aster"
                            total_pnl = aster_pnl
                        elif lighter_pnl is not None:
                            worst_pnl = lighter_pnl
                            worst_exchange = "Lighter"
                            total_pnl = lighter_pnl

                        # Format worst PnL message with percentage and exchange
                        if worst_pnl is not None and worst_exchange and position_value > 0:
                            pnl_pct = (worst_pnl / position_value) * 100

                            # Check if stop-loss threshold is breached
                            if config.enable_stop_loss and abs(pnl_pct) >= stop_loss_pct:
                                logger.warning(
                                    f"{Colors.RED}{Colors.BOLD}⚠️  STOP-LOSS TRIGGERED!{Colors.RESET} "
                                    f"Worst PnL: {pnl_pct:.1f}% >= {stop_loss_pct:.2f}% threshold on {worst_exchange}"
                                )
                                logger.info("Closing position early due to stop-loss...")
                                state_mgr.set_state(BotState.CLOSING)

                                try:
                                    await close_delta_neutral_position(env, aster, position["symbol"], cross_ticks=100)

                                    # Measure realised PnL here too. A stop-loss cycle
                                    # is precisely the kind this bot most needs to
                                    # count: excluding it from total_realized_pnl was
                                    # what let the "cumulative PnL is negative" check
                                    # look healthy while the losing cycles piled up.
                                    await record_cycle_result(
                                        state_mgr, env, aster, config, position, "stop-loss",
                                        extra={
                                            "pnl_at_close": worst_pnl,
                                            "pnl_pct_at_close": pnl_pct,
                                            "worst_exchange": worst_exchange,
                                        },
                                    )
                                    state_mgr.state["current_position"] = None
                                    state_mgr.save()

                                    logger.info(f"Stop-loss executed successfully. Waiting {config.wait_between_cycles_minutes} minutes before next cycle...")
                                    await asyncio.sleep(config.wait_between_cycles_minutes * 60)
                                    state_mgr.set_state(BotState.IDLE)
                                    continue  # Skip to next loop iteration

                                except Exception as e:
                                    logger.error(f"Failed to execute stop-loss: {e}", exc_info=True)
                                    state_mgr.set_state(BotState.ERROR)
                                    state_mgr.state["cumulative_stats"]["last_error"] = f"Stop-loss execution failed: {str(e)}"
                                    state_mgr.state["cumulative_stats"]["last_error_at"] = utc_now_iso()
                                    state_mgr.save()
                                    await asyncio.sleep(300)
                                    continue  # Skip to next loop iteration

                            pnl_color = Colors.GREEN if worst_pnl >= 0 else Colors.RED
                            pnl_str = f"{pnl_color}${worst_pnl:+.2f} ({pnl_pct:+.1f}% on {Colors.CYAN}{worst_exchange}{pnl_color}){Colors.RESET}"

                            # Add total PnL if available
                            total_pnl_str = ""
                            if total_pnl is not None:
                                total_pnl_pct = (total_pnl / position_value) * 100
                                total_color = Colors.GREEN if total_pnl >= 0 else Colors.RED
                                total_pnl_str = f" | Total PnL: {total_color}${total_pnl:+.2f} ({total_pnl_pct:+.1f}%){Colors.RESET}"

                            logger.info(
                                f"Holding position for {Colors.CYAN}{Colors.BOLD}{position['symbol']}{Colors.RESET} "
                                f"({size_str}) - "
                                f"{Colors.BLUE}{Colors.BOLD}{time_remaining:.2f} hours{Colors.RESET} remaining | "
                                f"Stop-loss: {Colors.YELLOW}{stop_loss_pct:.2f}%{Colors.RESET} | "
                                f"Worst PnL: {pnl_str}{total_pnl_str}"
                            )
                        elif worst_pnl is not None:
                            # Fallback if position_value couldn't be calculated
                            pnl_color = Colors.GREEN if worst_pnl >= 0 else Colors.RED
                            pnl_str = f"{pnl_color}${worst_pnl:+.2f} (on {Colors.CYAN}{worst_exchange}{pnl_color}){Colors.RESET}" if worst_exchange else f"{pnl_color}${worst_pnl:+.2f}{Colors.RESET}"

                            # Add total PnL if available
                            total_pnl_str = ""
                            if total_pnl is not None:
                                total_color = Colors.GREEN if total_pnl >= 0 else Colors.RED
                                total_pnl_str = f" | Total PnL: {total_color}${total_pnl:+.2f}{Colors.RESET}"

                            logger.info(
                                f"Holding position for {Colors.CYAN}{Colors.BOLD}{position['symbol']}{Colors.RESET} "
                                f"({size_str}) - "
                                f"{Colors.BLUE}{Colors.BOLD}{time_remaining:.2f} hours{Colors.RESET} remaining | "
                                f"Stop-loss: {Colors.YELLOW}{stop_loss_pct:.2f}%{Colors.RESET} | "
                                f"Worst PnL: {pnl_str}{total_pnl_str}"
                            )
                        else:
                            logger.info(
                                f"Holding position for {Colors.CYAN}{Colors.BOLD}{position['symbol']}{Colors.RESET} "
                                f"({size_str}) - "
                                f"{Colors.BLUE}{Colors.BOLD}{time_remaining:.2f} hours{Colors.RESET} remaining | "
                                f"Stop-loss: {Colors.YELLOW}{stop_loss_pct:.2f}%{Colors.RESET} | "
                                f"Worst PnL: {Colors.GRAY}N/A{Colors.RESET}"
                            )
                    except Exception as e:
                        logger.debug(f"Failed to fetch PnL for holding message: {e}")
                        logger.info(
                            f"Holding position for {Colors.CYAN}{Colors.BOLD}{position['symbol']}{Colors.RESET} "
                            f"({size_str}) - "
                            f"{Colors.BLUE}{Colors.BOLD}{time_remaining:.2f} hours{Colors.RESET} remaining | "
                            f"Stop-loss: {Colors.YELLOW}{stop_loss_pct:.2f}%{Colors.RESET}"
                        )

                    # Check if we should refresh the funding rate table
                    last_table_refresh = position.get("last_table_refresh")
                    should_refresh = False

                    if last_table_refresh is None:
                        should_refresh = True
                    else:
                        try:
                            last_refresh_time = from_iso_z(last_table_refresh)
                            time_since_refresh = (now - last_refresh_time).total_seconds() / 60
                            if time_since_refresh >= config.funding_table_refresh_minutes:
                                should_refresh = True
                        except Exception:
                            should_refresh = True

                    if should_refresh:
                        logger.info(f"Refreshing funding rate table (every {config.funding_table_refresh_minutes} minutes)...")
                        try:
                            await fetch_and_display_funding_rates(env, aster, config, current_symbol=position["symbol"])
                            position["last_table_refresh"] = utc_now_iso()
                            state_mgr.save()
                        except Exception as e:
                            logger.warning(f"Failed to refresh funding table: {e}")

                    await asyncio.sleep(config.check_interval_seconds)

            elif current_state == BotState.ERROR:
                logger.warning("Bot in ERROR state, attempting recovery...")
                await asyncio.sleep(60)
                state_mgr.set_state(BotState.IDLE)

            else:
                logger.warning(f"Unknown state: {current_state}, resetting to IDLE")
                state_mgr.set_state(BotState.IDLE)

    except HaltedError as exc:
        # Raised by assert_not_halted() before an open, or by a close that could not
        # flatten. Idle loudly instead of exiting - see halted_idle().
        state_mgr.state["cumulative_stats"]["last_error"] = str(exc)
        state_mgr.state["cumulative_stats"]["last_error_at"] = utc_now_iso()
        state_mgr.save()
        await halted_idle(exc)
    finally:
        await aster.close()


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(description="Lighter-Aster Delta Neutral Hedge Bot")
    parser.add_argument("--state-file", default="bot_state.json", help="State file path")
    parser.add_argument("--config", default="config.json", help="Config file path")
    args = parser.parse_args()

    # Load environment and config
    env = load_env()
    try:
        config = BotConfig.load_from_file(args.config)
    except ConfigError as exc:
        # Fatal on purpose. Starting on defaults means trading a different strategy
        # than the one that was configured, at 3x leverage and a 5% APR gate.
        logger.critical("%s", exc)
        print(f"\n{Colors.RED}{Colors.BOLD}CONFIGURATION ERROR{Colors.RESET}\n  {exc}\n")
        print(f"  {Colors.YELLOW}Fix {args.config} and start again. The bot will not "
              f"run on built-in defaults.{Colors.RESET}\n")
        sys.exit(2)

    # Initialize state manager
    state_mgr = StateManager(args.state_file)
    state_mgr.load()
    state_mgr.set_config(config)

    logger.info("Starting Lighter-Aster Delta Neutral Hedge Bot...")
    logger.info(f"Monitoring {len(config.symbols_to_monitor)} symbols")
    logger.info(f"Leverage: {config.leverage}x, Notional: ${config.notional_per_position}")
    logger.info(f"Hold duration: {config.hold_duration_hours} hours")

    # Run main loop
    try:
        asyncio.run(main_loop(state_mgr, env, config, args.config))
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
        state_mgr.set_state(BotState.SHUTDOWN)
        state_mgr.save()
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        state_mgr.set_state(BotState.ERROR)
        state_mgr.state["cumulative_stats"]["last_error"] = str(e)
        state_mgr.state["cumulative_stats"]["last_error_at"] = utc_now_iso()
        state_mgr.save()
        sys.exit(1)


if __name__ == "__main__":
    main()
