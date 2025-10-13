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
from dataclasses import dataclass, asdict

from dotenv import load_dotenv
import lighter

import lighter_client
from aster_api_manager import AsterApiManager

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

# ==================== Logging Setup ====================

os.makedirs('logs', exist_ok=True)

# File handler - DEBUG level
file_handler = logging.FileHandler('logs/lighter_aster_hedge.log', mode='w')
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
    enable_pnl_tracking: bool = True
    enable_health_monitoring: bool = True
    funding_table_refresh_minutes: float = 5.0

    @staticmethod
    def load_from_file(config_file: str) -> 'BotConfig':
        """Load configuration from JSON file."""
        try:
            with open(config_file, 'r') as f:
                data = json.load(f)

            # Remove comment fields
            data = {k: v for k, v in data.items() if not k.startswith('comment')}

            # Provide defaults
            defaults = {
                'symbols_to_monitor': DEFAULT_SYMBOLS,
                'quote': 'USDT',
                'leverage': 3,
                'notional_per_position': 100.0,
                'hold_duration_hours': 8.0,
                'wait_between_cycles_minutes': 5.0,
                'check_interval_seconds': 60,
                'min_net_apr_threshold': 5.0,
                'max_spread_pct': 0.15,
                'enable_stop_loss': True,
                'enable_pnl_tracking': True,
                'enable_health_monitoring': True,
                'funding_table_refresh_minutes': 5.0
            }

            for key, default_value in defaults.items():
                if key not in data:
                    data[key] = default_value
                    logger.info(f"Using default value for {key}: {default_value}")

            return BotConfig(**data)
        except FileNotFoundError:
            logger.warning(f"Config file {config_file} not found, using defaults")
            return BotConfig(symbols_to_monitor=DEFAULT_SYMBOLS)
        except Exception as e:
            logger.error(f"Error loading config: {e}")
            return BotConfig(symbols_to_monitor=DEFAULT_SYMBOLS)


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


def _calculate_apr(rate: float, periods_per_day: int) -> float:
    """Convert a per-period funding rate (decimal form) into annualized percentage."""
    return rate * periods_per_day * 365 * 100.0


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
        logger.debug(f"Failed to fetch Aster PnL for {symbol}: {e}")

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
        logger.debug(f"Failed to fetch Lighter PnL for {symbol}: {e}")
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

    async def fetch_aster_rate() -> Optional[float]:
        logger.debug(f"fetch_aster_rate: Starting for {symbol}")
        try:
            history = await aster.get_funding_rate_history(symbol, limit=1)
            if history and len(history) > 0:
                rate = float(history[0].get('fundingRate', 0))
                logger.debug(f"fetch_aster_rate: Got rate {rate} for {symbol}")
                return rate
            logger.warning(f"fetch_aster_rate: No funding history for {symbol}")
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
    aster_rate_decimal, lighter_rate_decimal, spread_data = await asyncio.gather(
        fetch_aster_rate(),
        fetch_lighter_rate(),
        fetch_symbol_spread(symbol, env, aster)
    )
    spread_pct, aster_mid, lighter_mid = spread_data
    logger.debug(f"fetch_symbol_funding: Received aster_rate={aster_rate_decimal}, lighter_rate={lighter_rate_decimal}, spread={spread_pct} for {symbol}")

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

    # Aster funding happens every 4 hours (6 times per day)
    # Lighter funding happens every 8 hours (3 times per day)
    aster_apr = _calculate_apr(aster_rate_decimal, 6)
    lighter_apr = _calculate_apr(lighter_rate_decimal, 3)

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
        "lighter_rate": lighter_rate_decimal * 100 if lighter_rate_decimal is not None else None,
        "lighter_apr": lighter_apr,
        "long_exch": long_exch,
        "short_exch": short_exch,
        "net_apr": net_apr,
        "spread_pct": spread_pct,
        "aster_mid": aster_mid,
        "lighter_mid": lighter_mid,
    }
    logger.debug(f"fetch_symbol_funding: Success for {symbol}: net_apr={net_apr:.2f}%, long={long_exch}, short={short_exch}")
    return result


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

    # Configure leverage
    await configure_leverage(leverage, env, aster, signer, symbol, l_market_id, verify=True)

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

    tasks = []
    leg_names: List[str] = []

    # Place orders on both exchanges
    if long_leg == "lighter":
        ref_price = lighter_ask if lighter_ask else lighter_bid
        if ref_price is None:
            raise RuntimeError("Lighter: No reference price available for long leg.")
        tasks.append(
            lighter_client.lighter_place_aggressive_order(
                signer,
                l_market_id,
                l_price_tick,
                l_amount_tick,
                "buy",
                size_base,
                ref_price,
                cross_ticks=cross_ticks,
            )
        )
        leg_names.append("Lighter (LONG)")
    elif long_leg == "aster":
        tasks.append(
            aster.place_perp_market_order(symbol, str(size_base), 'BUY')
        )
        leg_names.append("Aster (LONG)")
    else:
        raise RuntimeError(f"Unsupported long exchange: {long_exchange}")

    if short_leg == "lighter":
        ref_price = lighter_bid if lighter_bid else lighter_ask
        if ref_price is None:
            raise RuntimeError("Lighter: No reference price available for short leg.")
        tasks.append(
            lighter_client.lighter_place_aggressive_order(
                signer,
                l_market_id,
                l_price_tick,
                l_amount_tick,
                "sell",
                size_base,
                ref_price,
                cross_ticks=cross_ticks,
            )
        )
        leg_names.append("Lighter (SHORT)")
    elif short_leg == "aster":
        tasks.append(
            aster.place_perp_market_order(symbol, str(size_base), 'SELL')
        )
        leg_names.append("Aster (SHORT)")
    else:
        await signer.close()
        await api_client.close()
        raise RuntimeError(f"Unsupported short exchange: {short_exchange}")

    results = await asyncio.gather(*tasks, return_exceptions=True)
    errors = [(idx, res) for idx, res in enumerate(results) if isinstance(res, Exception)]

    if errors:
        print(f"\n{Colors.RED}{Colors.BOLD}❌ ERROR: One or more open orders failed!{Colors.RESET}")
        for idx, err_obj in errors:
            print(f"   {Colors.RED}- {leg_names[idx]}: {err_obj}{Colors.RESET}")
        successful = [leg_names[i] for i in range(len(results)) if not isinstance(results[i], Exception)]
        if successful:
            print(f"\n{Colors.YELLOW}{Colors.BOLD}⚠️  CRITICAL: Partial fill detected!{Colors.RESET}")
            print(f"   {Colors.YELLOW}Successfully opened on: {', '.join(successful)}{Colors.RESET}")
        await signer.close()
        await api_client.close()
        raise RuntimeError("Delta-neutral order placement failed on at least one exchange.")

    print(f"{Colors.GREEN}✓ Both orders placed successfully{Colors.RESET}")
    logger.info("Opened hedge: size_base=%s %s. Legs placed concurrently.", size_base, symbol_clean)

    await asyncio.sleep(2)  # allow exchanges time to process

    # Verify positions
    print(f"\n{Colors.CYAN}Verifying positions...{Colors.RESET}")

    try:
        # Verify Aster position
        aster_account = await aster.get_perp_account_info()
        aster_positions = aster_account.get('positions', [])
        aster_size = 0.0
        for pos in aster_positions:
            if pos.get('symbol') == symbol:
                aster_size = float(pos.get('positionAmt', 0))
                break
        aster_color = Colors.GREEN if abs(aster_size) > 0.0001 else Colors.YELLOW
        print(f"  {aster_color}Aster position:   {aster_size:+.6f} {symbol_clean}{Colors.RESET}")
    except Exception as e:
        logger.warning(f"Could not verify Aster position: {e}")
        print(f"  {Colors.YELLOW}Aster position:   Unable to verify{Colors.RESET}")

    try:
        # Verify Lighter position
        account_api = lighter.AccountApi(api_client)
        lighter_size = await lighter_client.get_lighter_open_size(account_api, env["ACCOUNT_INDEX"], l_market_id, symbol=symbol_clean)
        lighter_color = Colors.GREEN if abs(lighter_size) > 0.0001 else Colors.YELLOW
        print(f"  {lighter_color}Lighter position: {lighter_size:+.6f} {symbol_clean}{Colors.RESET}")
    except Exception as e:
        logger.warning(f"Could not verify Lighter position: {e}")
        print(f"  {Colors.YELLOW}Lighter position: Unable to verify{Colors.RESET}")

    print(
        f"\n{Colors.GREEN}{Colors.BOLD}✓ Hedge opened successfully!{Colors.RESET}\n"
        f"  {Colors.CYAN}Total exposure: {Colors.BOLD}{size_base:.6f} {symbol_clean}{Colors.RESET}{Colors.CYAN} on each exchange{Colors.RESET}\n"
        f"  {Colors.CYAN}Delta-neutral: {Colors.BOLD}LONG {long_exchange.capitalize()}{Colors.RESET}{Colors.CYAN}, {Colors.BOLD}SHORT {short_exchange.capitalize()}{Colors.RESET}\n"
    )

    await signer.close()
    await api_client.close()

    return {
        "lighter_market_id": l_market_id,
        "lighter_price_tick": l_price_tick,
        "lighter_amount_tick": l_amount_tick,
        "aster_step_size": aster_step_size,
        "aster_bid": aster_bid,
        "aster_ask": aster_ask,
        "lighter_bid": lighter_bid,
        "lighter_ask": lighter_ask,
        "size_base": size_base,
        "avg_mid": avg_mid,
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
    lighter_bid, lighter_ask = await lighter_client.get_lighter_best_bid_ask(order_api, symbol_clean, l_market_id)

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

    lighter_size = await lighter_client.get_lighter_open_size(account_api, env["ACCOUNT_INDEX"], l_market_id)
    lighter_color = Colors.YELLOW if abs(lighter_size) > l_amount_tick else Colors.GRAY
    print(f"  {lighter_color}Lighter position: {lighter_size:+.6f} {symbol_clean}{Colors.RESET}")

    print(f"\n{Colors.CYAN}Closing positions on both exchanges...{Colors.RESET}")
    tasks = []

    # Close Lighter position
    if abs(lighter_size) > l_amount_tick:
        lighter_close_side = "sell" if lighter_size > 0 else "buy"
        ref_price = lighter_bid if lighter_close_side == "sell" else lighter_ask
        if ref_price:
            tasks.append(
                lighter_client.lighter_close_position(
                    signer,
                    l_market_id,
                    l_price_tick,
                    l_amount_tick,
                    lighter_close_side,
                    abs(lighter_size),
                    ref_price,
                    cross_ticks=cross_ticks,
                )
            )
        else:
            logger.warning("Lighter: No reference price available to close position.")
            print(f"  {Colors.YELLOW}Lighter: No reference price available, cannot send close order.{Colors.RESET}")
    else:
        print(f"  {Colors.GRAY}Lighter: Position already flat or below minimum tick.{Colors.RESET}")

    # Close Aster position
    if abs(aster_size) > 0.0001:
        aster_close_side = 'BUY' if aster_size < 0 else 'SELL'
        tasks.append(
            aster.close_perp_position(symbol, str(abs(aster_size)), aster_close_side)
        )

    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        errors = [res for res in results if isinstance(res, Exception)]
        if errors:
            print(f"\n{Colors.RED}{Colors.BOLD}❌ ERROR: One or more close orders failed!{Colors.RESET}")
            for err_obj in errors:
                print(f"   {Colors.RED}- {err_obj}{Colors.RESET}")
            print(f"\n{Colors.YELLOW}{Colors.BOLD}⚠️  WARNING: Please verify positions manually.{Colors.RESET}")
            await signer.close()
            await api_client.close()
            raise RuntimeError("Failed to close positions on one or more venues.")

    print(f"{Colors.GREEN}✓ Close orders sent to both exchanges{Colors.RESET}")

    await asyncio.sleep(2)

    print(f"\n{Colors.CYAN}Verifying closure...{Colors.RESET}")
    # Re-check positions
    aster_account_after = await aster.get_perp_account_info()
    aster_positions_after = aster_account_after.get('positions', [])
    aster_size_after = 0.0
    for pos in aster_positions_after:
        if pos.get('symbol') == symbol:
            aster_size_after = float(pos.get('positionAmt', 0))
            break
    aster_after_color = Colors.GREEN if abs(aster_size_after) < 0.0001 else Colors.RED
    print(f"  {aster_after_color}Aster position:  {aster_size_after:+.6f} {symbol_clean}{Colors.RESET}")

    lighter_size_after = await lighter_client.get_lighter_open_size(account_api, env["ACCOUNT_INDEX"], l_market_id)
    lighter_after_color = Colors.GREEN if abs(lighter_size_after) < l_amount_tick else Colors.RED
    print(f"  {lighter_after_color}Lighter position: {lighter_size_after:+.6f} {symbol_clean}{Colors.RESET}")

    aster_closed = abs(aster_size_after) < 0.0001
    lighter_closed = abs(lighter_size_after) < l_amount_tick

    if aster_closed and lighter_closed:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✓ Hedge closed successfully on both exchanges!{Colors.RESET}")
    else:
        print(f"\n{Colors.YELLOW}{Colors.BOLD}⚠️  WARNING: One or more positions not fully closed.{Colors.RESET}")
        if not aster_closed:
            print(f"  {Colors.RED}Aster position remaining: {aster_size_after:+.6f} {symbol_clean}{Colors.RESET}")
        if not lighter_closed:
            print(f"  {Colors.RED}Lighter position remaining: {lighter_size_after:+.6f} {symbol_clean}{Colors.RESET}")
        print(f"  {Colors.YELLOW}Please check both exchanges manually.{Colors.RESET}\n")

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
        print(f"{'Symbol':<10} {'Net APR':<10} {'Long':<8} {'Short':<8} {'Aster APR':<11} {'Lighter APR':<13} {'Aster Mid':<15} {'Lighter Mid':<15} {'Spread':<10}")
        print(f"{'-' * 150}")

        for i, r in enumerate(available[:limit]):
            marker = f"{Colors.CYAN}→{Colors.RESET}" if r['symbol'] == current_symbol else " "
            color = Colors.GREEN if r['net_apr'] >= 10 else Colors.YELLOW if r['net_apr'] >= 5 else Colors.RESET
            spread_str = f"{r['spread_pct']:.3f}%" if r.get('spread_pct') is not None else "N/A"

            aster_mid_str = format_price(r.get('aster_mid'))
            lighter_mid_str = format_price(r.get('lighter_mid'))

            print(f"{marker} {r['symbol']:<8} {color}{r['net_apr']:>8.2f}%{Colors.RESET} "
                  f"{r['long_exch']:<8} {r['short_exch']:<8} "
                  f"{r['aster_apr']:>9.2f}% {r['lighter_apr']:>11.2f}% "
                  f"{aster_mid_str:<15} {lighter_mid_str:<15} {spread_str:<10}")

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

async def verify_and_recover_position(state_mgr: StateManager, env: dict, aster: AsterApiManager) -> bool:
    """
    Verify that a saved position actually exists on both exchanges.
    Returns True if position is valid and should be held, False otherwise.
    """
    position = state_mgr.state.get("current_position")
    if not position:
        return False

    symbol = position.get("symbol")
    if not symbol:
        logger.warning("Position in state has no symbol, clearing...")
        state_mgr.state["current_position"] = None
        state_mgr.save()
        return False

    logger.info(f"\n{Colors.CYAN}═══════════════════════════════════════════════════════════════{Colors.RESET}")
    logger.info(f"{Colors.CYAN}Position Recovery: Checking for existing {symbol} position...{Colors.RESET}")
    logger.info(f"{Colors.CYAN}═══════════════════════════════════════════════════════════════{Colors.RESET}\n")

    print(f"\n{Colors.CYAN}Attempting to recover position for {symbol}...{Colors.RESET}")
    print(f"  Opened at: {position.get('opened_at', 'Unknown')}")
    print(f"  Long:  {position.get('long_exchange', 'Unknown')}")
    print(f"  Short: {position.get('short_exchange', 'Unknown')}\n")

    api_client = None
    try:
        # Get market details
        symbol_clean = symbol.replace("USDT", "")

        # Initialize Lighter client
        api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
        order_api = lighter.OrderApi(api_client)
        account_api = lighter.AccountApi(api_client)

        # Get Lighter market ID
        l_market_id, _, _ = await lighter_client.get_lighter_market_details(order_api, symbol_clean)

        # Check Aster position
        print(f"{Colors.CYAN}Checking positions on both exchanges...{Colors.RESET}")
        aster_account = await aster.get_perp_account_info()
        aster_positions = aster_account.get('positions', [])
        aster_size = 0.0
        for pos in aster_positions:
            if pos.get('symbol') == symbol:
                aster_size = float(pos.get('positionAmt', 0))
                break
        aster_color = Colors.YELLOW if abs(aster_size) > 0.0001 else Colors.GRAY
        print(f"  {aster_color}Aster:   {aster_size:+.6f} {symbol_clean}{Colors.RESET}")

        # Check Lighter position
        lighter_size = await lighter_client.get_lighter_open_size(account_api, env["ACCOUNT_INDEX"], l_market_id, symbol=symbol_clean)
        lighter_color = Colors.YELLOW if abs(lighter_size) > 0.0001 else Colors.GRAY
        print(f"  {lighter_color}Lighter: {lighter_size:+.6f} {symbol_clean}{Colors.RESET}\n")

        # Verify positions exist and are opposite (delta-neutral)
        has_aster_pos = abs(aster_size) > 0.0001
        has_lighter_pos = abs(lighter_size) > 0.0001

        if has_aster_pos and has_lighter_pos:
            # Check if positions are opposite (delta-neutral)
            if (aster_size > 0 and lighter_size < 0) or (aster_size < 0 and lighter_size > 0):
                print(f"{Colors.GREEN}✓ Valid delta-neutral position found!{Colors.RESET}")
                print(f"  {Colors.CYAN}Resuming HOLDING state...{Colors.RESET}")

                # Check if actual position size differs from saved size_base
                metadata = position.get("metadata", {})
                saved_size_base = metadata.get("size_base", 0.0)

                # Get the absolute actual sizes (they should be equal on both exchanges)
                actual_aster_size = abs(aster_size)
                actual_lighter_size = abs(lighter_size)

                # Use the average of both as the actual size
                actual_size_base = (actual_aster_size + actual_lighter_size) / 2.0

                # Check if there's a significant difference (more than 0.1% or 0.001 units)
                size_diff = abs(actual_size_base - saved_size_base)
                size_diff_pct = (size_diff / saved_size_base * 100) if saved_size_base > 0 else 0

                if size_diff > 0.001 and size_diff_pct > 0.1:
                    print(f"{Colors.YELLOW}⚠ Position size mismatch detected:{Colors.RESET}")
                    print(f"  {Colors.GRAY}Saved size_base:  {saved_size_base:.6f} {symbol_clean}{Colors.RESET}")
                    print(f"  {Colors.GRAY}Actual Aster:     {actual_aster_size:.6f} {symbol_clean}{Colors.RESET}")
                    print(f"  {Colors.GRAY}Actual Lighter:   {actual_lighter_size:.6f} {symbol_clean}{Colors.RESET}")
                    print(f"  {Colors.GREEN}Updating size_base to: {actual_size_base:.6f} {symbol_clean}{Colors.RESET}\n")

                    logger.warning(
                        f"Position size mismatch: saved={saved_size_base:.6f}, "
                        f"actual_aster={actual_aster_size:.6f}, actual_lighter={actual_lighter_size:.6f}"
                    )

                    # Update the metadata with the actual size
                    metadata["size_base"] = actual_size_base
                    position["metadata"] = metadata
                    state_mgr.save()

                    logger.info(f"Updated size_base from {saved_size_base:.6f} to {actual_size_base:.6f}")
                else:
                    print(f"  {Colors.GREEN}Position size matches saved value: {saved_size_base:.6f} {symbol_clean}{Colors.RESET}\n")

                # Calculate time remaining
                target_close = from_iso_z(position["target_close_at"])
                now = utc_now()
                time_remaining = (target_close - now).total_seconds() / 3600

                if time_remaining > 0:
                    print(f"  {Colors.BLUE}Time remaining: {Colors.BOLD}{time_remaining:.2f} hours{Colors.RESET}\n")
                else:
                    print(f"  {Colors.YELLOW}Hold duration already complete, will close soon{Colors.RESET}\n")

                logger.info(f"Position recovery successful for {symbol}")
                return True
            else:
                print(f"{Colors.YELLOW}⚠ Positions exist but are not properly hedged:{Colors.RESET}")
                print(f"  {Colors.YELLOW}Both positions are on the same side (not delta-neutral){Colors.RESET}")
                print(f"  {Colors.RED}Clearing saved state. Please close positions manually.{Colors.RESET}\n")
        elif has_aster_pos or has_lighter_pos:
            print(f"{Colors.YELLOW}⚠ Partial position detected:{Colors.RESET}")
            if has_aster_pos:
                print(f"  {Colors.YELLOW}Aster has position but Lighter does not{Colors.RESET}")
            else:
                print(f"  {Colors.YELLOW}Lighter has position but Aster does not{Colors.RESET}")
            print(f"  {Colors.RED}Clearing saved state. Please close positions manually.{Colors.RESET}\n")
        else:
            print(f"{Colors.GRAY}No positions found on either exchange.{Colors.RESET}")
            print(f"  {Colors.GRAY}Clearing saved state and resuming normal operation.{Colors.RESET}\n")

        # Clear invalid position from state
        state_mgr.state["current_position"] = None
        state_mgr.save()
        logger.info(f"Position state cleared for {symbol}")
        return False

    except Exception as e:
        print(f"{Colors.RED}Error during position recovery: {e}{Colors.RESET}")
        print(f"  Clearing saved state for safety.\n")
        logger.error(f"Position recovery failed: {e}", exc_info=True)

        # Clear position state on error
        state_mgr.state["current_position"] = None
        state_mgr.save()
        return False
    finally:
        if api_client:
            await api_client.close()


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

async def main_loop(state_mgr: StateManager, env: dict, config: BotConfig):
    """Main bot loop."""

    # Initialize Aster API manager
    aster = AsterApiManager(
        api_user=env["ASTER_API_USER"],
        api_signer=env["ASTER_API_SIGNER"],
        api_private_key=env["ASTER_API_PRIVATE_KEY"],
        apiv1_public=env["ASTER_APIV1_PUBLIC"],
        apiv1_private=env["ASTER_APIV1_PRIVATE"]
    )

    # Perform position recovery check on startup
    if state_mgr.state.get("current_position") is not None:
        logger.info("Saved position detected, attempting recovery...")
        position_valid = await verify_and_recover_position(state_mgr, env, aster)

        if position_valid:
            # Ensure state is set to HOLDING
            if state_mgr.get_state() != BotState.HOLDING:
                logger.info("Setting state to HOLDING after successful recovery")
                state_mgr.set_state(BotState.HOLDING)
        else:
            # Position was cleared, set to IDLE
            if state_mgr.get_state() == BotState.HOLDING:
                logger.info("Setting state to IDLE after clearing invalid position")
                state_mgr.set_state(BotState.IDLE)

    try:
        while True:
            current_state = state_mgr.get_state()

            if current_state == BotState.IDLE or current_state == BotState.WAITING:
                # Analyze and open position
                logger.info("Starting new cycle - analyzing funding rates...")

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
                display_funding_table(available, unavailable, current_symbol=None, limit=10)

                candidates = [r for r in available if r["net_apr"] >= config.min_net_apr_threshold]

                if not candidates:
                    logger.info("No candidates meet minimum APR threshold, waiting...")
                    state_mgr.set_state(BotState.WAITING)
                    await asyncio.sleep(60)
                    continue

                # Try to open best position
                best = candidates[0]
                logger.info(f"\n{Colors.CYAN}Opening position for {best['symbol']} (Net APR: {best['net_apr']:.2f}%){Colors.RESET}")

                state_mgr.set_state(BotState.OPENING)

                try:
                    metadata = await open_delta_neutral_position(
                        env,
                        aster,
                        best['symbol'],
                        best['long_exch'],
                        best['short_exch'],
                        config.leverage,
                        config.notional_per_position,
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
                        "expected_net_apr": best['net_apr']
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

                        # Mark cycle as complete
                        state_mgr.state["completed_cycles"].append({
                            "symbol": position["symbol"],
                            "opened_at": position["opened_at"],
                            "closed_at": utc_now_iso(),
                            "expected_net_apr": position.get("expected_net_apr", 0.0),
                            "status": "success"
                        })
                        state_mgr.state["cumulative_stats"]["total_cycles"] += 1
                        state_mgr.state["cumulative_stats"]["successful_cycles"] += 1
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

                    # Fetch current PnL from both exchanges
                    try:
                        aster_pnl, lighter_pnl = await get_position_pnls(env, aster, position['symbol'])

                        # Calculate position value for percentage calculation
                        metadata = position.get('metadata', {})
                        size_base = metadata.get('size_base', 0.0)
                        avg_mid = metadata.get('avg_mid', 0.0)
                        position_value = size_base * avg_mid if size_base and avg_mid else config.notional_per_position

                        # Calculate worst PnL (most negative) and determine exchange
                        worst_pnl = None
                        worst_exchange = None

                        if aster_pnl is not None and lighter_pnl is not None:
                            if aster_pnl <= lighter_pnl:
                                worst_pnl = aster_pnl
                                worst_exchange = "Aster"
                            else:
                                worst_pnl = lighter_pnl
                                worst_exchange = "Lighter"
                        elif aster_pnl is not None:
                            worst_pnl = aster_pnl
                            worst_exchange = "Aster"
                        elif lighter_pnl is not None:
                            worst_pnl = lighter_pnl
                            worst_exchange = "Lighter"

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

                                    # Mark cycle as stopped-loss in completed_cycles
                                    state_mgr.state["completed_cycles"].append({
                                        "symbol": position["symbol"],
                                        "opened_at": position["opened_at"],
                                        "closed_at": utc_now_iso(),
                                        "expected_net_apr": position.get("expected_net_apr", 0.0),
                                        "status": "stop-loss",
                                        "pnl_at_close": worst_pnl,
                                        "pnl_pct_at_close": pnl_pct,
                                        "worst_exchange": worst_exchange
                                    })
                                    state_mgr.state["cumulative_stats"]["total_cycles"] += 1
                                    state_mgr.state["cumulative_stats"]["failed_cycles"] += 1
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
                            logger.info(
                                f"Holding position for {Colors.CYAN}{Colors.BOLD}{position['symbol']}{Colors.RESET} - "
                                f"{Colors.BLUE}{Colors.BOLD}{time_remaining:.2f} hours{Colors.RESET} remaining | "
                                f"Stop-loss: {Colors.YELLOW}{stop_loss_pct:.2f}%{Colors.RESET} | "
                                f"Worst PnL: {pnl_str}"
                            )
                        elif worst_pnl is not None:
                            # Fallback if position_value couldn't be calculated
                            pnl_color = Colors.GREEN if worst_pnl >= 0 else Colors.RED
                            pnl_str = f"{pnl_color}${worst_pnl:+.2f} (on {Colors.CYAN}{worst_exchange}{pnl_color}){Colors.RESET}" if worst_exchange else f"{pnl_color}${worst_pnl:+.2f}{Colors.RESET}"
                            logger.info(
                                f"Holding position for {Colors.CYAN}{Colors.BOLD}{position['symbol']}{Colors.RESET} - "
                                f"{Colors.BLUE}{Colors.BOLD}{time_remaining:.2f} hours{Colors.RESET} remaining | "
                                f"Stop-loss: {Colors.YELLOW}{stop_loss_pct:.2f}%{Colors.RESET} | "
                                f"Worst PnL: {pnl_str}"
                            )
                        else:
                            logger.info(
                                f"Holding position for {Colors.CYAN}{Colors.BOLD}{position['symbol']}{Colors.RESET} - "
                                f"{Colors.BLUE}{Colors.BOLD}{time_remaining:.2f} hours{Colors.RESET} remaining | "
                                f"Stop-loss: {Colors.YELLOW}{stop_loss_pct:.2f}%{Colors.RESET} | "
                                f"Worst PnL: {Colors.GRAY}N/A{Colors.RESET}"
                            )
                    except Exception as e:
                        logger.debug(f"Failed to fetch PnL for holding message: {e}")
                        logger.info(
                            f"Holding position for {Colors.CYAN}{Colors.BOLD}{position['symbol']}{Colors.RESET} - "
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
    config = BotConfig.load_from_file(args.config)

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
        asyncio.run(main_loop(state_mgr, env, config))
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
