#!/usr/bin/env python3
"""
check_lighter_positions.py
--------------------------
Standalone script to check current open positions on Lighter exchange.

Usage:
    python check_lighter_positions.py
    python check_lighter_positions.py --symbol BTC
"""

import asyncio
import argparse
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import lighter

# Import helper functions
import lighter_client


# ANSI color codes
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GRAY = '\033[90m'


def print_header():
    """Print script header."""
    print(f"\n{Colors.BOLD}{'═' * 100}")
    print(f"{'LIGHTER POSITION CHECKER':^100}")
    print(f"{'═' * 100}{Colors.RESET}\n")
    print(f"{Colors.GRAY}Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC{Colors.RESET}\n")


def print_position_table(positions: list, balance_info: dict = None):
    """Display positions in a formatted table."""
    if not positions:
        print(f"{Colors.YELLOW}No open positions found.{Colors.RESET}\n")
        return

    print(f"{Colors.GREEN}Open Positions ({len(positions)} found):{Colors.RESET}\n")

    # Header
    print(f"{'Symbol':<12} {'Side':<8} {'Size':<15} {'Entry Price':<15} {'Unrealized PnL':<18}")
    print(f"{'-' * 100}")

    total_pnl = 0.0

    for pos in positions:
        symbol = pos.get('symbol', 'UNKNOWN')
        size = pos.get('size', 0.0)
        side = 'LONG' if size > 0 else 'SHORT'
        abs_size = abs(size)
        entry_price = pos.get('entry_price', 0.0)
        unrealized_pnl = pos.get('unrealized_pnl', 0.0)

        total_pnl += unrealized_pnl

        # Color code PnL
        pnl_color = Colors.GREEN if unrealized_pnl >= 0 else Colors.RED
        side_color = Colors.CYAN if size > 0 else Colors.YELLOW

        print(f"{symbol:<12} {side_color}{side:<8}{Colors.RESET} "
              f"{abs_size:<15.6f} ${entry_price:<14.4f} "
              f"{pnl_color}${unrealized_pnl:>16.4f}{Colors.RESET}")

    print(f"{'-' * 100}")

    # Summary
    total_color = Colors.GREEN if total_pnl >= 0 else Colors.RED
    print(f"\n{Colors.BOLD}Total Unrealized PnL: {total_color}${total_pnl:.4f}{Colors.RESET}\n")

    # Balance info if provided
    if balance_info:
        print(f"{Colors.BLUE}Account Balance:{Colors.RESET}")
        print(f"  Portfolio Value:    ${balance_info['portfolio_value']:.2f}")
        print(f"  Available Balance:  ${balance_info['available_balance']:.2f}")
        print(f"  Margin in Use:      ${balance_info['portfolio_value'] - balance_info['available_balance']:.2f}\n")


def print_detailed_position(details: dict, symbol: str):
    """Display detailed position information."""
    print(f"\n{Colors.BOLD}{'─' * 100}")
    print(f"Detailed Position: {symbol}")
    print(f"{'─' * 100}{Colors.RESET}\n")

    side = details.get('side', 'FLAT')
    size = details.get('size', 0.0)
    abs_size = details.get('abs_size', 0.0)
    entry_price = details.get('entry_price', 0.0)
    unrealized_pnl = details.get('unrealized_pnl', 0.0)
    leverage = details.get('leverage', 0.0)
    margin_mode = details.get('margin_mode', 0)
    imf = details.get('initial_margin_fraction', 0.0)

    side_color = Colors.CYAN if size > 0 else Colors.YELLOW
    pnl_color = Colors.GREEN if unrealized_pnl >= 0 else Colors.RED

    print(f"  Symbol:              {symbol}")
    print(f"  Side:                {side_color}{side}{Colors.RESET}")
    print(f"  Size:                {abs_size:.6f} (signed: {size:+.6f})")
    print(f"  Entry Price:         ${entry_price:.4f}")
    print(f"  Unrealized PnL:      {pnl_color}${unrealized_pnl:.4f}{Colors.RESET}")
    print(f"  Leverage:            {leverage:.2f}x")
    print(f"  Margin Mode:         {'Cross' if margin_mode == 0 else 'Isolated'}")
    print(f"  Initial Margin %:    {imf * 100:.2f}%")
    print(f"\n{Colors.BOLD}{'─' * 100}{Colors.RESET}\n")


async def get_lighter_balance_info(env: dict) -> dict:
    """Get Lighter account balance."""
    try:
        ws_url = env["LIGHTER_WS_URL"]
        account_index = int(env.get("ACCOUNT_INDEX", "0"))

        available, portfolio_value = await lighter_client.get_lighter_balance(ws_url, account_index, timeout=10.0)

        return {
            'portfolio_value': portfolio_value,
            'available_balance': available
        }
    except Exception as e:
        print(f"{Colors.RED}Warning: Could not fetch balance info: {e}{Colors.RESET}")
        return None


async def check_all_positions(env: dict) -> None:
    """Check all open positions on Lighter."""
    print_header()

    # Initialize Lighter client
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
    account_api = lighter.AccountApi(api_client)
    account_index = int(env.get("ACCOUNT_INDEX", "0"))

    try:
        print(f"{Colors.CYAN}Fetching positions for account index {account_index}...{Colors.RESET}\n")

        # Get balance info
        balance_info = await get_lighter_balance_info(env)

        # Get all positions
        positions = await lighter_client.get_all_lighter_positions(account_api, account_index)

        # Display positions
        print_position_table(positions, balance_info)

    except Exception as e:
        print(f"{Colors.RED}Error fetching positions: {e}{Colors.RESET}\n")
        import traceback
        traceback.print_exc()
    finally:
        await api_client.close()


async def check_specific_position(env: dict, symbol: str) -> None:
    """Check position for a specific symbol."""
    print_header()

    # Initialize Lighter client
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
    order_api = lighter.OrderApi(api_client)
    account_api = lighter.AccountApi(api_client)
    account_index = int(env.get("ACCOUNT_INDEX", "0"))

    try:
        print(f"{Colors.CYAN}Fetching position for {symbol}...{Colors.RESET}\n")

        # Get market details
        symbol_clean = symbol.replace("USDT", "")
        market_id, price_tick, amount_tick = await lighter_client.get_lighter_market_details(order_api, symbol_clean)

        print(f"Market ID: {market_id}")
        print(f"Price Tick: {price_tick}")
        print(f"Amount Tick: {amount_tick}\n")

        # Get detailed position
        details = await lighter_client.get_lighter_position_details(account_api, account_index, market_id)

        if details:
            print_detailed_position(details, symbol)
        else:
            print(f"{Colors.YELLOW}No position found for {symbol}.{Colors.RESET}\n")

        # Also get balance info
        balance_info = await get_lighter_balance_info(env)
        if balance_info:
            print(f"{Colors.BLUE}Account Balance:{Colors.RESET}")
            print(f"  Portfolio Value:    ${balance_info['portfolio_value']:.2f}")
            print(f"  Available Balance:  ${balance_info['available_balance']:.2f}\n")

    except ValueError as e:
        print(f"{Colors.RED}Error: {e}{Colors.RESET}\n")
    except Exception as e:
        print(f"{Colors.RED}Error fetching position: {e}{Colors.RESET}\n")
        import traceback
        traceback.print_exc()
    finally:
        await api_client.close()


def load_env_vars() -> dict:
    """Load environment variables."""
    load_dotenv()

    env = {
        "LIGHTER_BASE_URL": os.getenv("LIGHTER_BASE_URL", os.getenv("BASE_URL", "https://mainnet.zklighter.elliot.ai")),
        "LIGHTER_WS_URL": os.getenv("LIGHTER_WS_URL", os.getenv("WEBSOCKET_URL", "wss://mainnet.zklighter.elliot.ai/stream")),
        "API_KEY_PRIVATE_KEY": os.getenv("API_KEY_PRIVATE_KEY") or os.getenv("LIGHTER_PRIVATE_KEY"),
        "ACCOUNT_INDEX": int(os.getenv("ACCOUNT_INDEX", os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))),
        "API_KEY_INDEX": int(os.getenv("API_KEY_INDEX", os.getenv("LIGHTER_API_KEY_INDEX", "0"))),
    }

    # Check required variables
    if not env["API_KEY_PRIVATE_KEY"]:
        print(f"{Colors.RED}Error: LIGHTER_PRIVATE_KEY or API_KEY_PRIVATE_KEY not found in environment{Colors.RESET}")
        sys.exit(1)

    return env


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="Check current open positions on Lighter exchange",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Check all positions
  python check_lighter_positions.py

  # Check specific symbol
  python check_lighter_positions.py --symbol BTC
  python check_lighter_positions.py --symbol ETHUSDT
        """
    )
    parser.add_argument("--symbol", "-s", help="Check specific symbol (e.g., BTC, ETH, BTCUSDT)")

    args = parser.parse_args()

    # Load environment
    env = load_env_vars()

    # Run position check
    try:
        if args.symbol:
            asyncio.run(check_specific_position(env, args.symbol))
        else:
            asyncio.run(check_all_positions(env))
    except KeyboardInterrupt:
        print(f"\n{Colors.YELLOW}Interrupted by user.{Colors.RESET}\n")
    except Exception as e:
        print(f"{Colors.RED}Fatal error: {e}{Colors.RESET}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
