#!/usr/bin/env python3
"""
emergency_exit.py
-----------------
Emergency exit script to close any delta-neutral positions found on Lighter and Aster exchanges.

Features:
- Detects positions on both exchanges
- Shows current PnL for each leg
- Calculates total PnL
- Asks for confirmation before closing
- Closes positions on both exchanges

Usage:
    python emergency_exit.py
"""

import asyncio
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import lighter

import lighter_client
from aster_api_manager import AsterApiManager


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
    print(f"\n{Colors.BOLD}{Colors.RED}{'═' * 100}")
    print(f"{'EMERGENCY EXIT - DELTA NEUTRAL POSITION CLOSER':^100}")
    print(f"{'═' * 100}{Colors.RESET}\n")
    print(f"{Colors.GRAY}Timestamp: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC{Colors.RESET}\n")


def load_env() -> dict:
    """Load environment variables."""
    load_dotenv()

    env = {
        # Aster credentials
        "ASTER_API_USER": os.getenv("ASTER_API_USER"),
        "ASTER_API_SIGNER": os.getenv("ASTER_API_SIGNER"),
        "ASTER_API_PRIVATE_KEY": os.getenv("ASTER_API_PRIVATE_KEY"),
        "ASTER_APIV1_PUBLIC": os.getenv("ASTER_APIV1_PUBLIC"),
        "ASTER_APIV1_PRIVATE": os.getenv("ASTER_APIV1_PRIVATE"),

        # Lighter credentials
        "LIGHTER_BASE_URL": os.getenv("LIGHTER_BASE_URL", os.getenv("BASE_URL", "https://mainnet.zklighter.elliot.ai")),
        "LIGHTER_WS_URL": os.getenv("LIGHTER_WS_URL", os.getenv("WEBSOCKET_URL", "wss://mainnet.zklighter.elliot.ai/stream")),
        "API_KEY_PRIVATE_KEY": os.getenv("API_KEY_PRIVATE_KEY") or os.getenv("LIGHTER_PRIVATE_KEY"),
        "ACCOUNT_INDEX": int(os.getenv("ACCOUNT_INDEX", os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))),
        "API_KEY_INDEX": int(os.getenv("API_KEY_INDEX", os.getenv("LIGHTER_API_KEY_INDEX", "0"))),
    }

    # Check required variables
    missing = [key for key in ("ASTER_API_USER", "ASTER_API_SIGNER", "ASTER_API_PRIVATE_KEY",
                                "ASTER_APIV1_PUBLIC", "ASTER_APIV1_PRIVATE", "API_KEY_PRIVATE_KEY")
               if not env.get(key)]
    if missing:
        print(f"{Colors.RED}Error: Missing required environment variables: {', '.join(missing)}{Colors.RESET}")
        sys.exit(1)

    return env


async def get_all_positions(env: dict, aster: AsterApiManager) -> dict:
    """
    Get all positions on both exchanges.

    Returns:
        dict with 'aster' and 'lighter' keys containing position lists
    """
    print(f"{Colors.CYAN}Scanning for positions on both exchanges...{Colors.RESET}\n")

    # Initialize Lighter client
    api_client = lighter.ApiClient(configuration=lighter.Configuration(host=env["LIGHTER_BASE_URL"]))
    order_api = lighter.OrderApi(api_client)
    account_api = lighter.AccountApi(api_client)

    try:
        # Get Aster positions
        print("  Checking Aster...")
        aster_account = await aster.get_perp_account_info()
        aster_positions = []

        for pos in aster_account.get('positions', []):
            size = float(pos.get('positionAmt', 0))
            if abs(size) > 0.0001:  # Has position
                symbol = pos.get('symbol')
                entry_price = float(pos.get('entryPrice', 0))
                unrealized_pnl = float(pos.get('unrealizedProfit', 0))

                aster_positions.append({
                    'symbol': symbol,
                    'size': size,
                    'entry_price': entry_price,
                    'unrealized_pnl': unrealized_pnl,
                    'side': 'LONG' if size > 0 else 'SHORT'
                })

        print(f"    Found {len(aster_positions)} position(s)")

        # Get Lighter positions
        print("  Checking Lighter...")
        lighter_positions = await lighter_client.get_all_lighter_positions(account_api, env["ACCOUNT_INDEX"])
        print(f"    Found {len(lighter_positions)} position(s)\n")

        await api_client.close()

        return {
            'aster': aster_positions,
            'lighter': lighter_positions
        }

    except Exception as e:
        print(f"{Colors.RED}Error fetching positions: {e}{Colors.RESET}")
        await api_client.close()
        raise


def match_delta_neutral_positions(aster_positions: list, lighter_positions: list) -> list:
    """
    Match positions that form delta-neutral pairs.

    Returns:
        List of matched pairs with combined info
    """
    matched_pairs = []

    for aster_pos in aster_positions:
        aster_symbol = aster_pos['symbol']

        # Try to find matching Lighter position
        for lighter_pos in lighter_positions:
            lighter_symbol = lighter_pos['symbol'] + 'USDT'  # Lighter uses 'BTC', Aster uses 'BTCUSDT'

            if aster_symbol == lighter_symbol:
                # Check if they're opposite (delta-neutral)
                aster_size = aster_pos['size']
                lighter_size = lighter_pos['size']

                if (aster_size > 0 and lighter_size < 0) or (aster_size < 0 and lighter_size > 0):
                    # Delta-neutral pair found!
                    matched_pairs.append({
                        'symbol': aster_symbol,
                        'aster': aster_pos,
                        'lighter': lighter_pos
                    })

    return matched_pairs


def display_positions(matched_pairs: list):
    """Display matched delta-neutral positions with PnL."""
    if not matched_pairs:
        print(f"{Colors.YELLOW}No delta-neutral positions found.{Colors.RESET}\n")
        return None

    print(f"{Colors.BOLD}Delta-Neutral Positions Found:{Colors.RESET}\n")
    print(f"{'Symbol':<12} {'Exchange':<10} {'Side':<8} {'Size':<15} {'Entry Price':<15} {'Unrealized PnL':<18}")
    print(f"{'-' * 100}")

    total_pnl = 0.0

    for pair in matched_pairs:
        symbol = pair['symbol']
        aster_pos = pair['aster']
        lighter_pos = pair['lighter']

        # Aster leg
        aster_pnl = aster_pos['unrealized_pnl']
        aster_color = Colors.GREEN if aster_pnl >= 0 else Colors.RED
        print(f"{symbol:<12} {'Aster':<10} {aster_pos['side']:<8} "
              f"{abs(aster_pos['size']):<15.6f} ${aster_pos['entry_price']:<14.4f} "
              f"{aster_color}${aster_pnl:>16.4f}{Colors.RESET}")

        # Lighter leg
        lighter_pnl = lighter_pos['unrealized_pnl']
        lighter_color = Colors.GREEN if lighter_pnl >= 0 else Colors.RED
        print(f"{' '*12} {'Lighter':<10} {lighter_pos['side']:<8} "
              f"{abs(lighter_pos['size']):<15.6f} ${lighter_pos['entry_price']:<14.4f} "
              f"{lighter_color}${lighter_pnl:>16.4f}{Colors.RESET}")

        # Pair total
        pair_pnl = aster_pnl + lighter_pnl
        pair_color = Colors.GREEN if pair_pnl >= 0 else Colors.RED
        print(f"{' '*12} {Colors.BOLD}{'Pair Total':<10}{Colors.RESET} {' '*8} "
              f"{' '*15} {' '*15} {pair_color}{Colors.BOLD}${pair_pnl:>16.4f}{Colors.RESET}")
        print(f"{'-' * 100}")

        total_pnl += pair_pnl

    # Grand total
    total_color = Colors.GREEN if total_pnl >= 0 else Colors.RED
    print(f"\n{Colors.BOLD}Total Unrealized PnL: {total_color}${total_pnl:.4f}{Colors.RESET}\n")

    return total_pnl


async def close_positions(env: dict, aster: AsterApiManager, matched_pairs: list):
    """Close all matched delta-neutral positions."""
    print(f"\n{Colors.CYAN}Closing positions on both exchanges...{Colors.RESET}\n")

    # Initialize Lighter client
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

    try:
        for pair in matched_pairs:
            symbol = pair['symbol']
            symbol_clean = symbol.replace("USDT", "")

            print(f"Processing {symbol}...")

            # Get Lighter market details
            l_market_id, l_price_tick, l_amount_tick = await lighter_client.get_lighter_market_details(order_api, symbol_clean)
            lighter_bid, lighter_ask = await lighter_client.get_lighter_best_bid_ask(order_api, symbol_clean, l_market_id)

            tasks = []

            # Close Lighter position
            lighter_pos = pair['lighter']
            lighter_size = lighter_pos['size']

            if abs(lighter_size) > l_amount_tick:
                lighter_close_side = "sell" if lighter_size > 0 else "buy"
                ref_price = lighter_bid if lighter_close_side == "sell" else lighter_ask

                if ref_price:
                    print(f"  Closing Lighter {lighter_pos['side']} position: {abs(lighter_size):.6f} {symbol_clean}")
                    tasks.append(
                        lighter_client.lighter_close_position(
                            signer,
                            l_market_id,
                            l_price_tick,
                            l_amount_tick,
                            lighter_close_side,
                            abs(lighter_size),
                            ref_price,
                            cross_ticks=100,
                        )
                    )
                else:
                    print(f"  {Colors.YELLOW}Warning: No reference price for Lighter, skipping{Colors.RESET}")

            # Close Aster position
            aster_pos = pair['aster']
            aster_size = aster_pos['size']

            if abs(aster_size) > 0.0001:
                aster_close_side = 'BUY' if aster_size < 0 else 'SELL'
                print(f"  Closing Aster {aster_pos['side']} position: {abs(aster_size):.6f} {symbol_clean}")
                tasks.append(
                    aster.close_perp_position(symbol, str(abs(aster_size)), aster_close_side)
                )

            # Execute closes
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                errors = [res for res in results if isinstance(res, Exception)]

                if errors:
                    print(f"  {Colors.RED}✗ Error closing {symbol}:{Colors.RESET}")
                    for err in errors:
                        print(f"    {err}")
                else:
                    print(f"  {Colors.GREEN}✓ Close orders sent for {symbol}{Colors.RESET}")

            print()

        # Wait for orders to process
        await asyncio.sleep(3)

        # Verify closure
        print(f"\n{Colors.CYAN}Verifying closure...{Colors.RESET}\n")

        for pair in matched_pairs:
            symbol = pair['symbol']
            symbol_clean = symbol.replace("USDT", "")

            # Check Aster
            aster_account = await aster.get_perp_account_info()
            aster_size = 0.0
            for pos in aster_account.get('positions', []):
                if pos.get('symbol') == symbol:
                    aster_size = float(pos.get('positionAmt', 0))
                    break

            # Check Lighter
            l_market_id, l_price_tick, l_amount_tick = await lighter_client.get_lighter_market_details(order_api, symbol_clean)
            lighter_size = await lighter_client.get_lighter_open_size(account_api, env["ACCOUNT_INDEX"], l_market_id)

            aster_closed = abs(aster_size) < 0.0001
            lighter_closed = abs(lighter_size) < l_amount_tick

            if aster_closed and lighter_closed:
                print(f"  {Colors.GREEN}✓ {symbol}: Fully closed on both exchanges{Colors.RESET}")
            else:
                print(f"  {Colors.YELLOW}⚠ {symbol}: Partially closed{Colors.RESET}")
                if not aster_closed:
                    print(f"    Aster remaining: {aster_size:+.6f}")
                if not lighter_closed:
                    print(f"    Lighter remaining: {lighter_size:+.6f}")

        print()

    finally:
        await signer.close()
        await api_client.close()


async def main():
    """Main execution."""
    print_header()

    # Load environment
    env = load_env()

    # Initialize Aster
    aster = AsterApiManager(
        api_user=env["ASTER_API_USER"],
        api_signer=env["ASTER_API_SIGNER"],
        api_private_key=env["ASTER_API_PRIVATE_KEY"],
        apiv1_public=env["ASTER_APIV1_PUBLIC"],
        apiv1_private=env["ASTER_APIV1_PRIVATE"]
    )

    try:
        # Get all positions
        positions = await get_all_positions(env, aster)

        # Match delta-neutral pairs
        matched_pairs = match_delta_neutral_positions(positions['aster'], positions['lighter'])

        # Display positions and PnL
        total_pnl = display_positions(matched_pairs)

        if not matched_pairs:
            print(f"{Colors.GRAY}No positions to close. Exiting.{Colors.RESET}\n")
            return

        # Ask for confirmation
        print(f"{Colors.BOLD}{Colors.RED}WARNING: This will close all delta-neutral positions shown above!{Colors.RESET}")
        print(f"{Colors.YELLOW}This action cannot be undone.{Colors.RESET}\n")

        try:
            confirmation = input(f"{Colors.BOLD}Press ENTER to proceed with closing, or Ctrl+C to cancel: {Colors.RESET}")
        except KeyboardInterrupt:
            print(f"\n\n{Colors.YELLOW}Operation cancelled by user.{Colors.RESET}\n")
            return

        # Close positions
        await close_positions(env, aster, matched_pairs)

        print(f"\n{Colors.GREEN}{Colors.BOLD}Emergency exit complete!{Colors.RESET}\n")

    except Exception as e:
        print(f"\n{Colors.RED}Error during emergency exit: {e}{Colors.RESET}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        await aster.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n\n{Colors.YELLOW}Operation cancelled by user.{Colors.RESET}\n")
        sys.exit(0)
