# Lighter Position Checker

A standalone Python script to check current open positions on Lighter exchange.

## Features

- ✅ View all open positions at once
- ✅ Check specific symbol positions with detailed info
- ✅ Display unrealized PnL for each position
- ✅ Show account balance and margin usage
- ✅ Color-coded output (green for profit, red for loss)
- ✅ Works independently from the main trading bot

## Usage

### Check All Positions
```bash
python check_lighter_positions.py
```

### Check Specific Symbol
```bash
# By symbol name
python check_lighter_positions.py --symbol BTC
python check_lighter_positions.py --symbol ETH

# With USDT suffix
python check_lighter_positions.py --symbol BTCUSDT
```

### Help
```bash
python check_lighter_positions.py --help
```

## Output Examples

### All Positions View
```
════════════════════════════════════════════════════════════════════════════════════════════════════
                                        LIGHTER POSITION CHECKER
════════════════════════════════════════════════════════════════════════════════════════════════════

Timestamp: 2025-10-13 14:30:45 UTC

Open Positions (2 found):

Symbol       Side     Size            Entry Price     Unrealized PnL
────────────────────────────────────────────────────────────────────────────────────────────────────
BTC          LONG     0.150000        $45000.0000         $150.2500
ETH          SHORT    2.500000        $2500.0000          -$25.7500
────────────────────────────────────────────────────────────────────────────────────────────────────

Total Unrealized PnL: $124.5000

Account Balance:
  Portfolio Value:    $5432.10
  Available Balance:  $3200.50
  Margin in Use:      $2231.60
```

### Specific Position View
```
────────────────────────────────────────────────────────────────────────────────────────────────────
Detailed Position: BTC

  Symbol:              BTC
  Side:                LONG
  Size:                0.150000 (signed: +0.150000)
  Entry Price:         $45000.0000
  Unrealized PnL:      $150.2500
  Leverage:            3.00x
  Margin Mode:         Cross
  Initial Margin %:    33.33%

────────────────────────────────────────────────────────────────────────────────────────────────────
```

## Environment Variables Required

Make sure your `.env` file contains:

```bash
# Lighter API Configuration
LIGHTER_BASE_URL=https://mainnet.zklighter.elliot.ai
LIGHTER_WS_URL=wss://mainnet.zklighter.elliot.ai/stream
API_KEY_PRIVATE_KEY=your_private_key_here
ACCOUNT_INDEX=0
API_KEY_INDEX=0
```

## How It Works

The script uses the Lighter Python SDK to:

1. **Connect to Lighter API** - Uses your account credentials from `.env`
2. **Fetch Account Data** - Calls the account API endpoint by account index
3. **Parse Positions** - Extracts position data including:
   - Symbol and market ID
   - Position size (signed: positive=long, negative=short)
   - Average entry price
   - Unrealized PnL
   - Leverage and margin mode
4. **Display Results** - Formats the data in an easy-to-read table

## API Reference

The script uses these functions from `lighter_client.py`:

- `get_all_lighter_positions()` - Fetches all non-zero positions
- `get_lighter_position_details()` - Gets detailed info for a specific market
- `get_lighter_market_details()` - Resolves symbol to market ID
- `get_lighter_balance()` - Fetches account balance via WebSocket

## Troubleshooting

### "No positions found"
- Check that you have open positions on Lighter
- Verify your ACCOUNT_INDEX is correct

### "Symbol not found"
- Lighter uses symbols without USDT suffix (e.g., "BTC" not "BTCUSDT")
- The script automatically strips USDT suffix if provided

### Connection errors
- Verify your LIGHTER_BASE_URL is correct
- Check your API_KEY_PRIVATE_KEY is valid
- Ensure you have internet connectivity

### Rate limit errors
- The script respects API rate limits
- Wait a minute before retrying if you hit limits

## Integration with Main Bot

This script is designed to work alongside `lighter_aster_hedge.py` and uses the same:
- Environment variables
- Helper functions from `lighter_client.py`
- Lighter SDK configuration

You can run it anytime to check positions without interfering with the bot.

## Technical Details

**API Endpoint Used**: `GET /api/v1/account`
- Documentation: https://apidocs.lighter.xyz/reference/account-1
- Retrieves account details by index or address
- Returns positions, balances, and margin info

**Position Data Structure**:
```python
{
    'symbol': 'BTC',
    'size': 0.15,  # Signed size (positive=long, negative=short)
    'entry_price': 45000.0,
    'unrealized_pnl': 150.25,
    'leverage': 3.0,
    'margin_mode': 0,  # 0=Cross, 1=Isolated
    'initial_margin_fraction': 0.3333
}
```

## License

Same as main project.
