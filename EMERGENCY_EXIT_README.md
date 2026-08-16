# Emergency Exit Script

A standalone script to quickly close any delta-neutral positions found on Lighter and Aster exchanges.

## Features

- ✅ Automatically detects positions on both exchanges
- ✅ Matches delta-neutral pairs (opposite positions on same symbol)
- ✅ Shows detailed position information with PnL for each leg
- ✅ Calculates and displays total unrealized PnL
- ✅ Color-coded PnL display (green for profit, red for loss)
- ✅ Requires manual confirmation before closing
- ✅ Closes positions on both exchanges simultaneously
- ✅ Verifies closure after execution
- ✅ Works independently from the main trading bot

## When to Use

Use this script in emergency situations when you need to quickly close all delta-neutral positions:

- **Market volatility**: Unexpected market movements requiring immediate exit
- **Bot malfunction**: Main bot not responding or stuck
- **Manual intervention**: Need to exit positions outside normal bot cycle
- **Risk management**: Want to reduce exposure immediately
- **Testing/verification**: Check if positions can be closed cleanly

## Usage

### Basic Usage
```bash
python emergency_exit.py
```

The script will:
1. Scan both exchanges for positions
2. Match delta-neutral pairs
3. Display detailed position and PnL information
4. Wait for your confirmation (press ENTER)
5. Close all matched positions
6. Verify closure

### Example Output

```
════════════════════════════════════════════════════════════════════════════════════════════════════
                            EMERGENCY EXIT - DELTA NEUTRAL POSITION CLOSER
════════════════════════════════════════════════════════════════════════════════════════════════════

Timestamp: 2025-10-13 16:45:30 UTC

Scanning for positions on both exchanges...

  Checking Aster...
    Found 2 position(s)
  Checking Lighter...
    Found 2 position(s)

Delta-Neutral Positions Found:

Symbol       Exchange   Side     Size            Entry Price     Unrealized PnL
────────────────────────────────────────────────────────────────────────────────────────────────────
BTCUSDT      Aster      LONG     0.150000        $45000.0000         $150.2500
             Lighter    SHORT    0.150000        $45050.0000        -$125.7500
             Pair Total                                              $24.5000
────────────────────────────────────────────────────────────────────────────────────────────────────
ETHUSDT      Aster      SHORT    2.500000        $2500.0000          -$35.2500
             Lighter    LONG     2.500000        $2495.0000           $47.5000
             Pair Total                                              $12.2500
────────────────────────────────────────────────────────────────────────────────────────────────────

Total Unrealized PnL: $36.7500

WARNING: This will close all delta-neutral positions shown above!
This action cannot be undone.

Press ENTER to proceed with closing, or Ctrl+C to cancel: _
```

After confirmation:

```
Closing positions on both exchanges...

Processing BTCUSDT...
  Closing Lighter SHORT position: 0.150000 BTC
  Closing Aster LONG position: 0.150000 BTC
  ✓ Close orders sent for BTCUSDT

Processing ETHUSDT...
  Closing Lighter LONG position: 2.500000 ETH
  Closing Aster SHORT position: 2.500000 ETH
  ✓ Close orders sent for ETHUSDT

Verifying closure...

  ✓ BTCUSDT: Fully closed on both exchanges
  ✓ ETHUSDT: Fully closed on both exchanges

Emergency exit complete!
```

## How It Works

### 1. Position Detection
The script uses the same APIs as the main bot to fetch positions:
- **Aster**: `get_perp_account_info()` to fetch all perpetual positions
- **Lighter**: `get_all_lighter_positions()` to fetch all non-zero positions

### 2. Delta-Neutral Matching
Positions are matched as delta-neutral pairs when:
- Same symbol (e.g., BTCUSDT)
- Opposite sides (one LONG, one SHORT)
- Both positions have non-zero size

### 3. PnL Calculation
- **Aster PnL**: Retrieved from `unrealizedProfit` field in account info
- **Lighter PnL**: Calculated from position data returned by API
- **Total PnL**: Sum of both legs across all pairs

### 4. Position Closing
Positions are closed using:
- **Lighter**: `lighter_close_position()` with aggressive IOC orders
- **Aster**: `close_perp_position()` with reduce-only market orders

Both exchanges are called concurrently for fastest execution.

## Environment Variables Required

The script uses the same `.env` file as the main bot:

```bash
# Aster API Configuration
ASTER_API_USER=your_user_address
ASTER_API_SIGNER=your_signer_address
ASTER_API_PRIVATE_KEY=your_private_key
ASTER_APIV1_PUBLIC=your_public_key
ASTER_APIV1_PRIVATE=your_private_key

# Lighter API Configuration
LIGHTER_BASE_URL=https://mainnet.zklighter.elliot.ai
LIGHTER_WS_URL=wss://mainnet.zklighter.elliot.ai/stream
API_KEY_PRIVATE_KEY=your_lighter_private_key
ACCOUNT_INDEX=0
API_KEY_INDEX=0
```

## Safety Features

### Confirmation Required
- Script will **NEVER** close positions without explicit confirmation
- Press ENTER to confirm, or Ctrl+C to cancel
- Displays all positions and PnL before asking for confirmation

### Position Verification
- Verifies closure after sending close orders
- Shows which positions closed successfully
- Warns if any positions remain open

### Error Handling
- Catches and displays errors for each position
- Continues closing other positions even if one fails
- Shows detailed error messages

### Delta-Neutral Only
- **Only closes matched delta-neutral pairs**
- Ignores single-sided positions
- Prevents accidentally closing hedges incorrectly

## Troubleshooting

### "No delta-neutral positions found"
- Check that you have positions on both exchanges
- Verify positions are on the same symbol
- Ensure positions are opposite sides (LONG vs SHORT)

### "No reference price for Lighter"
- Order book may be empty for that symbol
- Try again in a few seconds
- Check if the market is active

### "Partially closed" warning
- One exchange closed successfully, other didn't
- Check positions manually on both exchanges
- May need to close remaining position manually

### Connection errors
- Verify your API credentials in `.env`
- Check internet connectivity
- Ensure API endpoints are accessible

### Rate limit errors
- Wait a minute before retrying
- The script uses the same rate limiting as main bot

## Comparison with Main Bot

| Feature | Main Bot | Emergency Exit |
|---------|----------|----------------|
| Opens positions | ✓ | ✗ |
| Closes positions | ✓ (scheduled) | ✓ (immediate) |
| PnL tracking | ✓ | ✓ (display only) |
| Requires confirmation | ✗ | ✓ |
| Works with bot state | ✓ | ✗ |
| Can interrupt bot | ✗ | ✓ |

**Note**: The emergency exit script is completely independent from the main bot. You can run it while the bot is running, but be aware that:
- The bot may try to open new positions after you close them
- Consider stopping the bot before using emergency exit

## License

Same as main project.
