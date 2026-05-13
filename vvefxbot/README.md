# VvE FxBOT — Production ICT MMXM Trading Bot

VvE FxBOT is a production-grade, semi-automated Forex trading bot built in Python.
It detects ICT MMXM (Market Maker Buy/Sell Model) setups across configurable pairs and
sessions, gates every signal through risk and correlation checks, then routes execution
approval through Telegram before placing orders on MetaTrader 5.

---

## Folder Structure

```
vvefxbot/
├── core/
│   ├── configengine.py      # Config loader & validator (config.json + .env)
│   ├── logger.py            # Structured logger with daily rotation & .env masking
│   ├── mt5connector.py      # MetaTrader 5 connection & order management
│   └── stateengine.py       # SQLite persistence (signals, trades, state)
├── modules/
│   ├── sessionengine.py     # IST session & killzone gating
│   ├── scannermmxm.py       # 5-step ICT MMXM signal detection (M15 + M1)
│   ├── riskengine.py        # Lot sizing, RR checks, slippage, portfolio exposure
│   ├── executionengine.py   # 13-step trade execution flow (triggered by Telegram YES)
│   ├── trademanager.py      # Background TP1/BE/TP2 trade monitor
│   ├── telegrambridge.py    # Signal delivery & YES/NO approval via Telegram
│   ├── correlationfilter.py # Group-based correlation trade gating
│   └── reportgoogle.py      # Google Sheets trade logging
├── db/                      # SQLite database (fxbot.db created at runtime)
├── logs/                    # Rotating log files (bot.log)
├── config.json              # Bot configuration
├── .env.example             # Environment variable template
├── requirements.txt         # Python dependencies
├── main.py                  # System orchestrator entry point
└── README.md
```

---

## Prerequisites

- **Python 3.10+** (3.11 recommended)
- **MetaTrader 5 terminal** installed and logged in (Windows or Linux via Wine)
- **A Telegram bot** created via [@BotFather](https://t.me/BotFather)
- **A Google Cloud service account** with Sheets API enabled and a credentials JSON file
- **A Google Sheet** shared with the service account email

---

## Installation

```bash
# 1. Clone or unzip the project
cd vvefxbot

# 2. Create and activate a virtual environment (recommended)
python -m venv vvefxbot_env
source vvefxbot_env/bin/activate   # Linux/macOS
vvefxbot_env\Scripts\activate      # Windows

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Environment Setup (.env)

Copy `.env.example` to `.env` and fill in your real values:

```bash
cp .env.example .env
```

Edit `.env`:

```env
MT5_LOGIN=your_mt5_account_number
MT5_PASSWORD=your_mt5_password
MT5_SERVER=your_broker_server_name
TELEGRAM_TOKEN=your_telegram_bot_token
TELEGRAM_CHAT_ID=your_telegram_chat_id
GOOGLE_SHEET_ID=your_google_spreadsheet_id
GOOGLE_CREDS_PATH=path/to/your/service_account_credentials.json
```

> ⚠️ Never commit `.env` to version control. It is already in `.gitignore`.

---

## Configuration (config.json)

Key settings to review before running:

| Key | Default | Description |
|---|---|---|
| `pairs` | `["EURUSD"]` | Symbols to trade |
| `risk_percent` | `1.0` | Risk per trade as % of pool |
| `trading_pool_size` | `1000.0` | Capital pool used for lot sizing |
| `demo_mode` | `true` | **Phase 1: always `true`** |
| `max_trades_day` | `10` | Maximum trades per day |
| `scan_frequency_seconds` | `10` | How often pairs are scanned |
| `effective_rr_min` | `2.0` | Minimum effective RR to accept a signal |

> ℹ️ **Phase 1 runs in demo mode.** `demo_mode: true` means no changes to live accounts. Set to `false` only when you are ready for live trading (see Phase 2 note below).

---

## How to Run

```bash
python main.py
```

---

## Startup Sequence

When `python main.py` is executed, the bot:

1. **Loads and validates** `config.json` and `.env` — raises an error immediately if any required field is missing.
2. **Opens the SQLite database** (`db/fxbot.db`) and creates all 7 tables if they don't exist.
3. **Connects to MetaTrader 5** using credentials from `.env`. Logs the account number on success.
4. **Starts the session engine** to track Asia / London / New York killzones in IST.
5. **Connects to Google Sheets** for trade reporting (non-fatal if it fails).
6. **Starts three background daemon threads:**
   - **Heartbeat** (every 60 s) — logs MT5 connection status and balance; reconnects if needed.
   - **Session monitor** (every 60 s) — logs active session, killzone, and avoid-window status.
   - **Trade monitor** (every 5 s) — manages open trades: TP1 → SL to breakeven → TP2 close.
7. **Starts the Telegram polling listener** — waits for YES/NO button presses from you.
8. **Enters the main scan loop** — every 10 seconds, all allowed pairs are scanned concurrently for A+ MMXM signals.

---

## Approving / Rejecting Signals via Telegram

When the scanner detects an A+ signal (score ≥ 85), a formatted message is sent to your Telegram chat:

```
🤖 VvE FxBOT — A+ SIGNAL DETECTED

📊 Pair: EURUSD
🕐 Session: London
📈 Direction: BUY
⚡ Setup: ICT MMXM

Entry: 1.08450
SL: 1.08200 (25.0 pips)
TP1: 1.08700 (1R)
TP2: 1.08950 (2R)

Risk: 1.0%
Lot: 0.04
Spread: 1.2 pips
Eff. RR: 2.04
Score: 92/100

🕐 2026-05-13 14:30:00 IST
Signal ID: xxxxxxxx-...
```

**Buttons:**
- **✅ YES EXECUTE** → Places the order immediately on MT5 (valid for 15 minutes)
- **❌ NO SKIP** → Prompts you to select a skip reason:
  `Spread High | News Window | Weak Displacement | Fake Sweep | Late Entry | Bad Session | Structure Unclear | Manual Reject`

All decisions are logged to the database.

---

## Reading Logs

Logs are written to `logs/bot.log` with daily rotation (30 days kept).

```
[2026-05-13 14:30:01] [INFO] [ScannerMMXM] [EURUSD] A+ SIGNAL | BUY | Score: 92 ...
[2026-05-13 14:30:02] [INFO] [TelegramBridge] Signal sent to Telegram: xxxxxxxx-...
[2026-05-13 14:31:00] [INFO] [Main] HEARTBEAT | MT5: connected | Balance: 1000.0
```

Log format: `[TIMESTAMP] [LEVEL] [MODULE] message`

> 🔒 Credentials from `.env` are **never** written to logs — they are masked automatically.

Tail the live log:
```bash
tail -f logs/bot.log
```

---

## Phase 2 Note

> When you are ready for live trading:
> 1. Set `"demo_mode": false` in `config.json`
> 2. Update `"trading_pool_size"` to your actual live capital pool
> 3. Review `"risk_percent"` and `"spread_limits"` for your broker
> 4. Ensure your MT5 terminal is connected to a **live account**
>
> Phase 2 will introduce automated session-based pair expansion, advanced trailing management, and enhanced reporting.

---

## License

Private. VvE FxBOT is proprietary software. Do not distribute.
