"""
backtest.py — VvE FxBOT Backtest Runner

Reads backtest_config.json for pair list, date range, and data source.

data_source.mode options:
  "mt5"  — auto-fetch history directly from MT5 (MT5 must be open)
  "csv"  — load from CSV files in data_source.csv_dir folder
            Files must be named: EURUSD_M1.csv, EURUSD_M15.csv, EURUSD_H1.csv

Usage:
    python backtest.py
    python backtest.py --config backtest_config.json
"""

import os
import sys
import json
import argparse
import pandas as pd
from datetime import datetime, timezone

import MetaTrader5 as mt5

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.configengine import ConfigEngine
from core.logger import get_logger
from backtest.connector import BacktestConnector
from backtest.engine import BacktestEngine

logger = get_logger("Backtest")


# ──────────────────────────────────────────────────────────────────────
# MT5 DATA FETCHER
# ──────────────────────────────────────────────────────────────────────

_TF_MAP = {
    "M1":  mt5.TIMEFRAME_M1,
    "M15": mt5.TIMEFRAME_M15,
    "H1":  mt5.TIMEFRAME_H1,
}


def _fetch_from_mt5(symbol: str, timeframe: str, date_from: datetime, date_to: datetime) -> pd.DataFrame:
    """
    Pull historical OHLCV bars from the connected MT5 terminal.

    Args:
        symbol (str): Trading pair, e.g. 'EURUSD'.
        timeframe (str): 'M1', 'M15', or 'H1'.
        date_from (datetime): UTC start datetime.
        date_to (datetime): UTC end datetime.

    Returns:
        pd.DataFrame with columns: time, open, high, low, close, tick_volume
    """
    tf = _TF_MAP.get(timeframe)
    if tf is None:
        raise ValueError(f"Unsupported timeframe: {timeframe}")

    # Ensure symbol is selected in Market Watch
    if not mt5.symbol_select(symbol, True):
        raise RuntimeError(f"Symbol {symbol} not found or could not be selected in MT5.")

    logger.info(f"Fetching {symbol} {timeframe} from MT5: {date_from.date()} → {date_to.date()}")
    
    # MT5 often requires naive datetime objects (no timezone)
    dt_from = date_from.replace(tzinfo=None) if date_from.tzinfo else date_from
    dt_to = date_to.replace(tzinfo=None) if date_to.tzinfo else date_to
    
    rates = mt5.copy_rates_range(symbol, tf, dt_from, dt_to)

    if rates is None or len(rates) == 0:
        err = mt5.last_error()
        raise RuntimeError(
            f"MT5 returned no data for {symbol} {timeframe}. "
            f"Error: {err}. \n   Potential Fixes:\n"
            f"   1. Ensure MT5 is open and logged in.\n"
            f"   2. Check MT5 -> Tools -> Options -> Charts -> 'Max bars in chart' (Set to 'Unlimited').\n"
            f"   3. Ensure the symbol {symbol} exists in your broker's Market Watch."
        )

    df = pd.DataFrame(rates)
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df = df[["time", "open", "high", "low", "close", "tick_volume"]].copy()
    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info(f"  ✓ {symbol} {timeframe}: {len(df)} bars fetched.")
    return df


def fetch_all_timeframes(symbol: str, date_from: datetime, date_to: datetime) -> dict:
    """Fetch M1, M15, H1 from MT5 for one symbol."""
    return {
        "M1":  _fetch_from_mt5(symbol, "M1",  date_from, date_to),
        "M15": _fetch_from_mt5(symbol, "M15", date_from, date_to),
        "H1":  _fetch_from_mt5(symbol, "H1",  date_from, date_to),
    }


# ──────────────────────────────────────────────────────────────────────
# CSV DATA LOADER
# ──────────────────────────────────────────────────────────────────────

def _parse_csv(path: str) -> pd.DataFrame:
    """
    Parse an OHLCV CSV file.
    Supports MT5 tab-export and standard comma-separated CSV.
    """
    with open(path, "r") as f:
        first_line = f.readline()
    sep = "\t" if "\t" in first_line else ","

    df = pd.read_csv(path, sep=sep, header=0)
    df.columns = [c.strip().lstrip("<").rstrip(">").lower() for c in df.columns]

    rename_map = {"tickvol": "tick_volume", "tick_vol": "tick_volume",
                  "vol": "tick_volume", "volume": "tick_volume"}
    df.rename(columns=rename_map, inplace=True)

    if "date" in df.columns and "time" in df.columns:
        df["time"] = pd.to_datetime(
            df["date"].astype(str) + " " + df["time"].astype(str)
        )
        df.drop(columns=["date"], inplace=True)
    elif "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"])
    else:
        raise ValueError(f"Cannot parse datetime from columns: {list(df.columns)}")

    if df["time"].dt.tz is None:
        df["time"] = df["time"].dt.tz_localize("UTC")

    if "tick_volume" not in df.columns:
        df["tick_volume"] = 0

    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df[["time", "open", "high", "low", "close", "tick_volume"]]


def _resample(m1_df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """Resample M1 to a higher timeframe."""
    df = m1_df.set_index("time")
    r = df.resample(rule).agg(
        {"open": "first", "high": "max", "low": "min",
         "close": "last", "tick_volume": "sum"}
    ).dropna().reset_index()
    return r


def load_from_csv(csv_dir: str, symbol: str,
                  date_from: datetime, date_to: datetime) -> dict:
    """
    Load M1/M15/H1 OHLCV from CSV files in csv_dir.
    Expected filenames: EURUSD_M1.csv, EURUSD_M15.csv, EURUSD_H1.csv
    M15 and H1 are auto-derived from M1 if their files are missing.
    """
    def _load(tf: str) -> pd.DataFrame:
        path = os.path.join(csv_dir, f"{symbol}_{tf}.csv")
        if not os.path.exists(path):
            return pd.DataFrame()
        logger.info(f"Loading CSV: {path}")
        return _parse_csv(path)

    m1 = _load("M1")
    if m1.empty:
        raise FileNotFoundError(
            f"Required file not found: {csv_dir}/{symbol}_M1.csv"
        )

    m15 = _load("M15")
    h1  = _load("H1")

    if m15.empty:
        logger.warning(f"{symbol} M15 CSV not found — deriving from M1.")
        m15 = _resample(m1, "15min")
    if h1.empty:
        logger.warning(f"{symbol} H1 CSV not found — deriving from M1.")
        h1 = _resample(m1, "1h")

    # Apply date filter
    for df in [m1, m15, h1]:
        pass  # filtered below
    m1  = m1[(m1["time"] >= date_from)  & (m1["time"] <= date_to)].reset_index(drop=True)
    m15 = m15[(m15["time"] >= date_from) & (m15["time"] <= date_to)].reset_index(drop=True)
    h1  = h1[(h1["time"] >= date_from)  & (h1["time"] <= date_to)].reset_index(drop=True)

    logger.info(f"{symbol} CSV loaded: M1={len(m1)} | M15={len(m15)} | H1={len(h1)} bars")
    return {"M1": m1, "M15": m15, "H1": h1}


# ──────────────────────────────────────────────────────────────────────
# REPORT GENERATION
# ──────────────────────────────────────────────────────────────────────

def generate_report(all_trades: list, bt_config: dict) -> None:
    """Print summary and save CSV for all trades across all pairs."""
    date_from = bt_config["date_from"]
    date_to   = bt_config["date_to"]

    if not all_trades:
        print("\n⚠️  No trades generated. Try a longer date range or lower aplus_threshold in config.json.\n")
        return

    df = pd.DataFrame(all_trades)
    total      = len(df)
    wins       = len(df[df["result"] == "WIN"])
    losses     = len(df[df["result"] == "LOSS"])
    breakevens = len(df[df["result"] == "BREAKEVEN"])
    win_rate   = round(wins / total * 100, 1) if total else 0.0
    net_pnl    = round(df["profit_usd"].sum(), 2)
    avg_win    = round(df[df["result"] == "WIN"]["profit_usd"].mean(), 2) if wins else 0.0
    avg_loss   = round(df[df["result"] == "LOSS"]["profit_usd"].mean(), 2) if losses else 0.0

    gross_profit = df[df["profit_usd"] > 0]["profit_usd"].sum()
    gross_loss   = abs(df[df["profit_usd"] < 0]["profit_usd"].sum())
    pf = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf")

    cumulative  = df["profit_usd"].cumsum()
    max_dd      = round((cumulative - cumulative.cummax()).min(), 2)

    pairs_tested = ", ".join(sorted(df["pair"].unique()))

    print()
    print("=" * 65)
    print("  VvE FxBOT — MMXM BACKTEST REPORT")
    print(f"  Pairs  : {pairs_tested}")
    print(f"  Period : {date_from}  →  {date_to}")
    print("=" * 65)
    print(f"  Total Trades   : {total}")
    print(f"  Wins           : {wins}  ({win_rate}%)")
    print(f"  Losses         : {losses}")
    print(f"  Breakeven      : {breakevens}")
    print(f"  Net P&L        : ${net_pnl:+.2f}")
    print(f"  Avg Win        : ${avg_win:+.2f}")
    print(f"  Avg Loss       : ${avg_loss:+.2f}")
    print(f"  Profit Factor  : {pf}")
    print(f"  Max Drawdown   : ${max_dd:.2f}")
    print("=" * 65)
    print()

    # Per-pair breakdown
    for pair, grp in df.groupby("pair"):
        w = len(grp[grp["result"] == "WIN"])
        l = len(grp[grp["result"] == "LOSS"])
        pnl = round(grp["profit_usd"].sum(), 2)
        wr  = round(w / len(grp) * 100, 1) if len(grp) else 0
        print(f"  {pair:<10}  {len(grp):>3} trades | Win: {wr}% | P&L: ${pnl:+.2f}")
    print()

    # Trade table
    print(df[["open_time", "pair", "direction", "entry", "lot",
               "exit_reason", "result", "profit_usd"]].to_string(index=False))
    print()

    # Save CSV
    out_dir = bt_config.get("report", {}).get("output_dir", "backtest/results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"backtest_{date_from}_{date_to}.csv")
    if bt_config.get("report", {}).get("save_csv", True):
        df.to_csv(out_path, index=False)
        print(f"  ✅ Full results saved: {out_path}\n")


# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="VvE FxBOT MMXM Backtest")
    parser.add_argument(
        "--config", default="backtest_config.json",
        help="Path to backtest config (default: backtest_config.json)"
    )
    args = parser.parse_args()

    # ── Load backtest config ──────────────────────────────────────────
    if not os.path.exists(args.config):
        print(f"❌ Backtest config not found: {args.config}")
        sys.exit(1)

    with open(args.config) as f:
        bt_config = json.load(f)

    pairs     = bt_config["pairs"]
    date_from = datetime.fromisoformat(bt_config["date_from"]).replace(tzinfo=timezone.utc)
    date_to   = datetime.fromisoformat(bt_config["date_to"]).replace(tzinfo=timezone.utc)

    # ── Load bot config ───────────────────────────────────────────────
    config_engine = ConfigEngine("config.json")
    config        = config_engine.get_config()

    # Override risk from backtest config if specified
    if "risk_percent" in bt_config:
        config.risk_percent = float(bt_config["risk_percent"])
    if "initial_balance" in bt_config:
        config.trading_pool_size = float(bt_config["initial_balance"])

    # ── Determine data source mode + connect if needed ───────────────
    ds_cfg  = bt_config.get("data_source", {})
    ds_mode = ds_cfg.get("mode", "mt5").lower()
    csv_dir = ds_cfg.get("csv_dir", "backtest/data")

    mt5_connected = False

    if ds_mode == "mt5":
        logger.info("Data source: MT5 (auto-fetch). Connecting...")
        if not mt5.initialize():
            print(f"❌ MT5 initialization failed: {mt5.last_error()}")
            print("   Make sure MetaTrader 5 is open on your computer.")
            sys.exit(1)
        authorized = mt5.login(
            login=config.mt5_login,
            password=config.mt5_password,
            server=config.mt5_server,
        )
        if not authorized:
            print(f"❌ MT5 login failed: {mt5.last_error()}")
            mt5.shutdown()
            sys.exit(1)
        mt5_connected = True
        logger.info(f"MT5 connected. Will fetch: {', '.join(pairs)}")

    elif ds_mode == "csv":
        logger.info(f"Data source: CSV files from '{csv_dir}'")
        if not os.path.isdir(csv_dir):
            print(f"❌ CSV directory not found: {csv_dir}")
            print(f"   Create the folder and place CSV files named like: EURUSD_M1.csv")
            sys.exit(1)

    else:
        print(f"❌ Unknown data_source.mode: '{ds_mode}'. Use 'mt5' or 'csv'.")
        sys.exit(1)

    # ── Run backtest per pair ─────────────────────────────────────────
    all_trades = []
    try:
        for pair in pairs:
            logger.info(f"\n{'='*50}\nBacktesting {pair}\n{'='*50}")
            try:
                if ds_mode == "mt5":
                    data = fetch_all_timeframes(pair, date_from, date_to)
                else:  # csv
                    data = load_from_csv(csv_dir, pair, date_from, date_to)
            except (RuntimeError, FileNotFoundError) as e:
                logger.error(str(e))
                print(f"⚠️  Skipping {pair}: {e}")
                continue

            connector = BacktestConnector(config, data, pair)
            engine    = BacktestEngine(config, connector, pair)
            trades    = engine.run()
            all_trades.extend(trades)
            logger.info(f"{pair}: {len(trades)} trades simulated.")

    finally:
        if mt5_connected:
            mt5.shutdown()
            logger.info("MT5 disconnected.")

    # ── Generate report ───────────────────────────────────────────────
    generate_report(all_trades, bt_config)


if __name__ == "__main__":
    main()
