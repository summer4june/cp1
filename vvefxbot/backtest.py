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
    "D1":  mt5.TIMEFRAME_D1,
}


def _fetch_from_mt5(symbol: str, timeframe: str, date_from: datetime, date_to: datetime, offset_hours: float = 0.0) -> pd.DataFrame:
    """
    Pull historical OHLCV bars from the connected MT5 terminal.

    Args:
        symbol (str): Trading pair, e.g. 'EURUSD'.
        timeframe (str): 'M1', 'M15', or 'H1'.
        date_from (datetime): UTC start datetime.
        date_to (datetime): UTC end datetime.
        offset_hours (float): Timezone offset of the broker server relative to UTC.

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
    
    # Adjust query times to the broker's timezone to ensure we fetch the correct range
    from datetime import timedelta
    date_from_adj = date_from + timedelta(hours=offset_hours)
    date_to_adj = date_to + timedelta(hours=offset_hours)

    # MT5 often requires naive datetime objects (no timezone)
    dt_from = date_from_adj.replace(tzinfo=None) if date_from_adj.tzinfo else date_from_adj
    dt_to = date_to_adj.replace(tzinfo=None) if date_to_adj.tzinfo else date_to_adj
    
    rates = mt5.copy_rates_range(symbol, tf, dt_from, dt_to)

    if rates is None or len(rates) < 10:
        err = mt5.last_error()
        count = len(rates) if rates is not None else 0
        raise RuntimeError(
            f"Insufficient data for {symbol} {timeframe} (Fetched only {count} bars).\n"
            f"Error: {err}\n\n"
            f"CRITICAL: MT5 often doesn't have M1 history loaded by default. To fix this:\n"
            f"1. Open a chart for {symbol} on M1 timeframe.\n"
            f"2. Press 'Home' on your keyboard repeatedly to scroll back to {dt_from.year}.\n"
            f"3. Alternatively, go to Tools -> History Center -> {symbol} -> M1 and click 'Download'.\n"
            f"4. Check MT5 -> Tools -> Options -> Charts -> 'Max bars in chart' is set to 'Unlimited'."
        )

    df = pd.DataFrame(rates)
    # Convert broker local times to actual UTC timestamps
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True) - pd.to_timedelta(offset_hours, unit="h")
    df = df[["time", "open", "high", "low", "close", "tick_volume"]].copy()
    df.sort_values("time", inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info(f"  ✓ {symbol} {timeframe}: {len(df)} bars fetched.")
    return df


def fetch_all_timeframes(symbol: str, date_from: datetime, date_to: datetime, offset_hours: float = 0.0) -> dict:
    """Fetch M1, M15, H1, D1 from MT5 for one symbol."""
    from datetime import timedelta
    # Fetch D1 starting 60 days before date_from to ensure enough history for D1 bias range
    d1_start = date_from - timedelta(days=60)
    return {
        "M1":  _fetch_from_mt5(symbol, "M1",  date_from, date_to, offset_hours),
        "M15": _fetch_from_mt5(symbol, "M15", date_from, date_to, offset_hours),
        "H1":  _fetch_from_mt5(symbol, "H1",  date_from, date_to, offset_hours),
        "D1":  _fetch_from_mt5(symbol, "D1",  d1_start, date_to, offset_hours),
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
                  date_from: datetime, date_to: datetime, offset_hours: float = 0.0) -> dict:
    """
    Load M1/M15/H1/D1 OHLCV from CSV files in csv_dir.
    Expected filenames: EURUSD_M1.csv, EURUSD_M15.csv, EURUSD_H1.csv, EURUSD_D1.csv
    M15, H1, and D1 are auto-derived from M1 if their files are missing.
    """
    from datetime import timedelta
    def _load(tf: str) -> pd.DataFrame:
        path = os.path.join(csv_dir, f"{symbol}_{tf}.csv")
        if not os.path.exists(path):
            return pd.DataFrame()
        logger.info(f"Loading CSV: {path}")
        df = _parse_csv(path)
        if not df.empty and offset_hours != 0.0:
            df["time"] = df["time"] - pd.to_timedelta(offset_hours, unit="h")
        return df

    m1 = _load("M1")
    if m1.empty:
        raise FileNotFoundError(
            f"Required file not found: {csv_dir}/{symbol}_M1.csv"
        )

    m15 = _load("M15")
    h1  = _load("H1")
    d1  = _load("D1")

    if m15.empty:
        logger.warning(f"{symbol} M15 CSV not found — deriving from M1.")
        m15 = _resample(m1, "15min")
    if h1.empty:
        logger.warning(f"{symbol} H1 CSV not found — deriving from M1.")
        h1 = _resample(m1, "1h")
    if d1.empty:
        logger.warning(f"{symbol} D1 CSV not found — deriving from M1.")
        d1 = _resample(m1, "1D")

    # Filter M1, M15, H1 to requested date range
    m1_filtered  = m1[(m1["time"] >= date_from)  & (m1["time"] <= date_to)].reset_index(drop=True)
    m15_filtered = m15[(m15["time"] >= date_from) & (m15["time"] <= date_to)].reset_index(drop=True)
    h1_filtered  = h1[(h1["time"] >= date_from)  & (h1["time"] <= date_to)].reset_index(drop=True)

    # Filter D1 starting 60 days before date_from to ensure historical range bias checks succeed
    d1_start = date_from - timedelta(days=60)
    d1_filtered  = d1[(d1["time"] >= d1_start) & (d1["time"] <= date_to)].reset_index(drop=True)

    logger.info(f"{symbol} CSV loaded: M1={len(m1_filtered)} | M15={len(m15_filtered)} | H1={len(h1_filtered)} | D1={len(d1_filtered)} bars")
    return {"M1": m1_filtered, "M15": m15_filtered, "H1": h1_filtered, "D1": d1_filtered}


# ──────────────────────────────────────────────────────────────────────
# REPORT GENERATION
# ──────────────────────────────────────────────────────────────────────

def _pip_size_for_pair(pair: str) -> float:
    """Return correct pip size (0.01 for JPY/XAU/XAG, 0.0001 otherwise)."""
    p = pair.upper()
    if "JPY" in p or "XAU" in p or "XAG" in p:
        return 0.01
    return 0.0001


def generate_report(all_trades: list, bt_config: dict) -> None:
    """Print summary and save CSV for all trades across all pairs."""
    date_from = bt_config["date_from"]
    date_to   = bt_config["date_to"]
    strategy_label = bt_config.get("strategy", "MMXM").upper()

    if not all_trades:
        print("\n⚠️  No trades generated. Try a longer date range or lower aplus_threshold in config.json.\n")
        return

    df = pd.DataFrame(all_trades)

    # ── Add pip-correct SL/TP distance columns ───────────────────────────
    # The raw price columns (entry_price, sl_price, tp1_price, tp2_price)
    # are already in to_dict(). Compute pip distances using correct pip size.
    def _add_pip_cols(row):
        ps    = _pip_size_for_pair(row["pair"])
        entry = float(row.get("entry_price", row.get("entry", 0)))
        sl    = float(row.get("sl_price",    row.get("sl",    0)))
        tp1   = float(row.get("tp1_price",   row.get("tp1",   0)))
        tp2   = float(row.get("tp2_price",   row.get("tp2",   0)))
        return pd.Series({
            "sl_pips":  round(abs(entry - sl)  / ps, 1),
            "tp1_pips": round(abs(entry - tp1) / ps, 1),
            "tp2_pips": round(abs(entry - tp2) / ps, 1),
        })

    pip_cols = df.apply(_add_pip_cols, axis=1)
    df = pd.concat([df, pip_cols], axis=1)

    # ── Aggregate stats ──────────────────────────────────────────────────
    total      = len(df)
    wins       = len(df[df["result"] == "WIN"])
    losses     = len(df[df["result"] == "LOSS"])
    breakevens = len(df[df["result"] == "BREAKEVEN"])
    tp2_hits   = len(df[df["exit_reason"] == "TP2_HIT"])
    win_rate   = round(wins / total * 100, 1) if total else 0.0
    net_pnl    = round(df["profit_usd"].sum(), 2)
    avg_win    = round(df[df["result"] == "WIN"]["profit_usd"].mean(),  2) if wins   else 0.0
    avg_loss   = round(df[df["result"] == "LOSS"]["profit_usd"].mean(), 2) if losses else 0.0

    gross_profit = df[df["profit_usd"] > 0]["profit_usd"].sum()
    gross_loss   = abs(df[df["profit_usd"] < 0]["profit_usd"].sum())
    pf = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf")

    cumulative  = df["profit_usd"].cumsum()
    max_dd      = round((cumulative - cumulative.cummax()).min(), 2)

    pairs_tested = ", ".join(sorted(df["pair"].unique()))

    print()
    print("=" * 70)
    print(f"  VvE FxBOT — {strategy_label} BACKTEST REPORT")
    print(f"  Pairs  : {pairs_tested}")
    print(f"  Period : {date_from}  →  {date_to}")
    print("=" * 70)
    print(f"  Total Trades   : {total}")
    print(f"  Wins (TP2 Hit) : {wins}  ({win_rate}%)  |  TP2 Hit: {tp2_hits}  ({round(tp2_hits/total*100,1) if total else 0}%)")
    print(f"  Losses         : {losses}")
    print(f"  Breakeven      : {breakevens}")
    print(f"  Net P&L        : ${net_pnl:+.2f}")
    print(f"  Avg Win        : ${avg_win:+.2f}")
    print(f"  Avg Loss       : ${avg_loss:+.2f}")
    print(f"  Profit Factor  : {pf}")
    print(f"  Max Drawdown   : ${max_dd:.2f}")
    print("=" * 70)
    print()

    # Per-pair breakdown with TP2 hit rate
    print(f"  {'Pair':<10}  {'Trades':>6}  {'Win%':>6}  {'TP2%':>6}  {'WIN':>5}  {'LOSS':>5}  {'BE':>5}  {'P&L':>9}  {'SL(pips)':>9}")
    print("  " + "-" * 73)
    for pair, grp in df.groupby("pair"):
        n   = len(grp)
        w   = len(grp[grp["result"] == "WIN"])
        l   = len(grp[grp["result"] == "LOSS"])
        be  = len(grp[grp["result"] == "BREAKEVEN"])
        tp2 = len(grp[grp["exit_reason"] == "TP2_HIT"])
        pnl = round(grp["profit_usd"].sum(), 2)
        wr  = round(w  / n * 100, 1) if n else 0
        tp2r = round(tp2 / n * 100, 1) if n else 0
        sl_mean = round(grp["sl_pips"].mean(), 1) if "sl_pips" in grp else 0
        print(f"  {pair:<10}  {n:>6}  {wr:>5.1f}%  {tp2r:>5.1f}%  {w:>5}  {l:>5}  {be:>5}  ${pnl:>+8.2f}  {sl_mean:>9.1f}")
    print()

    # Trade table
    print(df[["open_time", "pair", "direction", "year", "session", "entry_leg", "entry_price", "lot",
               "sl_pips", "tp2_pips", "exit_reason", "result", "profit_usd"]].to_string(index=False))
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

    # Load broker timezone offset
    offset_hours = float(bt_config.get("broker_timezone_offset_hours", 0.0))
    if offset_hours != 0.0:
        logger.info(f"Applying broker server timezone offset of {offset_hours} hours to shift data to UTC.")

    # Strategy to backtest (default MMXM for backward-compatibility)
    strategy_name = bt_config.get("strategy", "MMXM").upper()
    valid_strategies = ["MMXM", "OTE", "ZGMT"]
    if strategy_name not in valid_strategies:
        print(f"❌ Unknown strategy: '{strategy_name}'. Valid options: {', '.join(valid_strategies)}")
        sys.exit(1)

    logger.info(f"Backtesting strategy: {strategy_name}")

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
                    data = fetch_all_timeframes(pair, date_from, date_to, offset_hours)
                else:  # csv
                    data = load_from_csv(csv_dir, pair, date_from, date_to, offset_hours)
            except (RuntimeError, FileNotFoundError) as e:
                logger.error(str(e))
                print(f"⚠️  Skipping {pair}: {e}")
                continue

            connector = BacktestConnector(config, data, pair)

            # ── Instantiate the chosen scanner ───────────────────────────
            from core.stateengine import StateEngine as _SE
            _bt_state = _SE(":memory:")

            if strategy_name == "MMXM":
                from modules.scannermmxm import ScannerMMXM
                scanner = ScannerMMXM(config, connector, _bt_state)
            elif strategy_name == "OTE":
                from modules.scannerote import ScannerOTE
                # Ensure OTE is enabled for the backtest run
                if isinstance(config.ote_scanner, dict):
                    config.ote_scanner["enabled"] = True
                scanner = ScannerOTE(config, connector, _bt_state)
            elif strategy_name == "ZGMT":
                from modules.scannerzgmt import ScannerZGMT
                # Ensure ZGMT is enabled for the backtest run
                if isinstance(config.zgmt_scanner, dict):
                    config.zgmt_scanner["enabled"] = True
                scanner = ScannerZGMT(config, connector, _bt_state)
            else:
                from modules.scannermmxm import ScannerMMXM
                scanner = ScannerMMXM(config, connector, _bt_state)

            engine = BacktestEngine(config, connector, pair, scanner=scanner)
            trades = engine.run()
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
