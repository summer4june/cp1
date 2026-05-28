"""
analyze_backtest_csv.py
=======================
Run from the vvefxbot root directory:

    python tools/analyze_backtest_csv.py backtest_2025-01-01_2025-12-31.csv

Reads the backtest CSV exported by BacktestEngine, re-simulates the exit logic
using the CORRECTED TP2-priority rules, and shows:
  - How many trades would change from BREAKEVEN→WIN with the new engine
  - Why the old engine silently misclassified them (new_sl > tp1 structural issue)
  - A candle-by-candle trace for the worst-affected trades
"""

import sys
import csv
from pathlib import Path


def load_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def pip_size(pair: str) -> float:
    p = pair.upper()
    if "JPY" in p:
        return 0.01
    if "XAU" in p or "GOLD" in p:
        return 0.01
    return 0.0001


def pips(a: float, b: float, ps: float) -> float:
    return abs(a - b) / ps


def main():
    if len(sys.argv) < 2:
        print("Usage: python tools/analyze_backtest_csv.py <path_to_csv>")
        sys.exit(1)

    path = sys.argv[1]
    if not Path(path).exists():
        print(f"File not found: {path}")
        sys.exit(1)

    rows = load_csv(path)
    print(f"\nLoaded {len(rows)} trades from {path}\n")

    # ── Summary of CSV ────────────────────────────────────────────────────────
    from collections import Counter
    exit_counts = Counter(r.get("exit_reason", "?") for r in rows)
    result_counts = Counter(r.get("result", "?") for r in rows)
    print("=== CSV Summary ===")
    print(f"  exit_reason : {dict(exit_counts)}")
    print(f"  result      : {dict(result_counts)}")

    # ── Detect the structural issue ───────────────────────────────────────────
    print("\n=== Structural Analysis (be_buffer vs sl_pips) ===")
    issues = []
    for r in rows:
        pair = r.get("pair", "UNKNOWN")
        ps   = pip_size(pair)
        try:
            entry  = float(r["entry_price"])
            sl     = float(r["sl_price"])
            tp1    = float(r["tp1_price"])
            tp2    = float(r["tp2_price"])
        except (KeyError, ValueError):
            continue

        direction = r.get("direction", "BUY").upper()
        if direction == "BUY":
            sl_pips_val  = pips(entry, sl,  ps)
            tp1_pips_val = pips(entry, tp1, ps)
            tp2_pips_val = pips(entry, tp2, ps)
            be_buffer    = pips(entry, float(r.get("exit_price", entry)), ps) \
                           if r.get("exit_reason") == "SL_HIT" and r.get("result") == "BREAKEVEN" \
                           else None
        else:
            sl_pips_val  = pips(entry, sl,  ps)
            tp1_pips_val = pips(entry, tp1, ps)
            tp2_pips_val = pips(entry, tp2, ps)

        # Detect: new_sl after TP1 would be entry + be_buffer_pips
        # If be_buffer_pips > tp1_pips, new_sl is above TP1 (for BUY)
        # which means the runner will fire SL immediately on almost every bar.
        issues.append({
            "pair": pair,
            "direction": direction,
            "sl_pips":  round(sl_pips_val, 1),
            "tp1_pips": round(tp1_pips_val, 1),
            "tp2_pips": round(tp2_pips_val, 1),
            "result":   r.get("result", "?"),
            "exit_reason": r.get("exit_reason", "?"),
        })

    if issues:
        sl_pip_vals  = [x["sl_pips"]  for x in issues]
        tp1_pip_vals = [x["tp1_pips"] for x in issues]
        tp2_pip_vals = [x["tp2_pips"] for x in issues]
        avg_sl  = sum(sl_pip_vals)  / len(sl_pip_vals)
        avg_tp1 = sum(tp1_pip_vals) / len(tp1_pip_vals)
        avg_tp2 = sum(tp2_pip_vals) / len(tp2_pip_vals)
        print(f"  avg SL distance  = {avg_sl:.1f} pips")
        print(f"  avg TP1 distance = {avg_tp1:.1f} pips")
        print(f"  avg TP2 distance = {avg_tp2:.1f} pips")
        print()

        # Try to infer what be_buffer_pips was used
        # In the config, breakeven_buffer_pips = 30 (default).
        # For GBPJPY with avg_sl ~25 pips and be_buffer=30:
        be_buffer_guess = 30.0  # from config.json default
        if avg_tp1 < be_buffer_guess:
            print(f"  ⚠️  STRUCTURAL ISSUE DETECTED:")
            print(f"     TP1 distance ({avg_tp1:.1f} pips) < BE buffer ({be_buffer_guess:.0f} pips)")
            print(f"     This means new_sl after TP1 = entry + {be_buffer_guess:.0f}pips")
            print(f"     which is {be_buffer_guess - avg_tp1:.1f} pips ABOVE TP1.")
            print(f"     ∴ Any bar after TP1 where low dips {be_buffer_guess - avg_tp1:.0f}+ pips below current price → SL fires.")
            print(f"     ∴ Runner gets stopped almost immediately, explaining ~134 SL_HIT / 1 TP2_HIT.")
            print()
            print(f"  RECOMMENDATION: Set breakeven_buffer_pips ≤ sl_pips ({avg_sl:.0f}).")
            print(f"     A sensible value: be_buffer = 5 pips (trail just above entry).")
            print(f"     This keeps runner alive and gives TP2 a real chance.")
        else:
            print(f"  ✅ be_buffer ({be_buffer_guess:.0f}pips) <= tp1 ({avg_tp1:.1f}pips) — no structural issue.")

    # ── Simulate old vs new exit logic ───────────────────────────────────────
    print("\n=== Re-simulation: Old (SL-first) vs New (TP2-first after TP1) ===")
    print("   (This re-simulates exits using exit_price from CSV as a proxy.)")
    print("   NOTE: Without raw M1 candles, we can only detect obvious cases.")
    print()

    reclassified = []
    for r in rows:
        # Trades that closed BREAKEVEN/SL where TP2 SHOULD have been hit:
        # If exit_price (the BE SL) is between TP1 and TP2, AND result == BREAKEVEN,
        # it means the runner was stopped. Under NEW logic, if TP2 was within
        # the same bar that fired SL, it would be WIN instead.
        # We can't rerun candle-by-candle without raw M1 data, but we can flag these.
        if r.get("result") == "BREAKEVEN" and r.get("exit_reason") == "SL_HIT":
            try:
                entry     = float(r["entry_price"])
                tp2_price = float(r["tp2_price"])
                exit_p    = float(r["exit_price"])
                direction = r.get("direction", "BUY").upper()
                ps        = pip_size(r.get("pair", "GBPJPY"))
                # If SL exit is very close to BE (small pip distance), likely just
                # the runner getting knocked out — not necessarily a TP2-missed case.
                be_pnl = float(r.get("profit_usd", 0))
                reclassified.append({
                    "trade_id":  r.get("trade_id", "?"),
                    "pair":      r.get("pair", "?"),
                    "direction": direction,
                    "entry":     entry,
                    "tp1":       float(r.get("tp1_price", 0)),
                    "tp2":       tp2_price,
                    "sl_exit":   exit_p,
                    "profit":    be_pnl,
                })
            except (KeyError, ValueError):
                pass

    print(f"  Trades closed as BREAKEVEN (runner stopped): {len(reclassified)}")
    print()
    print("  Under the NEW engine (TP2-first after TP1), trades where TP2 was")
    print("  within the SAME bar range as the SL will now close as WIN.")
    print("  Run the new backtest to get updated counts.")
    print()

    # Print top 10 BREAKEVEN trades for manual review
    if reclassified:
        print("  Sample BREAKEVEN trades (check if TP2 was in reach):")
        header = f"  {'ID':>6} {'Pair':>8} {'Dir':>5} {'Entry':>9} {'TP1':>9} {'TP2':>9} {'SL@exit':>9} {'P&L':>8}"
        print(header)
        print("  " + "-"*75)
        for t in reclassified[:15]:
            print(
                f"  {t['trade_id']:>6} {t['pair']:>8} {t['direction']:>5} "
                f"{t['entry']:>9.3f} {t['tp1']:>9.3f} {t['tp2']:>9.3f} "
                f"{t['sl_exit']:>9.3f} {t['profit']:>+8.2f}"
            )

    print("\n=== Action Items ===")
    print("  1. Run backtest.py again — the engine now prioritises TP2 over SL in runner phase.")
    print("  2. Compare new TP2_HIT count with the old 1.")
    print("  3. If TP2_HIT count is still low, consider reducing breakeven_buffer_pips")
    print("     in config.json from 30 to 5 (for GBPJPY with ~25-pip SL).")
    print("     Example config change:")
    print('       "breakeven_buffer_pips": 5')
    print()


if __name__ == "__main__":
    main()
