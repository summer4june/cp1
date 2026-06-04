def _check_exits(
    self,
    trade: "SimulatedTrade",
    bar: pd.Series,
    bar_idx: int,
    bar_time: datetime,
) -> bool:
    direction = trade.direction
    bar_high  = bar["high"]
    bar_low   = bar["low"]

    if trade.status == "PENDING":
        triggered = False
        if direction == "BUY" and bar_low <= trade.entry:
            triggered = True
        elif direction == "SELL" and bar_high >= trade.entry:
            triggered = True
            
        if triggered:
            trade.status = "OPEN"
            trade.open_time = bar_time
            trade.open_bar = bar_idx
            
            triggered_session = self._get_session_for_time(bar_time)
            if triggered_session:
                trade.session = triggered_session
                
            logger.info(
                f"[BT] Limit Trade TRIGGERED | {trade.pair} {direction} | "
                f"Bar {bar_idx} | Entry: {trade.entry:.5f} | "
                f"SL: {trade.sl:.5f} | TP2: {trade.tp2:.5f} | Lot: {trade.lot}"
            )
        else:
            return False

    if direction == "BUY":
        sl_hit  = bar_low  <= trade.current_sl
        tp3_hit = bar_high >= trade.tp3 if trade.rr_format == "1:3" else False
        tp2_hit = bar_high >= trade.tp2
        tp1_hit = bar_high >= trade.tp1
    else:  # SELL
        sl_hit  = bar_high >= trade.current_sl
        tp3_hit = bar_low  <= trade.tp3 if trade.rr_format == "1:3" else False
        tp2_hit = bar_low  <= trade.tp2
        tp1_hit = bar_low  <= trade.tp1

    if trade.use_partial_tp:
        
        # ── 1:3 FORMAT ──
        if trade.rr_format == "1:3":
            # Stage 3: TP2 already hit, waiting for TP3 or SL(at TP1)
            if trade.tp2_hit:
                if tp3_hit:
                    pips = self._calc_pips(trade.entry, trade.tp3, direction)
                    tp3_pnl = round(pips * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + tp3_pnl
                    trade.status = "CLOSED"
                    trade.result = "WIN"
                    trade.exit_reason = "TP3_HIT"
                    trade.exit_price = trade.tp3
                    trade.profit_usd = round(total_pnl, 2)
                    trade.close_bar = bar_idx
                    trade.close_time = bar_time
                    self._closed_trades.append(trade)
                    logger.info(f"[BT] ✅ TP3 Hit | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                    return True
                if sl_hit:
                    pips = self._calc_pips(trade.entry, trade.current_sl, direction)
                    sl_pnl = round(pips * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + sl_pnl
                    trade.status = "CLOSED"
                    trade.result = "WIN" # Technically still a win since SL is at TP1
                    trade.exit_reason = "SL_HIT"
                    trade.exit_price = trade.current_sl
                    trade.profit_usd = round(total_pnl, 2)
                    trade.close_bar = bar_idx
                    trade.close_time = bar_time
                    self._closed_trades.append(trade)
                    logger.info(f"[BT] 🔶 Stopped at TP1 (Stage 3 SL) | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                    return True
                return False

            # Stage 2: TP1 already hit, waiting for TP2 or SL(at BE+buffer)
            if trade.tp1_hit:
                if tp2_hit:
                    partial_lot = round(trade.remaining_lot * 0.5, 8)
                    remainder_lot = round(trade.remaining_lot - partial_lot, 8)
                    pips = self._calc_pips(trade.entry, trade.tp2, direction)
                    tp2_pnl = round(pips * self.pip_value * partial_lot, 2)
                    trade.partial_profit += tp2_pnl
                    trade.remaining_lot = max(round(remainder_lot, 8), 0.0)
                    trade.tp2_hit = True
                    trade.current_sl = trade.tp1 # Move SL to TP1 level
                    
                    logger.info(f"[BT] ✅ TP2 Hit (1:3 mode) | {trade.pair} | SL → TP1")
                    
                    if tp3_hit: # Same bar
                        pips3 = self._calc_pips(trade.entry, trade.tp3, direction)
                        tp3_pnl = round(pips3 * self.pip_value * trade.remaining_lot, 2)
                        total_pnl = trade.partial_profit + tp3_pnl
                        trade.status = "CLOSED"
                        trade.result = "WIN"
                        trade.exit_reason = "TP3_HIT"
                        trade.exit_price = trade.tp3
                        trade.profit_usd = round(total_pnl, 2)
                        trade.close_bar = bar_idx
                        trade.close_time = bar_time
                        self._closed_trades.append(trade)
                        logger.info(f"[BT] ✅ TP3 (Same bar) | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                        return True
                    return False
                if sl_hit:
                    pips = self._calc_pips(trade.entry, trade.current_sl, direction)
                    sl_pnl = round(pips * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + sl_pnl
                    trade.status = "CLOSED"
                    trade.result = "BREAKEVEN"
                    trade.exit_reason = "SL_HIT"
                    trade.exit_price = trade.current_sl
                    trade.profit_usd = round(total_pnl, 2)
                    trade.close_bar = bar_idx
                    trade.close_time = bar_time
                    self._closed_trades.append(trade)
                    logger.info(f"[BT] 🔶 Runner SL (BE) | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                    return True
                return False

            # Stage 1: Pre-TP1
            if sl_hit:
                pips = self._calc_pips(trade.entry, trade.current_sl, direction)
                loss = round(pips * self.pip_value * trade.remaining_lot, 2)
                trade.status = "CLOSED"
                trade.result = "LOSS"
                trade.exit_reason = "SL_HIT"
                trade.exit_price = trade.current_sl
                trade.profit_usd = round(loss, 2)
                trade.close_bar = bar_idx
                trade.close_time = bar_time
                self._closed_trades.append(trade)
                logger.info(f"[BT] ❌ SL Hit (pre-TP1) | {trade.pair} | P&L: {loss:+.2f}")
                return True
                
            if tp1_hit:
                partial_lot = round(trade.lot * trade.partial_tp_fraction, 8)
                remainder_lot = round(trade.lot - partial_lot, 8)
                pips_tp1 = self._calc_pips(trade.entry, trade.tp1, direction)
                partial_pnl = round(pips_tp1 * self.pip_value * partial_lot, 2)
                trade.partial_profit = partial_pnl
                trade.remaining_lot = max(round(remainder_lot, 8), 0.0)
                trade.tp1_hit = True
                trade.be_moved = True
                
                buffer = trade.be_buffer_pips * trade.pip_size
                if direction == "BUY":
                    trade.current_sl = round(trade.entry + buffer, 5)
                else:
                    trade.current_sl = round(trade.entry - buffer, 5)
                    
                logger.info(f"[BT] 📊 TP1 Hit (1:3 mode) | {trade.pair} | SL → BE+{trade.be_buffer_pips:.0f}pips")

                if tp2_hit:
                    partial_lot2 = round(trade.remaining_lot * 0.5, 8)
                    remainder_lot2 = round(trade.remaining_lot - partial_lot2, 8)
                    pips2 = self._calc_pips(trade.entry, trade.tp2, direction)
                    tp2_pnl = round(pips2 * self.pip_value * partial_lot2, 2)
                    trade.partial_profit += tp2_pnl
                    trade.remaining_lot = max(round(remainder_lot2, 8), 0.0)
                    trade.tp2_hit = True
                    trade.current_sl = trade.tp1
                    logger.info(f"[BT] ✅ TP2 (Same bar) | {trade.pair} | SL → TP1")

                    if tp3_hit:
                        pips3 = self._calc_pips(trade.entry, trade.tp3, direction)
                        tp3_pnl = round(pips3 * self.pip_value * trade.remaining_lot, 2)
                        total_pnl = trade.partial_profit + tp3_pnl
                        trade.status = "CLOSED"
                        trade.result = "WIN"
                        trade.exit_reason = "TP3_HIT"
                        trade.exit_price = trade.tp3
                        trade.profit_usd = round(total_pnl, 2)
                        trade.close_bar = bar_idx
                        trade.close_time = bar_time
                        self._closed_trades.append(trade)
                        logger.info(f"[BT] ✅ TP3 (Same bar) | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                        return True
                    return False
                return False
            return False


        # ── 1:2 FORMAT (Legacy ZGMT) ──
        else:
            if trade.tp1_hit:
                if tp2_hit:
                    pips = self._calc_pips(trade.entry, trade.tp2, direction)
                    tp2_pnl = round(pips * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + tp2_pnl
                    trade.status = "CLOSED"
                    trade.result = "WIN"
                    trade.exit_reason = "TP2_HIT"
                    trade.exit_price = trade.tp2
                    trade.profit_usd = round(total_pnl, 2)
                    trade.close_bar = bar_idx
                    trade.close_time = bar_time
                    self._closed_trades.append(trade)
                    logger.info(f"[BT] ✅ TP2 Hit | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                    return True
                if sl_hit:
                    pips = self._calc_pips(trade.entry, trade.current_sl, direction)
                    sl_pnl = round(pips * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + sl_pnl
                    trade.status = "CLOSED"
                    trade.result = "BREAKEVEN"
                    trade.exit_reason = "SL_HIT"
                    trade.exit_price = trade.current_sl
                    trade.profit_usd = round(total_pnl, 2)
                    trade.close_bar = bar_idx
                    trade.close_time = bar_time
                    self._closed_trades.append(trade)
                    logger.info(f"[BT] 🔶 Runner SL | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                    return True
                return False

            if sl_hit:
                pips = self._calc_pips(trade.entry, trade.current_sl, direction)
                loss = round(pips * self.pip_value * trade.remaining_lot, 2)
                trade.status = "CLOSED"
                trade.result = "LOSS"
                trade.exit_reason = "SL_HIT"
                trade.exit_price = trade.current_sl
                trade.profit_usd = round(loss, 2)
                trade.close_bar = bar_idx
                trade.close_time = bar_time
                self._closed_trades.append(trade)
                logger.info(f"[BT] ❌ SL Hit (pre-TP1) | {trade.pair} | P&L: {loss:+.2f}")
                return True

            if tp1_hit:
                partial_lot = round(trade.lot * trade.partial_tp_fraction, 8)
                remainder_lot = round(trade.lot - partial_lot, 8)
                pips_tp1 = self._calc_pips(trade.entry, trade.tp1, direction)
                partial_pnl = round(pips_tp1 * self.pip_value * partial_lot, 2)
                trade.partial_profit = partial_pnl
                trade.remaining_lot = max(round(remainder_lot, 8), 0.0)
                trade.tp1_hit = True
                trade.be_moved = True
                
                buffer = trade.be_buffer_pips * trade.pip_size
                if direction == "BUY":
                    trade.current_sl = round(trade.entry + buffer, 5)
                else:
                    trade.current_sl = round(trade.entry - buffer, 5)

                logger.info(f"[BT] 📊 TP1 Hit | {trade.pair} | P&L locked: {partial_pnl:+.2f} | SL → BE+{trade.be_buffer_pips:.0f}pips")

                if tp2_hit:
                    pips_tp2 = self._calc_pips(trade.entry, trade.tp2, direction)
                    tp2_pnl = round(pips_tp2 * self.pip_value * trade.remaining_lot, 2)
                    total_pnl = trade.partial_profit + tp2_pnl
                    trade.status = "CLOSED"
                    trade.result = "WIN"
                    trade.exit_reason = "TP2_HIT"
                    trade.exit_price = trade.tp2
                    trade.profit_usd = round(total_pnl, 2)
                    trade.close_bar = bar_idx
                    trade.close_time = bar_time
                    self._closed_trades.append(trade)
                    logger.info(f"[BT] ✅ TP1+TP2 same bar | {trade.pair} | Total P&L: {total_pnl:+.2f}")
                    return True
            return False

    # ══════════════════════════════════════════════════════════════════
    # Legacy MMXM split mode: SL always takes priority (conservative)
    # ══════════════════════════════════════════════════════════════════
    if sl_hit:
        pips      = self._calc_pips(trade.entry, trade.current_sl, direction)
        loss      = round(pips * self.pip_value * trade.remaining_lot, 2)
        total_pnl = trade.partial_profit + loss
        trade.status      = "CLOSED"
        trade.result      = "BREAKEVEN" if trade.be_moved else "LOSS"
        trade.exit_reason = "SL_HIT"
        trade.exit_price  = trade.current_sl
        trade.profit_usd  = round(total_pnl, 2)
        trade.close_bar   = bar_idx
        trade.close_time  = bar_time
        self._closed_trades.append(trade)
        logger.info(
            f"[BT] {'🔶' if trade.be_moved else '❌'} SL Hit | {trade.pair} | "
            f"P&L: {trade.profit_usd:+.2f} | Result: {trade.result}"
        )
        return True

    # MMXM: TP2 hit (after TP1 already hit)
    if tp2_hit and trade.tp1_hit:
        pips      = self._calc_pips(trade.entry, trade.tp2, direction)
        tp2_pnl   = round(pips * self.pip_value * trade.remaining_lot, 2)
        total_pnl = trade.partial_profit + tp2_pnl
        trade.status      = "CLOSED"
        trade.result      = "WIN"
        trade.exit_reason = "TP2_HIT"
        trade.exit_price  = trade.tp2
        trade.profit_usd  = round(total_pnl, 2)
        trade.close_bar   = bar_idx
        trade.close_time  = bar_time
        self._closed_trades.append(trade)
        logger.info(
            f"[BT] ✅ TP2 Hit | {trade.pair} | P&L: {trade.profit_usd:+.2f}"
        )
        return True

    # MMXM: TP1 hit (first time)
    if tp1_hit and not trade.tp1_hit:
        half_lot = max(0.01, round(trade.lot / 2, 2))
        pips     = self._calc_pips(trade.entry, trade.tp1, direction)
        partial  = round(pips * self.pip_value * half_lot, 2)
        trade.partial_profit = partial
        trade.remaining_lot  = max(0.01, round(trade.lot - half_lot, 2))
        trade.tp1_hit  = True
        trade.be_moved = True
        trade.current_sl = trade.entry   # MMXM: SL to exact BE
        logger.info(
            f"[BT] 📊 TP1 Hit | {trade.pair} | "
            f"Partial locked: {partial:+.2f} | SL → BE"
        )

    return False
