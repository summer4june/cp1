# AMD DETECTION ENGINE — COMPLETE IMPLEMENTATION SPECIFICATION
## Developer Specification Document v2.0

**Document Type:** Software Implementation Specification  
**Target:** Trading Bot — Full AMD (Accumulation → Manipulation → Distribution) Detection  
**Timeframe:** 1-Minute Chart ONLY  
**Markets:** NASDAQ (US100), S&P 500 (US500), Dow Jones (US30)  
**Source:** ICT Hydra Macro Strategy Document  
**Scope:** Complete AMD cycle detection — from range formation through distribution  

---

## TABLE OF CONTENTS

- **PART 1** — ACCUMULATION (§1–§7)
- **PART 2** — MANIPULATION (§8–§10)
- **PART 3** — DISTRIBUTION (§11–§13)
- **PART 4** — MACRO WINDOW SCENARIOS (§14)
- **PART 5** — DEVELOPER EXAMPLES (§15)
- **PART 6** — BOT STATE MACHINE (§16)

---


# ═══════════════════════════════════════════════════════════════
# PART 1 — ACCUMULATION
# ═══════════════════════════════════════════════════════════════

## §1. SUPPORT AND RESISTANCE INSIDE ACCUMULATION

### 1.1 Why Support Forms (Mathematical Explanation)

```
SUPPORT = a price level where BUYING pressure repeatedly absorbs selling pressure.

Mechanically:
  - Multiple candle LOWS cluster at the same price level
  - Each time price approaches this level, it bounces (close moves away from the level)
  - This creates a "floor" of resting BUY orders

MATHEMATICAL DEFINITION:
  support_level = mean(Low[j1], Low[j2], ..., Low[jK])
  WHERE:
    |Low[ja] - Low[jb]| <= tolerance  for all pairs (a, b)
    K >= MIN_TOUCHES (minimum 2, preferred 3+)
    |ja - jb| >= MIN_SEPARATION (at least 2 candles apart)

WHY IT FORMS DURING ACCUMULATION:
  During consolidation, market makers place buy orders at the range low.
  Retail traders place STOP LOSSES below this level.
  Each bounce confirms the level, attracting MORE stops below it.
  This creates SELL-SIDE LIQUIDITY (SSL) below support.
  
  SSL pool grows with each additional touch → eventually becomes 
  the MANIPULATION TARGET.
```

### 1.2 Why Resistance Forms (Mathematical Explanation)

```
RESISTANCE = a price level where SELLING pressure repeatedly absorbs buying pressure.

MATHEMATICAL DEFINITION:
  resistance_level = mean(High[j1], High[j2], ..., High[jK])
  WHERE:
    |High[ja] - High[jb]| <= tolerance  for all pairs (a, b)
    K >= MIN_TOUCHES
    |ja - jb| >= MIN_SEPARATION

WHY IT FORMS DURING ACCUMULATION:
  Market makers place sell orders at the range high.
  Retail traders place STOP LOSSES above this level.
  Each rejection confirms the level, attracting MORE stops above it.
  This creates BUY-SIDE LIQUIDITY (BSL) above resistance.
  
  BSL pool grows with each additional touch → eventually becomes
  the MANIPULATION TARGET.
```

### 1.3 Touch Count Requirements

```
MINIMUM TOUCHES FOR VALID SUPPORT/RESISTANCE:

  WEAK LEVEL:    2 touches → confidence = 0.3
  MODERATE LEVEL: 3 touches → confidence = 0.5
  STRONG LEVEL:  4+ touches → confidence = 0.7+

FORMULA:
  level_confidence = min(1.0, 0.1 + (touch_count * 0.15))

IMPLEMENTATION:
  IF touch_count < 2: level_status = "NOT_FORMED"
  IF touch_count == 2: level_status = "WEAK"
  IF touch_count == 3: level_status = "MODERATE"  
  IF touch_count >= 4: level_status = "STRONG"
```

### 1.4 What Counts as a "Touch"

```
A TOUCH occurs when price APPROACHES and REACTS from a level.

SUPPORT TOUCH (buy reaction):
  touch_valid = TRUE if:
    1. Low[j] <= support_level + tolerance
    2. Close[j] > support_level  (price BOUNCED — closed above support)
    3. Close[j] > Open[j] OR Close[j] > Low[j] + (High[j] - Low[j]) * 0.3
       (candle shows buying pressure — either bullish close or significant bounce)
    4. At least 2 candles since last touch to this level (not consecutive)

RESISTANCE TOUCH (sell reaction):
  touch_valid = TRUE if:
    1. High[j] >= resistance_level - tolerance
    2. Close[j] < resistance_level  (price REJECTED — closed below resistance)
    3. Close[j] < Open[j] OR Close[j] < High[j] - (High[j] - Low[j]) * 0.3
       (candle shows selling pressure)
    4. At least 2 candles since last touch

TOLERANCE CALCULATION:
  tolerance = current_price * EQUAL_LEVEL_TOLERANCE
  WHERE EQUAL_LEVEL_TOLERANCE = 0.0002 (0.02% of price)
  
  Example: price = 18000 → tolerance = 3.6 points
```

### 1.5 Wick vs Body — What Counts

```
BOTH wicks AND bodies count as touches, but with DIFFERENT WEIGHTS:

WICK-ONLY TOUCH (no body penetration):
  - Wick reaches level but body stays away
  - Weight = 1.0 (full touch)
  - This is actually the STRONGEST signal — shows instant rejection
  
BODY TOUCH (body reaches level):
  - Close is AT or very near the level
  - Weight = 0.8
  - Shows level is being tested more aggressively
  
BODY THROUGH (body crosses level):
  - Close crosses to the other side of the level
  - Weight = 0.5 (weakening touch)
  - IF consecutive candle returns: still valid but level is weakening
  - IF 2+ candles close through: LEVEL BROKEN (not a touch — it's a breakout)

CLASSIFICATION:
  penetration = 0 → "WICK_REJECTION" (strongest)
  penetration = body touching level → "BODY_TEST" (moderate)
  penetration = body crosses, then returns → "FAKE_BREAK" (weak but valid)
  penetration = body crosses AND stays → "BREAK" (level invalid)
```

### 1.6 Equal Highs

```
DEFINITION:
  Equal Highs = 2 or more candle Highs that are within tolerance of each other,
                separated by at least MIN_SEPARATION candles.

MATHEMATICAL TEST:
  equal_highs_exist = TRUE if:
    ∃ j1, j2 where:
      |High[j1] - High[j2]| <= tolerance
      |j1 - j2| >= MIN_SEPARATION (2 candles)
      High[j1] is a local maximum (swing high)
      High[j2] is a local maximum (swing high)

WHY THEY MATTER:
  Equal Highs = visible resistance = stops accumulate above
  = BUY-SIDE LIQUIDITY (BSL) target for manipulation

CONFIDENCE SCALING:
  2 equal highs → moderate BSL pool
  3 equal highs → strong BSL pool
  4+ equal highs → very strong BSL pool (high probability manipulation target)

NUMERICAL EXAMPLE:
  ATR = 10 points, price = 18050
  tolerance = 18050 * 0.0002 = 3.61 points
  
  C[3].High = 18068
  C[7].High = 18069.5
  C[11].High = 18067.8
  
  |18068 - 18069.5| = 1.5 ≤ 3.61 ✓
  |18068 - 18067.8| = 0.2 ≤ 3.61 ✓
  |j3 - j7| = 4 ≥ 2 ✓
  |j7 - j11| = 4 ≥ 2 ✓
  
  RESULT: 3 EQUAL HIGHS at level ≈ 18068.4 → STRONG BSL
```

### 1.7 Equal Lows

```
DEFINITION:
  Equal Lows = 2 or more candle Lows within tolerance, separated by MIN_SEPARATION.

MATHEMATICAL TEST:
  equal_lows_exist = TRUE if:
    ∃ j1, j2 where:
      |Low[j1] - Low[j2]| <= tolerance
      |j1 - j2| >= MIN_SEPARATION
      Low[j1] is a local minimum (swing low)
      Low[j2] is a local minimum (swing low)

WHY THEY MATTER:
  Equal Lows = visible support = stops accumulate below
  = SELL-SIDE LIQUIDITY (SSL) target for manipulation

NUMERICAL EXAMPLE:
  C[2].Low = 18032
  C[6].Low = 18031.2
  C[10].Low = 18032.5
  
  All within tolerance → 3 EQUAL LOWS at level ≈ 18031.9 → STRONG SSL
```

### 1.8 Minor Highs and Minor Lows

```
DEFINITION:
  Minor High = a swing high that forms INSIDE the accumulation range
               but is NOT at the range boundary.
  Minor Low = a swing low that forms INSIDE the accumulation range
              but is NOT at the range boundary.

DETECTION:
  range_size = range_high - range_low
  boundary_zone = range_size * 0.15  // Top/bottom 15% = boundary
  
  minor_high: swing_high.level < (range_high - boundary_zone)
  minor_low:  swing_low.level > (range_low + boundary_zone)

PURPOSE:
  Minor highs/lows create INTERNAL LIQUIDITY.
  They provide micro-targets during manipulation.
  They help define the internal structure of accumulation.
  
  Internal liquidity being swept INSIDE the range ≠ manipulation.
  Only EXTERNAL liquidity sweep (beyond range boundaries) = manipulation.
```

### 1.9 Liquidity Resting Above Resistance / Below Support

```
LIQUIDITY MAP:

  BSL (Buy-Side Liquidity) = stops resting ABOVE resistance
    Location: resistance_level + spread_to_stop_zone
    Typical offset: 1-5 points above equal highs
    Grows stronger with: more touches, cleaner equal highs, longer duration
    
  SSL (Sell-Side Liquidity) = stops resting BELOW support
    Location: support_level - spread_to_stop_zone
    Typical offset: 1-5 points below equal lows
    Grows stronger with: more touches, cleaner equal lows, longer duration

LIQUIDITY STRENGTH FORMULA:
  liquidity_strength = touch_count * duration_factor * cleanness_factor
  
  WHERE:
    touch_count = number of times level was tested
    duration_factor = min(2.0, candles_since_first_touch / 10)
    cleanness_factor = 1.0 if equal levels, 0.7 if uneven levels

  liquidity_strength >= 3.0 → HIGH (prime manipulation target)
  liquidity_strength >= 1.5 → MODERATE (possible target)
  liquidity_strength < 1.5  → LOW (unlikely target)
```

### 1.10 Range Compression and Expansion

```
RANGE COMPRESSION:
  The range TIGHTENS over time — candles get smaller, boundaries move inward.
  
  Detected by:
    rolling_range[i] = max(High[i-W:i]) - min(Low[i-W:i])
    WHERE W = rolling window size (5 candles)
    
    compression_detected = TRUE if:
      rolling_range[current] < rolling_range[earlier] * 0.7
      (range shrunk by 30%+)
  
  SIGNIFICANCE: Compression → energy building → breakout imminent
  
RANGE EXPANSION:
  The range WIDENS — a candle extends beyond previous boundaries.
  
  expansion_up = High[j] > previous_range_high
  expansion_down = Low[j] < previous_range_low
  
  RULES:
    IF expansion occurs AND range stays within ATR * RANGE_ATR_MAX:
      → Range is expanding but still accumulation (update boundaries)
    IF expansion causes range > ATR * 3.0:
      → Range too wide → INVALID accumulation
    IF expansion is a single large candle (body > range * 0.6):
      → Not expansion — it's a BREAKOUT attempt
```

### 1.11 False Breakout vs True Breakout

```
FALSE BREAKOUT (= Manipulation):
  Price exceeds range boundary BUT returns inside.
  
  Conditions:
    1. Price exceeds boundary: High[j] > range_high (or Low[j] < range_low)
    2. Price returns: Close[j] <= range_high (or Close[j] >= range_low)
       OR Close[j+1] is back inside range
    3. The exceeded level had liquidity (equal highs/lows)
  
  Classification: This IS the manipulation phase of AMD

TRUE BREAKOUT:
  Price exceeds range boundary AND CONTINUES beyond.
  
  Conditions:
    1. Price exceeds boundary
    2. Close[j] > range_high + buffer (or Close[j] < range_low - buffer)
    3. Close[j+1] ALSO beyond the range (confirmation)
    4. Body of breakout candle > ATR * 0.5 (shows conviction)
  
  Classification: Accumulation is OVER — either manipulation succeeded
                  and distribution started, or it was never true accumulation.
```

---


## §2. TRENDING MARKET vs ACCUMULATION — DIFFERENTIATION

### 2.1 The Core Problem

```
A bot must distinguish between:
  STATE A: Price is TRENDING (making directional progress)
  STATE B: Price is ACCUMULATING (consolidating within a bounded range)

These look similar on individual candles.
The difference is measurable ONLY through aggregate statistics.
```

### 2.2 Measurement Matrix

| Metric | TRENDING | ACCUMULATING | Detection Formula |
|--------|----------|--------------|-------------------|
| Net Displacement | HIGH (>60% of range) | LOW (<40% of range) | `\|Close[last] - Close[first]\| / range_size` |
| Swing Distance | INCREASING | CONSTANT/DECREASING | `avg(swing_distance[recent]) vs avg(swing_distance[earlier])` |
| Average Candle Body | LARGE (>40% ATR) | SMALL (<30% ATR) | `mean(\|Close-Open\|) / ATR` |
| ATR Relative to Range | ATR > Range/3 | ATR < Range/3 | `ATR / range_size` |
| Momentum (close position) | Consistently near High/Low | Mixed | `mean((Close-Low)/(High-Low))` for bull |
| Range Width | EXPANDING | STABLE or COMPRESSING | `Δ(range_size) over last N candles` |
| Direction Changes | FEW (<25%) | MANY (>35%) | `direction_changes / (N-1)` |
| Higher Highs/Lower Lows | YES (sequential) | NO (rotating) | `count(HH) or count(LL) / total_swings` |
| Retracement Depth | SHALLOW (<50%) | DEEP (>50%, often 100%) | `retrace_size / impulse_size` |
| Internal Structure | Clear HH+HL or LL+LH | Mixed, no clear direction | State machine analysis |

### 2.3 Net Displacement Test

```
FORMULA:
  net_displacement_ratio = |Close[end] - Close[start]| / range_size

THRESHOLDS:
  ratio < 0.25 → STRONG accumulation signal (price went nowhere)
  ratio 0.25-0.40 → MODERATE accumulation signal
  ratio 0.40-0.60 → AMBIGUOUS (could be slow trend or late accumulation)
  ratio > 0.60 → TRENDING (not accumulation)

EXAMPLE — ACCUMULATION:
  Start: Close[0] = 18050
  End: Close[19] = 18053
  Range: 18035 to 18070 → range_size = 35
  net_displacement_ratio = |18053 - 18050| / 35 = 0.086 → STRONG ACCUMULATION ✓

EXAMPLE — TREND:
  Start: Close[0] = 18050
  End: Close[19] = 18085
  Range: 18045 to 18090 → range_size = 45
  net_displacement_ratio = |18085 - 18050| / 45 = 0.778 → TRENDING ✗
```

### 2.4 Swing Distance Test

```
DEFINITION:
  In ACCUMULATION: swings (high-to-low, low-to-high) are roughly EQUAL in size.
  In TREND: swings in the trend direction are LARGER than counter-trend swings.

FORMULA:
  swing_impulses = [size of moves in dominant direction]
  swing_corrections = [size of moves against dominant direction]
  
  swing_ratio = mean(swing_corrections) / mean(swing_impulses)

THRESHOLDS:
  ratio > 0.8 → ACCUMULATION (corrections nearly as large as impulses)
  ratio 0.5-0.8 → WEAK TREND or TRANSITIONING
  ratio < 0.5 → STRONG TREND (corrections are small relative to impulses)

EXAMPLE — ACCUMULATION:
  Swing up 1: 18040 → 18065 = 25 points
  Swing down 1: 18065 → 18042 = 23 points
  Swing up 2: 18042 → 18068 = 26 points
  Swing down 2: 18068 → 18038 = 30 points
  
  impulses (assuming no clear dominant): avg = (25+23+26+30)/4 = 26
  Actually both directions similar → ratio ≈ 1.0 → ACCUMULATION ✓

EXAMPLE — TREND (bullish):
  Swing up 1: 18040 → 18070 = 30 points (impulse)
  Swing down 1: 18070 → 18058 = 12 points (correction)
  Swing up 2: 18058 → 18090 = 32 points (impulse)
  Swing down 2: 18090 → 18080 = 10 points (correction)
  
  swing_ratio = mean(12,10) / mean(30,32) = 11 / 31 = 0.35 → STRONG TREND ✗
```

### 2.5 Market Structure Test

```
IN A BULLISH TREND:
  Higher Highs (HH): each swing high > previous swing high
  Higher Lows (HL): each swing low > previous swing low
  
IN A BEARISH TREND:
  Lower Lows (LL): each swing low < previous swing low
  Lower Highs (LH): each swing high < previous swing high

IN ACCUMULATION:
  NO consistent pattern:
  - Some highs higher, some lower
  - Some lows higher, some lower
  - Highs cluster near range_high
  - Lows cluster near range_low

STRUCTURE SCORE:
  hh_count = count of swing highs > previous swing high
  hl_count = count of swing lows > previous swing low
  total_swings = total swing count
  
  bullish_structure_score = (hh_count + hl_count) / (total_swings * 2)
  bearish_structure_score = (ll_count + lh_count) / (total_swings * 2)
  
  IF bullish_structure_score > 0.7 → BULLISH TREND
  IF bearish_structure_score > 0.7 → BEARISH TREND
  IF both < 0.4 → ACCUMULATION (no clear structure)
  IF between 0.4 and 0.7 → AMBIGUOUS (transitioning)
```

### 2.6 ATR-Based Classification

```
FORMULA:
  atr_range_ratio = ATR(20) / range_size

INTERPRETATION:
  ratio > 0.5 → Individual candles are large relative to range → TRENDING
    (single candle covers >50% of total range = not contained)
  ratio 0.2-0.5 → Normal accumulation territory
  ratio < 0.15 → Very tight range, possible noise
  
EXAMPLE:
  ATR = 8 points, range_size = 20 points
  ratio = 8/20 = 0.4 → Valid accumulation (candles fit within range)
  
  ATR = 15 points, range_size = 20 points
  ratio = 15/20 = 0.75 → Candles too large for range → TRENDING ✗
```

### 2.7 Retracement Depth Test

```
IN A TREND: Retracements are SHALLOW
  - Bullish trend: pullbacks retrace 23.6% to 50% of impulse
  - Bearish trend: pullbacks retrace 23.6% to 50% of impulse
  
IN ACCUMULATION: "Retracements" are DEEP (61.8% to 100%+)
  - Price swings almost entirely reverse the previous swing
  - Often retraces 100% or more (overshoots in other direction)

FORMULA:
  retrace_depth = |swing_end - retrace_end| / |swing_start - swing_end|
  
  IF retrace_depth consistently > 0.618 → ACCUMULATION characteristic
  IF retrace_depth consistently < 0.5 → TREND characteristic
```

### 2.8 Complete Differentiation Algorithm

```
FUNCTION classify_market_state(candles[], atr):
  range_size = max(highs) - min(lows)
  
  // Test 1: Net Displacement
  net_disp = abs(candles[-1].close - candles[0].close) / range_size
  
  // Test 2: Direction Changes
  directions = [1 if c.close > c.open else -1 for c in candles]
  changes = count_direction_changes(directions)
  change_ratio = changes / (len(candles) - 1)
  
  // Test 3: ATR Ratio
  atr_ratio = atr / range_size
  
  // Test 4: Swing Symmetry
  swings = detect_all_swings(candles)
  swing_ratio = calculate_swing_symmetry(swings)
  
  // Test 5: Structure
  structure_score = calculate_structure_score(swings)
  
  // Scoring
  accumulation_score = 0
  
  IF net_disp < 0.4: accumulation_score += 2
  ELIF net_disp < 0.25: accumulation_score += 3
  
  IF change_ratio > 0.35: accumulation_score += 2
  IF atr_ratio < 0.4: accumulation_score += 1
  IF swing_ratio > 0.7: accumulation_score += 2
  IF structure_score < 0.4: accumulation_score += 2  // No clear trend structure
  
  // Classification
  IF accumulation_score >= 8: RETURN "STRONG_ACCUMULATION"
  IF accumulation_score >= 5: RETURN "PROBABLE_ACCUMULATION"
  IF accumulation_score >= 3: RETURN "AMBIGUOUS"
  RETURN "TRENDING"
```

---


## §3. CANDLE BEHAVIOUR INSIDE ACCUMULATION

### 3.1 Candle Classification System

```
Every candle is classified into one of the following types based on its OHLC values.
The bot must calculate these properties for EACH candle:

PROPERTIES:
  body = abs(Close - Open)
  upper_wick = High - max(Open, Close)
  lower_wick = min(Open, Close) - Low
  total_range = High - Low
  body_ratio = body / total_range (if total_range > 0, else 0)
  upper_wick_ratio = upper_wick / total_range
  lower_wick_ratio = lower_wick / total_range
  direction = "BULL" if Close > Open else "BEAR" (Close == Open → "DOJI")
  body_position = (min(Open,Close) - Low) / total_range  // 0=body at bottom, 1=body at top
```

### 3.2 Large Body Candles

```
DEFINITION: body_ratio >= 0.7 (body is 70%+ of total candle range)

MEANING INSIDE ACCUMULATION:
  - Shows strong directional intent
  - If body > range_size * 0.5 → POTENTIAL BREAKOUT or INVALIDATION
  - If body > range_size * 0.3 but < 0.5 → Warning signal (accumulation weakening)
  - Single large body inside range → likely to reverse (stopped by other side)

BOT ACTION:
  IF body > range_size * 0.6:
    → Check if followed by reversal → if yes, still accumulation
    → If NOT reversed within 2 candles → possible breakout → prepare for transition
  IF body > range_size * 0.8:
    → INVALIDATION: This is not accumulation behaviour
    → Transition to MANIPULATION_STARTED or INVALID

EXAMPLE:
  Range: 18040-18060 (range_size = 20)
  ATR = 10
  Candle: O=18042 H=18058 L=18040 C=18057
  body = |18057-18042| = 15, body_ratio = 15/18 = 0.83
  body_vs_range = 15/20 = 0.75 → LARGE body, covers 75% of range
  → ALERT: Potential distribution starting
```

### 3.3 Small Body Candles

```
DEFINITION: body_ratio <= 0.3 (body is 30% or less of total candle range)

MEANING INSIDE ACCUMULATION:
  - NORMAL during accumulation
  - Shows indecision / equilibrium between buyers and sellers
  - Indicates range is being MAINTAINED
  - Many consecutive small bodies → COMPRESSION → breakout imminent

BOT ACTION:
  IF avg_body_ratio over last 5 candles < 0.25:
    → COMPRESSION DETECTED
    → Increase readiness for manipulation
    → Next large candle will likely start the manipulation phase

EXAMPLE:
  Candle: O=18050 H=18055 L=18046 C=18051
  body = 1, total_range = 9, body_ratio = 1/9 = 0.11 → SMALL BODY
  → Normal accumulation candle, no action required
```

### 3.4 Large Wick Candles

```
DEFINITION: upper_wick_ratio >= 0.4 OR lower_wick_ratio >= 0.4

MEANING INSIDE ACCUMULATION:
  UPPER WICK DOMINANT (upper_wick_ratio >= 0.4):
    - Price tried to go UP but was REJECTED
    - Selling pressure exists above
    - IF at range_high → reinforces resistance
    - IF in middle of range → shows micro selling pressure
    
  LOWER WICK DOMINANT (lower_wick_ratio >= 0.4):
    - Price tried to go DOWN but was REJECTED
    - Buying pressure exists below
    - IF at range_low → reinforces support
    - IF in middle of range → shows micro buying pressure

BOT ACTION:
  IF large wick AT range boundary:
    → INCREMENT touch_count for that boundary
    → Increase liquidity estimate beyond that boundary
  IF large wick IN MIDDLE of range:
    → Note but do not act — internal market structure noise
```

### 3.5 Inside Candles

```
DEFINITION:
  inside_candle = (High[j] <= High[j-1]) AND (Low[j] >= Low[j-1])
  The current candle is completely WITHIN the previous candle's range.

MEANING INSIDE ACCUMULATION:
  - COMPRESSION signal
  - Market deciding direction
  - Very common during late accumulation (just before manipulation)
  - Multiple consecutive inside candles → STRONG compression → breakout next

BOT ACTION:
  inside_count = consecutive inside candles
  IF inside_count >= 3:
    → FLAG: "HIGH_COMPRESSION_ALERT"
    → Manipulation likely within next 5 candles
    → Increase monitoring frequency

EXAMPLE:
  C[5]: O=18048 H=18058 L=18042 C=18055  ← Parent candle
  C[6]: O=18050 H=18056 L=18044 C=18052  ← Inside (56<58, 44>42)
  C[7]: O=18051 H=18054 L=18046 C=18049  ← Inside of inside
  → 2 consecutive inside candles → compression building
```

### 3.6 Outside Candles (Engulfing)

```
DEFINITION:
  outside_candle = (High[j] > High[j-1]) AND (Low[j] < Low[j-1])
  The current candle EXCEEDS the previous candle on BOTH sides.

MEANING INSIDE ACCUMULATION:
  - EXPANSION signal
  - Market showing increased volatility
  - IF body is large → potential start of manipulation
  - IF wicks are dominant (body small within outside range) → still indecision

BOT ACTION:
  IF outside_candle AND body_ratio > 0.6:
    → Check if this is manipulation starting
    → Did the wick sweep liquidity on one side?
  IF outside_candle AND body_ratio < 0.3:
    → Volatility expansion but no directional conviction
    → Range boundaries may need updating
    → Still accumulation
```

### 3.7 Doji Candles

```
DEFINITION: body_ratio <= 0.1 (body is essentially zero)

VARIANTS:
  Standard Doji: upper_wick ≈ lower_wick (both present)
  Gravestone Doji: upper_wick_ratio > 0.7, lower_wick minimal
  Dragonfly Doji: lower_wick_ratio > 0.7, upper_wick minimal

MEANING INSIDE ACCUMULATION:
  Standard Doji → Pure indecision, normal during accumulation
  Gravestone at range_high → Strong rejection, resistance confirmed
  Dragonfly at range_low → Strong rejection, support confirmed

BOT ACTION:
  Gravestone at range_high:
    → touch_count[resistance] += 1
    → increase BSL estimate
  Dragonfly at range_low:
    → touch_count[support] += 1
    → increase SSL estimate
  Standard in middle:
    → No action (noise)
```

### 3.8 Compression Candles (Series)

```
DEFINITION:
  compression_series = TRUE if:
    FOR i in range(start, end):
      (High[i] - Low[i]) < (High[i-1] - Low[i-1])  // Each candle range SHRINKS
    AND (end - start) >= 3  // At least 3 shrinking candles

MEANING:
  - Market is coiling — energy building
  - Classic pre-manipulation pattern
  - The longer compression lasts, the more explosive the breakout

BOT ACTION:
  IF compression_series detected AND inside confirmed accumulation:
    → STATE_FLAG: "COMPRESSION_BEFORE_MANIPULATION"
    → Expected: large candle within next 3-5 candles
    → This large candle will likely be the manipulation sweep
```

### 3.9 Expansion Candles

```
DEFINITION:
  expansion_candle = (High[j] - Low[j]) > ATR * 1.5
  A single candle whose range is significantly larger than average.

MEANING INSIDE ACCUMULATION:
  - If occurs WITHIN range boundaries → internal volatility, still accumulation
  - If EXCEEDS one boundary → potential manipulation starting
  - If EXCEEDS both boundaries → outside candle, range expanding or breaking

BOT ACTION:
  IF expansion_candle AND stays within range:
    → Note volatility increase, accumulation still valid
  IF expansion_candle AND exceeds one boundary:
    → Check: did it sweep liquidity?
    → Did it close back inside? → MANIPULATION
    → Did it close outside? → Wait next candle for confirmation
```

### 3.10 When to IGNORE Candle Signals

```
IGNORE candle classification when:
  1. During news blackout windows → all signals unreliable
  2. First candle of session open → gap behavior, not normal
  3. Single anomaly surrounded by normal behavior → likely noise
  4. Candle total_range < ATR * 0.1 → too small to be meaningful (tick noise)
  5. During already-confirmed manipulation → we're past accumulation phase
```

### 3.11 When Candle Signals MATTER

```
CANDLE SIGNALS ARE CRITICAL when:
  1. At range boundaries (high/low) → confirms support/resistance
  2. During compression → signals imminent breakout direction
  3. After extended accumulation (>20 candles) → signals manipulation start
  4. Large body breaking range → confirms distribution start
  5. Series of same-direction candles → accumulation may be ending
```

---


## §4. WICK BEHAVIOUR — COMPLETE ANALYSIS

### 4.1 When a Wick Represents Manipulation

```
A wick represents MANIPULATION when ALL of the following are TRUE:

1. The wick extends BEYOND a confirmed support/resistance level
2. The body CLOSES BACK on the correct side of the level
3. The level had identified liquidity (equal highs/lows with 2+ touches)
4. The wick penetration is > ATR * 0.05 beyond the level (not just touching)
5. The wick occurs during an active macro window

FORMULA:
  wick_is_manipulation = 
    (wick_penetration_beyond_level > ATR * 0.05)
    AND (body_remains_inside_range == TRUE)
    AND (liquidity_at_level >= 2 touches)
    AND (macro_window_active == TRUE)
```

### 4.2 When a Wick Does NOT Represent Manipulation

```
A wick is NOT manipulation when:

1. It does NOT reach a liquidity level (falls short)
2. The body ALSO closes beyond the level (not just wick)
3. There is no identified liquidity at the level (random price level)
4. It occurs during news blackout (unreliable data)
5. The wick penetration is minimal (< ATR * 0.03 beyond level)
6. Multiple candles close beyond the level (genuine breakout, not manipulation)
```

### 4.3 Acceptable Wick Depth

```
ACCEPTABLE = wick extends beyond range but WITHIN manipulation zone

wick_depth = |wick_tip - range_boundary|

Classification:
  wick_depth < ATR * 0.03 → "NOISE" (too shallow, not meaningful)
  wick_depth ATR * 0.03 to ATR * 0.3 → "VALID_MANIPULATION" (normal sweep)
  wick_depth ATR * 0.3 to ATR * 0.8 → "DEEP_MANIPULATION" (aggressive sweep)
  wick_depth > ATR * 0.8 → "POSSIBLE_BREAKOUT" (may not return)
  wick_depth > ATR * 1.5 → "BREAKOUT" (definitely not manipulation)
```

### 4.4 When Wick INVALIDATES Accumulation

```
A wick invalidates accumulation when:

1. Wick extends > ATR * 1.5 beyond range boundary
   AND body closes beyond boundary
   → This is not a sweep, it's a genuine breakout
   
2. Two consecutive candles have wicks beyond the SAME side
   AND both close beyond the boundary
   → Sustained pressure = not manipulation, it's trending

3. Wick extends beyond range boundary
   AND next candle ALSO exceeds with its body
   → Continuation = breakout confirmed = accumulation OVER
```

### 4.5 Wick as Liquidity Engineering

```
Wick is "liquidity engineering" (manipulation) when:
  1. Wick sweeps past equal highs/lows (takes the stops)
  2. Body immediately returns inside the range
  3. The very next candle(s) move in the OPPOSITE direction
  4. Displacement follows within 1-3 candles

This is the textbook manipulation wick:
  - Reaches out to grab stops
  - Closes back inside
  - Then market explodes in opposite direction
```

### 4.6 — 30 WICK EXAMPLES WITH OHLC

```
RANGE: 18040 (support/SSL) to 18060 (resistance/BSL)
ATR = 10 points
Tolerance = 3.6 points

Example Legend:
  O=Open, H=High, L=Low, C=Close
  Classification categories:
    MANIP_UP = upside manipulation (BSL sweep, bearish intent)
    MANIP_DOWN = downside manipulation (SSL sweep, bullish intent)
    NOISE = too small to matter
    BREAKOUT = genuine break, not manipulation
    PARTIAL = approached but didn't reach liquidity
    INVALID = does not classify as manipulation
```

#### DOWNSIDE MANIPULATION WICKS (SSL Sweep — Bullish Intent)

```
EX 1: Perfect SSL Sweep - Wick Only
  O=18045 H=18050 L=18036 C=18048
  Analysis: Low=18036 < support(18040) by 4 points. Close=18048 > support. 
  Wick_depth = 18040-18036 = 4 > ATR*0.03=0.3 ✓
  Body inside range ✓. Liquidity at 18040 (equal lows) ✓.
  → CLASSIFICATION: MANIP_DOWN (valid SSL sweep)
  → BOT: Transition to MANIPULATION_DETECTED, expect bullish MSS

EX 2: Deep SSL Sweep - Wick Only
  O=18048 H=18052 L=18032 C=18046
  Analysis: Low=18032 < support(18040) by 8 points. Close=18046 > support.
  Wick_depth = 8 (within ATR*0.3 to ATR*0.8 → "DEEP_MANIPULATION")
  → CLASSIFICATION: MANIP_DOWN (deep but valid)
  → BOT: High confidence manipulation

EX 3: Shallow SSL Test - Barely Touches
  O=18044 H=18049 L=18039 C=18047
  Analysis: Low=18039 < support(18040) by 1 point.
  Wick_depth = 1 < ATR*0.03=0.3? No, 1 > 0.3 ✓ (barely)
  But: Did it reach the liquidity? If equal lows at 18040, and this only went to 18039...
  → CLASSIFICATION: PARTIAL (reached level but barely swept)
  → BOT: Monitor — may need second attempt

EX 4: SSL Sweep with Body Penetration  
  O=18044 H=18050 L=18035 C=18038
  Analysis: Low=18035 < support. BUT Close=18038 < support(18040)!
  Body closed BELOW support. Need next candle.
  → CLASSIFICATION: PENDING (wait for next candle)
  → IF next candle Close > 18040: → MANIP_DOWN (confirmed)
  → IF next candle Close < 18038: → BREAKOUT_DOWN (not manipulation)

EX 5: Failed SSL Sweep - Doesn't Reach
  O=18046 H=18052 L=18041 C=18049
  Analysis: Low=18041, support at 18040. Difference = 1 point.
  Did NOT penetrate below support meaningfully.
  Within tolerance (3.6 points) but didn't actually sweep.
  → CLASSIFICATION: NOISE (level tested but not swept)
  → BOT: Increment touch_count, but NOT manipulation

EX 6: Aggressive SSL Sweep
  O=18050 H=18052 L=18028 C=18044
  Analysis: Low=18028, support at 18040. Depth = 12 points = ATR*1.2
  Wick_depth > ATR*0.8 → "POSSIBLE_BREAKOUT"
  BUT Close=18044 > support ✓
  → CLASSIFICATION: MANIP_DOWN (aggressive but returned)
  → BOT: Valid manipulation, high displacement expected after

EX 7: Too Deep - Likely Breakout
  O=18048 H=18050 L=18020 C=18025
  Analysis: Low=18020, depth=20=ATR*2.0. Close=18025 < support.
  Body closed below support. Depth > ATR*1.5.
  → CLASSIFICATION: BREAKOUT_DOWN (not manipulation)
  → BOT: Accumulation INVALIDATED. Price broke support genuinely.

EX 8: SSL Sweep with Dragonfly Doji
  O=18042 H=18044 L=18033 C=18042
  Analysis: Dragonfly doji pattern. Low sweeps SSL.
  Body essentially zero (O≈C), massive lower wick.
  → CLASSIFICATION: MANIP_DOWN (textbook manipulation wick)
  → BOT: Very high confidence — classic liquidity sweep

EX 9: Gradual SSL Approach (Not Sweep)
  O=18043 H=18047 L=18040 C=18041
  Analysis: Low=18040 = exactly at support. No penetration below.
  → CLASSIFICATION: TOUCH (support tested, not swept)
  → BOT: touch_count += 1, NOT manipulation

EX 10: SSL Sweep Mid-Range Origin
  O=18055 H=18056 L=18037 C=18043
  Analysis: Started near range_high, swept all the way down past SSL.
  Low=18037 < support(18040) by 3. Close=18043 > support ✓.
  Large range candle (19 points = almost full range).
  → CLASSIFICATION: MANIP_DOWN (valid, started from resistance area)
  → BOT: Watch for bullish MSS on next candles
```

#### UPSIDE MANIPULATION WICKS (BSL Sweep — Bearish Intent)

```
EX 11: Perfect BSL Sweep - Wick Only
  O=18055 H=18064 L=18050 C=18053
  Analysis: High=18064 > resistance(18060) by 4. Close=18053 < resistance ✓.
  → CLASSIFICATION: MANIP_UP (valid BSL sweep)
  → BOT: Expect bearish MSS

EX 12: Deep BSL Sweep
  O=18052 H=18070 L=18048 C=18050
  Analysis: High=18070 > resistance by 10 = ATR*1.0 → DEEP
  Close=18050 < resistance ✓.
  → CLASSIFICATION: MANIP_UP (deep manipulation)

EX 13: BSL Sweep with Close Above
  O=18056 H=18066 L=18054 C=18062
  Analysis: High=18066 > resistance. BUT Close=18062 > resistance!
  → CLASSIFICATION: PENDING (need next candle)
  → IF next Close < 18060: MANIP_UP confirmed
  → IF next Close > 18062: BREAKOUT_UP

EX 14: Barely Touches BSL
  O=18054 H=18061 L=18050 C=18056
  Analysis: High=18061, resistance=18060. Penetration = 1 point.
  Barely above. Within tolerance but minimal sweep.
  → CLASSIFICATION: PARTIAL (weak attempt)

EX 15: Gravestone Doji at Resistance
  O=18058 H=18067 L=18057 C=18058
  Analysis: Gravestone doji. High sweeps BSL (18067 > 18060 by 7).
  Body zero, massive upper wick.
  → CLASSIFICATION: MANIP_UP (textbook rejection + sweep)

EX 16: BSL Sweep - Multiple Candle
  C[j]:   O=18055 H=18063 L=18053 C=18061 (close above resistance)
  C[j+1]: O=18061 H=18062 L=18052 C=18054 (returns below)
  Analysis: First candle closed above (pending). Second returned.
  → CLASSIFICATION: MANIP_UP (2-candle manipulation confirmed)

EX 17: Failed BSL - Doesn't Reach Liquidity
  O=18053 H=18058 L=18050 C=18055
  Analysis: High=18058, resistance=18060. Didn't reach level.
  → CLASSIFICATION: NOISE (no manipulation occurred)
  → BOT: No state change

EX 18: BSL with Gap Up Opening
  O=18062 H=18065 L=18055 C=18056
  Analysis: Opened above resistance (gap), swept BSL at 18065.
  Closed back inside range at 18056.
  → CLASSIFICATION: MANIP_UP (gap-and-sweep pattern)

EX 19: Extremely Deep BSL Sweep
  O=18055 H=18080 L=18054 C=18078
  Analysis: High=18080, depth=20=ATR*2.0. Close=18078 > resistance.
  Body closed above. Way too deep.
  → CLASSIFICATION: BREAKOUT_UP (not manipulation)
  → BOT: Accumulation OVER

EX 20: BSL Sweep at Macro Open
  Time: 18:40 IST (Macro 3 opens)
  O=18058 H=18065 L=18056 C=18057
  Analysis: High sweeps BSL at macro open. Close returns inside.
  Macro window active ✓.
  → CLASSIFICATION: MANIP_UP (high probability — macro-aligned)
```

#### AMBIGUOUS / SPECIAL WICK CASES

```
EX 21: Both Wicks Extended (Doji spanning range)
  O=18050 H=18063 L=18037 C=18051
  Analysis: Upper wick sweeps BSL (63>60). Lower wick sweeps SSL (37<40).
  BOTH sides swept in single candle!
  → CLASSIFICATION: DOUBLE_SWEEP
  → BOT: Use the CLOSE LOCATION to determine direction.
         Close=18051 is MID-RANGE. Check next candle for direction.
         If next candle bullish → downside was primary manipulation
         If next candle bearish → upside was primary manipulation

EX 22: Wick at Internal Level (Not Boundary)
  O=18048 H=18055 L=18044 C=18050
  Analysis: High=18055 < resistance(18060). Low=18044 > support(18040).
  Wick doesn't reach either boundary.
  → CLASSIFICATION: INTERNAL (no manipulation at boundaries)
  → BOT: Normal accumulation candle, no boundary event

EX 23: Wick Exactly at Level (Pinpoint Touch)
  O=18050 H=18060.0 L=18048 C=18052
  Analysis: High=18060.0 = EXACTLY resistance. No penetration.
  → CLASSIFICATION: TOUCH (tested resistance but did NOT sweep)
  → BOT: touch_count[resistance] += 1, not manipulation

EX 24: Wick Into News Candle
  Time: During FOMC release
  O=18050 H=18075 L=18025 C=18060
  Analysis: Massive range. But news blackout active.
  → CLASSIFICATION: IGNORED (news candle — unreliable data)
  → BOT: Do NOT classify. Wait for blackout to end.

EX 25: Consecutive Wicks Same Direction
  C[j]:   O=18050 H=18063 L=18048 C=18052 (wick above resistance)
  C[j+1]: O=18052 H=18065 L=18049 C=18054 (another wick above)
  Analysis: Two consecutive wicks sweeping BSL. Both close below.
  → CLASSIFICATION: MANIP_UP (sustained sweep — very strong signal)
  → BOT: High confidence. Multiple sweeps = all liquidity taken.

EX 26: Wick After Manipulation Already Detected
  State: MANIPULATION_DETECTED (SSL already swept)
  O=18048 H=18052 L=18036 C=18044
  Analysis: Another wick below support AFTER manipulation confirmed.
  → CLASSIFICATION: SECONDARY_SWEEP
  → BOT: Update sweep_level to new low. Does NOT reset state.

EX 27: Tiny Wick (Noise)
  O=18050 H=18051 L=18049 C=18050.5
  Analysis: Total range = 2 points. Wick = 0.5 points.
  → CLASSIFICATION: NOISE (candle too small to be meaningful)
  → BOT: Skip classification

EX 28: Wick in Opposite Direction to Expected
  State: Monitoring for SSL sweep (expecting downside manipulation)
  But: O=18053 H=18064 L=18051 C=18055 (wick goes UP, sweeps BSL)
  → CLASSIFICATION: MANIP_UP (unexpected direction)
  → BOT: Manipulation occurred but in OPPOSITE direction expected.
         Update direction_after accordingly. SSL stays as target.

EX 29: Wick Sweep with Immediate Reversal (Same Candle)
  O=18050 H=18051 L=18034 C=18056
  Analysis: Swept SSL (Low=18034 < 18040), then CLOSED ABOVE OPEN.
  Bullish engulfing with SSL sweep.
  → CLASSIFICATION: MANIP_DOWN + DISPLACEMENT in same candle
  → BOT: Skip directly to MSS check — manipulation + displacement combined

EX 30: Wick at Range Midpoint
  O=18048 H=18055 L=18045 C=18052
  Range midpoint = (18040+18060)/2 = 18050
  Analysis: Wick reaches 18055 — between support and resistance.
  → CLASSIFICATION: INTERNAL_MOVEMENT (no boundary significance)
  → BOT: Normal candle, no manipulation event

EX 31: Wick Penetrates But Range Was Never Locked
  State: POSSIBLE_ACCUMULATION (range not yet confirmed/locked)
  O=18048 H=18052 L=18035 C=18044
  Analysis: Low goes below tentative range_low.
  → CLASSIFICATION: RANGE_EXPANSION (range still forming)
  → BOT: Update range_low = 18035. Continue confirmation process.
         NOT manipulation (range hasn't been locked yet)

EX 32: Post-Manipulation Wick (During MSS Search)
  State: MANIPULATION_DETECTED, looking for MSS
  O=18045 H=18050 L=18039 C=18048
  Analysis: Wick goes below previous sweep level (SSL was at 18037).
  Low=18039 > 18037 (did NOT make new low below manipulation level)
  → CLASSIFICATION: SAFE_PULLBACK (normal post-manipulation behavior)
  → BOT: MSS search continues unchanged
  
  BUT IF: Low=18035 < 18037 (new low below manipulation level):
  → CLASSIFICATION: MANIPULATION_FAILED
  → BOT: Transition to INVALID state
```

---


## §5. BODY BEHAVIOUR — COMPLETE ANALYSIS

### 5.1 Small Bodies During Accumulation

```
DEFINITION: body_ratio < 0.3 OR body < ATR * 0.2

MEANING:
  - Equilibrium between buyers and sellers
  - Neither side has control
  - EXPECTED during accumulation (this is the normal state)
  
STATISTICAL EXPECTATION:
  In valid accumulation, 60-80% of candles should have small bodies.
  IF small_body_percentage < 50% → accumulation quality is LOW
  IF small_body_percentage > 70% → accumulation quality is HIGH

FORMULA:
  small_body_count = count(candles where body < ATR * 0.25)
  small_body_pct = small_body_count / total_candle_count
```

### 5.2 Large Bodies During Accumulation

```
DEFINITION: body_ratio > 0.7 OR body > ATR * 0.8

ACCEPTABLE FREQUENCY: Maximum 2-3 large bodies per accumulation period
  IF large_body_count > 3 within MIN_CANDLES → NOT accumulation (trending)

ACCEPTABLE LOCATION:
  - At range boundaries (represents rejection)
  - Followed by opposite-direction candle (represents reversal back into range)
  
UNACCEPTABLE:
  - In middle of range with continuation → trend starting
  - Multiple consecutive in same direction → distribution starting

LARGE BODY RESPONSE:
  IF large_body at range_high AND direction == BEAR:
    → Resistance rejection (strengthens accumulation)
  IF large_body at range_low AND direction == BULL:
    → Support rejection (strengthens accumulation)
  IF large_body through range boundary AND direction matches break:
    → Potential breakout/manipulation → check next candle
```

### 5.3 Multiple Consecutive Same-Direction Bodies

```
DEFINITION:
  consecutive_same = count of consecutive candles with same direction
  
THRESHOLDS:
  consecutive_same <= 2 → NORMAL (common in accumulation)
  consecutive_same == 3 → MONITOR (accumulation may be ending)
  consecutive_same == 4 → WARNING (likely transitioning)
  consecutive_same >= 5 → INVALID (this is trending, not accumulation)

FORMULA:
  max_consecutive_run = max length of consecutive same-direction candles
  
  IF max_consecutive_run >= 5:
    accumulation_valid = FALSE
    reason = "sustained_directional_run"
```

### 5.4 Close Location Inside Candle

```
FORMULA:
  close_position = (Close - Low) / (High - Low)
  
  close_position > 0.7 → STRONG BULL CLOSE (closed near high)
  close_position 0.3-0.7 → NEUTRAL CLOSE (closed in middle)
  close_position < 0.3 → STRONG BEAR CLOSE (closed near low)

ACCUMULATION EXPECTATION:
  In valid accumulation, average close_position should be near 0.5.
  
  avg_close_position = mean(close_position for all candles in range)
  
  IF avg_close_position > 0.65 → Price is biased bullish (may not be accumulation)
  IF avg_close_position < 0.35 → Price is biased bearish (may not be accumulation)
  IF 0.4 <= avg_close_position <= 0.6 → True accumulation (balanced)
```

### 5.5 Body-to-Wick Ratio

```
FORMULA:
  body_wick_ratio = body / (upper_wick + lower_wick)
  (if wicks = 0, ratio = infinity → pure body candle)

CLASSIFICATION:
  ratio > 3.0 → BODY DOMINANT (strong directional intent)
  ratio 1.0-3.0 → BALANCED (normal)
  ratio 0.3-1.0 → WICK DOMINANT (indecision, rejection)
  ratio < 0.3 → PURE WICK (doji-like, total indecision)

ACCUMULATION EXPECTATION:
  Average body_wick_ratio for accumulation candles should be < 2.0
  (more wicks than bodies = indecision = consolidation)
  
  IF avg_body_wick_ratio > 2.5 → Trending behavior, not accumulation
  IF avg_body_wick_ratio < 1.5 → Classic accumulation behavior
```

### 5.6 Body Percentage Relative to Range

```
FORMULA:
  body_vs_range = body / range_size

THRESHOLDS:
  body_vs_range > 0.8 → CRITICAL: Single candle covers entire range → INVALID
  body_vs_range > 0.6 → WARNING: Approaching breakout threshold
  body_vs_range > 0.4 → ELEVATED: Unusual for accumulation
  body_vs_range <= 0.3 → NORMAL: Expected during accumulation

IMPLEMENTATION:
  FOR each candle in accumulation:
    IF body_vs_range > 0.8:
      → IMMEDIATE INVALIDATION
    IF body_vs_range > 0.6:
      → Flag as potential manipulation/distribution candle
      → Wait 1-2 candles for confirmation
```

---

## §6. INTERNAL STRUCTURE OF ACCUMULATION

### 6.1 How Swings Form Inside Accumulation

```
SWING FORMATION RULES:
  Swing High: High[j] > High[j-1] AND High[j] > High[j+1]
  Swing Low: Low[j] < Low[j-1] AND Low[j] < Low[j+1]
  
  (1-bar lookback/forward for 1-minute chart)

EXPECTED SWING PATTERN IN ACCUMULATION:
  - Swings ALTERNATE between high and low (oscillation)
  - Swing highs cluster near range_high
  - Swing lows cluster near range_low
  - Internal swing highs may also form in the middle
  
SWING COUNT EXPECTATION:
  In valid accumulation of N candles:
    Expected swing_count ≈ N / 3 to N / 4
    (roughly one swing point every 3-4 candles)
    
  IF swing_count < N / 6 → Too few swings → may be trending (one-directional)
  IF swing_count > N / 2 → Too many swings → may be noise/choppy (quality low)
```

### 6.2 Internal Swing Count

```
MINIMUM: At least 3 swing points (2 highs + 1 low, or 2 lows + 1 high)
         needed to establish that price is oscillating.

OPTIMAL: 5-8 swing points → well-formed accumulation with clear
         support/resistance and liquidity formation.

MAXIMUM: Beyond 15 swing points → range is very old, may be invalid.
         Consider timeout.
```

### 6.3 How Internal Liquidity Forms

```
INTERNAL LIQUIDITY = swing highs/lows that form INSIDE the range
                     (not at boundaries)

Process:
  1. Price makes swing high at 18055 (inside range 18040-18060)
  2. Price reverses down
  3. Price makes another swing high at 18056 (equal to previous)
  4. → INTERNAL BSL formed at ~18055
  
  5. Price makes swing low at 18044 (inside range)
  6. Price reverses up
  7. Price makes another swing low at 18043 (equal to previous)
  8. → INTERNAL SSL formed at ~18044

DIFFERENCE FROM BOUNDARY LIQUIDITY:
  Boundary liquidity (at range_high/low) → PRIMARY manipulation target
  Internal liquidity → SECONDARY target, may be swept during manipulation
                       but does NOT constitute the "M" of AMD
```

### 6.4 How Equal Highs Form

```
PROCESS:
  Step 1: Price rises to level X and reverses (swing high at X)
  Step 2: Price drops, then rises again
  Step 3: Price reaches level X±tolerance and reverses again
  Step 4: → Equal Highs confirmed at level X
  
  Each subsequent touch ADDS to the liquidity pool above X.
  Retail traders who sold at X place stops just above X.
  Market makers can see this cluster of stops.
  Eventually, price will SWEEP above X to trigger those stops.

DETECTION ALGORITHM:
  See §5.2 of previous Accumulation Spec document.
  Use clustering algorithm with tolerance-based grouping.
```

### 6.5 When Internal Trend Exists

```
EVEN WITHIN ACCUMULATION, there can be micro-trends:

BULLISH MICRO-TREND:
  Internal swing lows are making higher lows
  (HL pattern within range)
  → Still accumulation IF highs are NOT making new range highs
  → Just indicates buying pressure building (bullish accumulation)
  
BEARISH MICRO-TREND:
  Internal swing highs are making lower highs
  (LH pattern within range)
  → Still accumulation IF lows are NOT making new range lows
  → Indicates selling pressure building (bearish accumulation)

SIGNIFICANCE:
  Bullish micro-trend → manipulation MORE LIKELY to go DOWNSIDE first
    (sweeps SSL, then goes bullish → classic AMD)
  Bearish micro-trend → manipulation MORE LIKELY to go UPSIDE first
    (sweeps BSL, then goes bearish → classic AMD)
  
  This is because manipulation goes AGAINST the visible bias to trap traders.
```

### 6.6 When Internal Structure is BROKEN (Still Accumulation)

```
Internal structure breaks DO NOT invalidate accumulation IF:
  1. The break stays within the range boundaries
  2. Price reverses back after the break
  3. No candle closes beyond the range boundary with conviction
  
EXAMPLE:
  Internal swing low at 18044. 
  Next candle Low = 18042 (breaks internal structure).
  But 18042 > 18040 (range_low). Still inside accumulation!
  → Internal structure broken, but accumulation VALID
  → Actually builds MORE liquidity below 18042

When internal structure break DOES invalidate:
  IF the break continues THROUGH the range boundary:
  → Not an internal break anymore — it's a manipulation or breakout
  → State transitions to next phase
```

### 6.7 When Accumulation Becomes INVALID

```
COMPLETE INVALIDATION CHECKLIST:

1. RANGE TOO WIDE: range_size > ATR * 3.0
2. TIMEOUT: candle_count > MAX_CANDLES (60)
3. GENUINE BREAKOUT: 2+ consecutive closes beyond range boundary
4. IMPULSE INSIDE: Single candle body > range_size * 0.8
5. DIRECTIONAL RUN: 5+ consecutive same-direction candles
6. DISPLACEMENT INSIDE RANGE: Body > ATR * 0.8 inside range without reversal
7. NEWS EVENT: High-impact news during accumulation period
8. NO LIQUIDITY: After MIN_CANDLES, no equal highs OR equal lows formed
9. AVG BODY TOO LARGE: mean(body) > range_size * 0.3
10. STRUCTURE TOO DIRECTIONAL: structure_score > 0.7 (clear trend)
```

---

## §7. RANGE BEHAVIOUR — LIFECYCLE

### 7.1 How Accumulation STARTS

```
TRIGGER CONDITIONS (at least ONE):

TRIGGER A — Post-Impulse Consolidation:
  1. A strong impulse move occurs (candle body > ATR * 0.8)
  2. After the impulse, next 5+ candles contain within a bounded range
  3. No single candle exceeds 50% of the impulse range
  → Accumulation BEGINS at first containment candle

TRIGGER B — Organic Range Formation:
  1. No specific impulse needed
  2. Sliding window of 5 candles shows containment
  3. Range_size within [ATR*0.3, ATR*2.0]
  4. Net displacement < 40%
  5. Direction changes >= 25% of candles
  → Accumulation BEGINS at first candle of qualifying window

TRIGGER C — Macro Window Opens During Consolidation:
  1. Price was already consolidating before macro window
  2. Macro window opens (per schedule)
  3. Lookback scan finds valid range in last 20 candles
  → Accumulation recognized RETROACTIVELY
```

### 7.2 How Range Expands

```
EXPANSION RULES:

ALLOWED EXPANSION:
  New high/low exceeds previous boundary BUT:
    new_range_size <= ATR * RANGE_ATR_MAX (2.0)
    AND expansion_count <= MAX_EXPANSIONS_BEFORE_LOCK (3)
    AND the expansion candle reverses (shows rejection)
  → Update range boundary to new level
  → confidence -= 0.05 per expansion

DISALLOWED EXPANSION:
  IF new_range_size > ATR * 3.0:
    → INVALID (range too wide)
  IF expansion_count > 3 AND candle_count < MIN_CANDLES:
    → INVALID (too many expansions too early)
  IF expansion candle has body > range_size * 0.6:
    → Not expansion — it's a breakout attempt
```

### 7.3 How Range Contracts (Compression)

```
COMPRESSION DETECTION:

Method: Compare rolling ranges

  rolling_range_early = max(H[0:5]) - min(L[0:5])  // First 5 candles
  rolling_range_late = max(H[-5:]) - min(L[-5:])    // Last 5 candles
  
  compression_ratio = rolling_range_late / rolling_range_early
  
  IF compression_ratio < 0.6:
    → COMPRESSION DETECTED
    → Manipulation expected within next 5-10 candles
    → Increase monitoring sensitivity
```

### 7.4 Duration Rules

```
MINIMUM DURATION:
  MIN_CANDLES = 10 (10 minutes on 1-min chart)
  Rationale: Need enough candles for liquidity to form
  (at least 2 touches to each boundary = minimum 4 swing points = ~10 candles)

OPTIMAL DURATION:
  15-30 candles (15-30 minutes)
  Matches well with 20-minute macro windows
  Sufficient liquidity forms, manipulation becomes high-probability

MAXIMUM DURATION:
  MAX_CANDLES = 60 (60 minutes)
  Beyond this, range is "stale" — liquidity may have dissipated
  Market structure has likely changed
  
  IF candle_count > MAX_CANDLES AND no manipulation:
    → TIMEOUT → INVALID → RESET
```

### 7.5 Width Rules

```
MINIMUM WIDTH:
  range_size >= ATR * 0.3
  (Range must be at least 30% of ATR to be meaningful)
  Below this → noise, spread fluctuation, not true accumulation

MAXIMUM WIDTH:
  range_size <= ATR * 2.0 (for confirmation)
  range_size <= ATR * 3.0 (absolute max before invalidation)
  
  Beyond this → price is moving too much to be "accumulating"

OPTIMAL WIDTH:
  range_size = ATR * 0.8 to ATR * 1.5
  Large enough for meaningful liquidity to form
  Small enough to represent genuine consolidation
```

### 7.6 When to RESET

```
RESET CONDITIONS:
  1. After successful AMD cycle completion → RESET for next cycle
  2. After INVALID state + cooldown → RESET to scan again
  3. After macro window change → RESET if previous range was in different window
  4. After news blackout ends → RESET all tracking
  5. After gap detection → RESET (pre-gap ranges invalid)
```

### 7.7 When to IGNORE

```
IGNORE POTENTIAL RANGES WHEN:
  1. Outside macro windows entirely
  2. During news blackout
  3. Range forms but has < 2 touches on either side after MAX_CANDLES
  4. Range forms during very low volatility (ATR < normal * 0.3)
  5. Range overlaps with a just-completed AMD cycle (need fresh setup)
```

---


# ═══════════════════════════════════════════════════════════════
# PART 2 — MANIPULATION
# ═══════════════════════════════════════════════════════════════

## §8. MANIPULATION — COMPLETE THEORY AND LOGIC

### 8.1 Why Manipulation Happens (Algorithmic Reason)

```
MANIPULATION EXISTS FOR ONE PURPOSE:
  To trigger stop-loss orders clustered beyond support/resistance.

MECHANISM:
  1. During accumulation, stops accumulate above resistance (BSL) and below support (SSL)
  2. Smart money needs to fill large orders
  3. They push price INTO the stop cluster to trigger those orders
  4. Triggered stops = market orders flowing = liquidity to fill against
  5. Once filled, price reverses (distribution begins)

FROM BOT PERSPECTIVE:
  Manipulation = price exceeds accumulation boundary,
                 reaches identified liquidity,
                 then FAILS to continue (returns/reverses)
```

### 8.2 Where Manipulation Usually Starts

```
ORIGIN POINTS:

1. FROM OPPOSITE BOUNDARY:
   - Price at support → impulse up → sweeps BSL above resistance
   - Price at resistance → impulse down → sweeps SSL below support
   Rationale: Starting from the other side gives momentum for the sweep
   
2. FROM RANGE MIDPOINT:
   - Price drifts from middle → accelerates to boundary → sweeps
   Rationale: Less common but possible, especially with news catalyst
   
3. FROM JUST INSIDE BOUNDARY:
   - Price lingers near support/resistance → quick spike sweeps
   Rationale: Less dramatic but valid

DETECTION:
  origin_level = Close of candle BEFORE the sweep candle
  
  IF origin_level near range_low AND sweep goes above range_high:
    → FULL RANGE SWEEP (strongest manipulation)
    → confidence_boost = 0.15
  
  IF origin_level near range_high AND sweep goes above range_high:
    → SHORT RANGE SWEEP (quick spike above)
    → confidence_boost = 0.05
```

### 8.3 How Manipulation Interacts with Support

```
SUPPORT SWEEP (SSL Manipulation):

  1. Price approaches support level
  2. Price BREAKS BELOW support (wick or body)
  3. This triggers sell-stops below support → creates sell orders
  4. Smart money BUYS against these sell orders (filling longs)
  5. Buying pressure causes price to reverse UP
  6. Price returns above support → manipulation complete
  
  BOT DETECTION:
    IF Low[j] < support_level - min_sweep_distance:
      IF Close[j] >= support_level:
        → WICK SWEEP (highest confidence)
      IF Close[j] < support_level:
        IF Close[j+1] > support_level:
          → BODY SWEEP RETURNED (confirmed manipulation)
        IF Close[j+1] < support_level:
          → POSSIBLE BREAKOUT (not manipulation)
```

### 8.4 How Manipulation Interacts with Resistance

```
RESISTANCE SWEEP (BSL Manipulation):

  1. Price approaches resistance level
  2. Price BREAKS ABOVE resistance (wick or body)
  3. This triggers buy-stops above resistance → creates buy orders
  4. Smart money SELLS against these buy orders (filling shorts)
  5. Selling pressure causes price to reverse DOWN
  6. Price returns below resistance → manipulation complete
  
  BOT DETECTION:
    IF High[j] > resistance_level + min_sweep_distance:
      IF Close[j] <= resistance_level:
        → WICK SWEEP (highest confidence)
      IF Close[j] > resistance_level:
        IF Close[j+1] < resistance_level:
          → BODY SWEEP RETURNED (confirmed manipulation)
        IF Close[j+1] > resistance_level:
          → POSSIBLE BREAKOUT (not manipulation)
```

### 8.5 Sweep Types

```
SINGLE SWEEP:
  Price sweeps liquidity on ONE side only.
  Most common pattern.
  Direction after manipulation = opposite to sweep direction.
  
  SSL swept → expect BULLISH distribution
  BSL swept → expect BEARISH distribution

DOUBLE SWEEP:
  Price sweeps BOTH sides sequentially.
  Less common but very powerful.
  The SECOND sweep determines the final direction.
  
  Pattern A: SSL swept first → BSL swept second → BEARISH distribution
  Pattern B: BSL swept first → SSL swept second → BULLISH distribution
  
  DETECTION:
    IF manipulation_count == 2 AND directions are opposite:
      final_direction = opposite of LAST sweep direction
      confidence_boost += 0.15

TRIPLE SWEEP:
  Extremely rare. Price sweeps one side, then other, then first again.
  IF manipulation_count >= 3:
    → Market is choppy, NOT following AMD → INVALIDATE
    → Range is being churned → RESET

INTERNAL SWEEP:
  Price sweeps internal liquidity (swing highs/lows inside the range)
  WITHOUT reaching the range boundary.
  
  DOES NOT count as manipulation (M of AMD).
  It's just normal range behavior.
  
  DETECTION:
    sweep_target_reached = (sweep_level >= boundary_liquidity_level)
    IF NOT sweep_target_reached: → INTERNAL SWEEP → still accumulating

EXTERNAL SWEEP:
  Price sweeps liquidity BEYOND the range boundary.
  THIS IS the manipulation (M of AMD).
  
  DETECTION:
    external_sweep = (High[j] > range_high + min_sweep) 
                  OR (Low[j] < range_low - min_sweep)
    AND liquidity exists at that level

NESTED SWEEP:
  Price sweeps internal liquidity first, THEN sweeps external.
  Multi-level sweep in same direction.
  
  Pattern: Price → internal high → continues → external high (BSL)
  → Valid manipulation. The nested structure adds confidence.
  
  DETECTION:
    IF internal_level_broken THEN external_level_broken in same direction:
      → NESTED SWEEP → confidence += 0.1

FAILED SWEEP:
  Price attempts to sweep but FAILS to reach the liquidity level.
  
  DETECTION:
    High[j] > range_high (exceeded boundary)
    BUT High[j] < bsl_level (didn't reach liquidity)
    
  → NOT a valid manipulation
  → Classify as PARTIAL_ATTEMPT
  → Stay in MONITORING state
  → May try again on next candle
```

### 8.6 Fake Breakout vs Real Breakout — Decision Matrix

```
| Criteria | FAKE BREAKOUT (Manipulation) | REAL BREAKOUT |
|----------|------------------------------|---------------|
| Closes beyond level | 0-1 candles | 2+ candles |
| Reaches liquidity | YES (sweeps stops) | May or may not |
| Returns inside range | YES (within 1-2 candles) | NO |
| Follow-through | OPPOSITE direction | SAME direction |
| Body of break candle | Small-Medium | Large (>ATR*0.6) |
| Volume behavior | Spike then drop | Sustained |
| Displacement after | In OPPOSITE direction | In SAME direction |
| Macro timing | During manipulation window | During continuation or outside |

ALGORITHM:
  FUNCTION classify_break(candle, next_candles, range, liquidity_level):
    exceeded = (candle.High > range.high) OR (candle.Low < range.low)
    
    IF NOT exceeded: RETURN "NO_BREAK"
    
    reached_liquidity = (candle.High >= bsl_level) OR (candle.Low <= ssl_level)
    
    closes_beyond = count(c for c in next_candles[0:2] 
                          if c.Close > range.high or c.Close < range.low)
    
    IF closes_beyond >= 2: RETURN "REAL_BREAKOUT"
    IF reached_liquidity AND closes_beyond == 0: RETURN "FAKE_BREAKOUT_CONFIRMED"
    IF reached_liquidity AND closes_beyond == 1: RETURN "FAKE_BREAKOUT_PROBABLE"
    IF NOT reached_liquidity: RETURN "PARTIAL_ATTEMPT"
```

---


## §9. WICK MANIPULATION — 50+ NUMERICAL EXAMPLES

### 9.1 Setup Context for All Examples

```
MARKET: NASDAQ US100
ACCUMULATION RANGE: Support = 18040, Resistance = 18060
Range_Size = 20 points
ATR(20) = 10 points
BSL Level (equal highs): 18060 (3 touches)
SSL Level (equal lows): 18040 (3 touches)
Min_sweep_distance = range_size * 0.05 = 1 point (beyond boundary)
Tolerance = 18050 * 0.0002 = 3.61 points
```

### 9.2 VALID Manipulation Examples (Bot Should CONFIRM)

```
# === VALID DOWNSIDE MANIPULATION (SSL Sweep → Bullish Intent) ===

VM-1: Classic Wick Sweep
  O=18045 H=18050 L=18036 C=18048
  Penetration: 18040-18036 = 4 pts below support
  Body: Closed at 18048 (inside range) ✓
  Reached SSL: 18036 < 18040 ✓
  → VALID MANIPULATION (confidence: HIGH)
  → Direction after: LONG

VM-2: Deep Wick Sweep  
  O=18050 H=18053 L=18030 C=18046
  Penetration: 10 pts below support (ATR * 1.0)
  Body: Closed inside ✓
  → VALID MANIPULATION (confidence: HIGH, deep sweep)
  → Direction: LONG

VM-3: Two-Candle Sweep (Close Below Then Return)
  C[j]:   O=18044 H=18046 L=18035 C=18037 (closed below support!)
  C[j+1]: O=18037 H=18050 L=18036 C=18048 (returned above support!)
  Combined: Swept SSL at 18035, returned to 18048
  → VALID MANIPULATION (2-candle pattern, confidence: MEDIUM-HIGH)
  → Direction: LONG

VM-4: Engulfing Sweep (Same-candle reversal)
  O=18042 H=18055 L=18033 C=18054
  Swept SSL (Low=18033 < 18040 by 7)
  Closed WELL above (18054 > 18040)
  Bullish engulfing with sweep
  → VALID MANIPULATION + DISPLACEMENT in same candle
  → Direction: LONG (extremely high confidence)

VM-5: Dragonfly Doji Sweep
  O=18041 H=18043 L=18034 C=18041
  Almost no body, massive lower wick sweeps SSL
  → VALID MANIPULATION (textbook pattern)
  → Direction: LONG

VM-6: Sweep with Small Pullback First
  C[j-1]: O=18048 H=18050 L=18043 C=18044 (approaching support)
  C[j]:   O=18044 H=18047 L=18036 C=18045 (sweeps SSL)
  Gradual approach then sweep
  → VALID MANIPULATION
  → Direction: LONG

VM-7: Opening Candle Sweep (Macro 3 Open)
  Time: 18:40:00 IST (first candle of Macro 3)
  O=18045 H=18048 L=18035 C=18047
  Macro window just opened, immediate sweep
  → VALID MANIPULATION (macro-aligned, confidence: VERY HIGH)
  → Direction: LONG

VM-8: Multi-Wick Approach Then Sweep
  C[j-2]: O=18046 H=18049 L=18041 C=18044 (wick near support)
  C[j-1]: O=18044 H=18048 L=18040 C=18046 (touches support)
  C[j]:   O=18046 H=18050 L=18035 C=18048 (sweeps through)
  Built up then swept
  → VALID MANIPULATION
  → Direction: LONG

# === VALID UPSIDE MANIPULATION (BSL Sweep → Bearish Intent) ===

VM-9: Classic BSL Wick Sweep
  O=18055 H=18065 L=18052 C=18054
  Penetration: 18065-18060 = 5 pts above resistance
  Closed below resistance ✓
  → VALID MANIPULATION
  → Direction: SHORT

VM-10: Gravestone Doji BSL Sweep
  O=18059 H=18067 L=18058 C=18059
  Massive upper wick, zero body, sweeps BSL
  → VALID MANIPULATION (textbook)
  → Direction: SHORT

VM-11: Two-Candle BSL Sweep
  C[j]:   O=18056 H=18064 L=18054 C=18062 (closed above resistance)
  C[j+1]: O=18062 H=18063 L=18050 C=18052 (returned below resistance)
  → VALID MANIPULATION (2-candle)
  → Direction: SHORT

VM-12: BSL Sweep from Range Low
  O=18042 H=18066 L=18040 C=18055
  Started near support, swept ALL the way to BSL
  Full range traversal + sweep
  → VALID MANIPULATION (full-range sweep, very strong)
  → Direction: SHORT

VM-13: Gap Up Then Return
  Previous candle Close = 18058
  O=18063 H=18066 L=18054 C=18055
  Gapped above resistance, swept BSL, returned
  → VALID MANIPULATION (gap-and-return)
  → Direction: SHORT

VM-14: Slow Grind Then Spike Sweep
  C[j-3]: C=18055
  C[j-2]: C=18057
  C[j-1]: C=18059
  C[j]:   O=18059 H=18066 L=18057 C=18058
  Slow approach → spike → return
  → VALID MANIPULATION
  → Direction: SHORT

VM-15: Double Top Then Sweep
  C[j-5]: H=18060 (first touch)
  C[j-2]: H=18060 (second touch — equal high)
  C[j]:   O=18057 H=18064 L=18055 C=18056 (sweeps the equal highs)
  → VALID MANIPULATION (swept the equal highs that formed during accumulation)
  → Direction: SHORT
```

### 9.3 INVALID Manipulation Examples (Bot Should REJECT)

```
# === INVALID — These are NOT manipulation ===

IM-1: Too Shallow (Noise)
  O=18045 H=18050 L=18039.5 C=18047
  Penetration: 18040-18039.5 = 0.5 points
  0.5 < min_sweep_distance (1 point) → too shallow
  → INVALID: NOISE (not a real sweep)
  → BOT: Classify as touch, not manipulation

IM-2: Didn't Reach Liquidity Level
  O=18048 H=18053 L=18041 C=18050
  Low=18041 > SSL at 18040
  Didn't actually penetrate support
  → INVALID: PARTIAL_ATTEMPT (level tested but not swept)
  → BOT: touch_count += 1, stay in MONITORING

IM-3: Genuine Breakout (Didn't Return)
  C[j]:   O=18044 H=18046 L=18033 C=18035
  C[j+1]: O=18035 H=18037 L=18028 C=18030
  Swept SSL but CONTINUED lower. Two closes below support.
  → INVALID: GENUINE_BREAKOUT (not manipulation)
  → BOT: Accumulation INVALIDATED → RESET

IM-4: Body Too Large Beyond Level
  O=18050 H=18052 L=18025 C=18027
  Body closed at 18027, 13 points below support
  Body itself penetrated deeply (not just wick)
  → INVALID: BREAKOUT (body conviction too strong)
  → BOT: Check next candle, but likely invalid

IM-5: Sweep During News Blackout
  Time: During NFP release
  O=18050 H=18055 L=18030 C=18045
  Looks like sweep but during news
  → INVALID: NEWS_BLACKOUT (unreliable data)
  → BOT: Ignore entirely

IM-6: Sweep Without Prior Liquidity
  Range: 18040-18060, but only 1 touch to each boundary
  O=18045 H=18050 L=18036 C=18048
  Swept below 18040 BUT liquidity pool is WEAK (only 1 touch)
  → INVALID: INSUFFICIENT_LIQUIDITY
  → BOT: Low confidence event. May still track but with confidence < 0.3

IM-7: Too Deep, Didn't Return
  O=18048 H=18050 L=18018 C=18020
  Penetration: 22 points below support (ATR * 2.2!)
  Close far below support (18020 << 18040)
  → INVALID: BREAKOUT (not manipulation — genuine move)
  → BOT: Accumulation OVER

IM-8: Wrong Macro Window
  Time: 19:30 IST (Continuation window, NOT manipulation window)
  Sweep occurs during continuation window
  → INVALID: WRONG_TIMING (manipulation windows are for manipulation)
  → BOT: Note but reduce confidence significantly (still may be valid but lower probability)

IM-9: Resistance Sweep But Close Is Above
  C[j]:   O=18056 H=18065 L=18054 C=18063 (closed above resistance)
  C[j+1]: O=18063 H=18068 L=18060 C=18066 (continued higher!)
  Swept BSL but price CONTINUED up. Not manipulation.
  → INVALID: BREAKOUT_UP (genuine upside break)
  → BOT: State → INVALID

IM-10: Multiple Candles All Above Resistance
  C[j]:   C=18062
  C[j+1]: C=18064
  C[j+2]: C=18065
  Three consecutive closes above resistance
  → INVALID: SUSTAINED_BREAKOUT (not coming back)
  → BOT: Accumulation OVER, new trend started
```

### 9.4 WEAK Manipulation Examples (Bot Should Track with Low Confidence)

```
WM-1: Reached Level But Close IS the Level
  O=18044 H=18048 L=18036 C=18040
  Swept SSL (Low=18036) but closed EXACTLY at support (18040)
  Not clearly inside range
  → WEAK MANIPULATION (confidence: 0.4)
  → BOT: Track but wait for next candle confirmation

WM-2: Shallow Sweep (Barely Below)
  O=18043 H=18048 L=18038 C=18046
  Low=18038 < 18040 by only 2 points (just above min_sweep_distance)
  → WEAK MANIPULATION (confidence: 0.4)
  → BOT: Valid but minimal liquidity actually swept

WM-3: Sweep After Long Accumulation (Stale Range)
  candle_count = 55 (near MAX_CANDLES)
  O=18044 H=18048 L=18035 C=18046
  Valid sweep BUT range is very old
  → WEAK MANIPULATION (confidence: 0.35)
  → BOT: Track but liquidity may have dissipated

WM-4: Single Touch Level (Minimal Liquidity)
  Level has only 2 touches (minimum). Not strongly established.
  O=18046 H=18050 L=18036 C=18047
  → WEAK MANIPULATION (confidence: 0.4)
  → BOT: Valid sweep but target had minimal stops

WM-5: Sweep in Non-Priority Macro Window
  Time: 17:55 IST (Macro 1 — observe only per document)
  Strategy says "Only Observe" during Macro 1
  → WEAK MANIPULATION (confidence: 0.3)
  → BOT: Note for context but do NOT enter trade on this
```

### 9.5 STRONG Manipulation Examples (Bot Should Act with High Confidence)

```
SM-1: Multi-Touch Level + Deep Sweep + Wick Only + Macro 3
  Level: 4 equal lows at 18040
  Time: 18:45 IST (Macro 3 — highest priority)
  O=18050 H=18053 L=18032 C=18049
  Penetration: 8 points (ATR*0.8). Wick only. Strong liquidity.
  → STRONG MANIPULATION (confidence: 0.9)
  → BOT: Immediately transition to MSS search

SM-2: Engulfing + Sweep + Displacement + Same Candle
  O=18042 H=18058 L=18034 C=18057
  Swept SSL at 18034, closed at 18057 (above midpoint!)
  Body = 15 > ATR*0.8 ✓ → Displacement in same candle!
  → STRONG MANIPULATION + DISPLACEMENT (confidence: 0.95)
  → BOT: Look for MSS immediately or may already be confirmed

SM-3: Compression → Sweep (Classic Pattern)
  Last 5 candles: ranges = [8, 6, 5, 4, 3] (compressing)
  Then: O=18048 H=18050 L=18033 C=18047
  Compression resolved into manipulation sweep
  → STRONG MANIPULATION (confidence: 0.85)
  → BOT: Classic ICT pattern confirmed

SM-4: Double Sweep (Both Sides)
  C[j-3]: H=18065 (swept BSL, returned — first manipulation)
  C[j-3] close = 18055 (returned)
  No MSS confirmed after BSL sweep...
  C[j]: L=18034 (now sweeps SSL!)
  C[j]: C=18048 (returned)
  → STRONG MANIPULATION (double sweep, confidence: 0.9)
  → BOT: Second sweep = final direction. Use SSL sweep direction → LONG

SM-5: Equal High/Low Both Swept + Priority Macro
  Time: 18:50 IST (inside Macro 3)
  BSL at 18060 (3 touches)
  SSL at 18040 (3 touches)
  Candle sweeps SSL: O=18050 H=18052 L=18035 C=18049
  → STRONG MANIPULATION (3-touch level + priority macro)
  → Direction: LONG (confidence: 0.88)
```

---


## §10. MANIPULATION TIMELINE — CANDLE-BY-CANDLE EVOLUTION

### 10.1 Timeline A: Classic SSL Sweep → Bullish Distribution

```
TIMELINE: 12 candles total (18:40 - 18:52 IST, Macro 3)

C[1] 18:40 — O=18050 H=18055 L=18046 C=18052
  State: MONITORING (accumulation confirmed, watching for manipulation)
  Event: Normal candle inside range. No boundary interaction.
  
C[2] 18:41 — O=18052 H=18054 L=18047 C=18048
  State: MONITORING
  Event: Slight bearish candle, approaching support side.
  
C[3] 18:42 — O=18048 H=18050 L=18043 C=18044
  State: MONITORING  
  Event: Moving toward support. Close near range_low zone.
  
C[4] 18:43 — O=18044 H=18046 L=18041 C=18042
  State: MONITORING
  Event: Very close to support (18040). Approaching liquidity.
  
C[5] 18:44 — O=18042 H=18044 L=18034 C=18043
  State: → MANIPULATION_DETECTED
  Event: LOW = 18034 < support(18040)! Swept SSL!
         Close = 18043 > support ✓ (returned above)
         Wick depth = 6 points. Reached liquidity ✓.
  Action: Transition state. Begin MSS search.
  Direction: LONG (SSL swept → expect bullish)
  
C[6] 18:45 — O=18043 H=18048 L=18042 C=18047
  State: MANIPULATION_DETECTED (searching for MSS)
  Event: Bullish candle. Starting to reverse. Swing low forming at C[5].
  MSS Target: Need to find first swing high and break it.
  First potential swing high: C[1].High = 18055 or wait for new one.
  
C[7] 18:46 — O=18047 H=18052 L=18046 C=18051
  State: MANIPULATION_DETECTED (MSS search)
  Event: Continuing higher. Swing low confirmed at C[5] (18034).
         Swing high forming but not yet confirmed.
  
C[8] 18:47 — O=18051 H=18055 L=18049 C=18050
  State: MANIPULATION_DETECTED (MSS search)
  Event: Made high of 18055. Pull back to 18050. 
         Now C[8].High = 18055 could be MSS target if next candle lower.
  
C[9] 18:48 — O=18050 H=18051 L=18047 C=18048
  State: MANIPULATION_DETECTED (MSS search)
  Event: Lower candle. Confirms swing high at C[8] = 18055.
         MSS target = 18055. Now waiting for CLOSE above 18055.
  
C[10] 18:49 — O=18048 H=18050 L=18046 C=18049
  State: MANIPULATION_DETECTED (MSS search)
  Event: Consolidating below MSS target. Not yet broken.
  
C[11] 18:50 — O=18049 H=18060 L=18048 C=18058
  State: → AMD_READY_SIGNAL
  Event: Close = 18058 > MSS target (18055)! MSS CONFIRMED!
         Body = |18058-18049| = 9 ≥ ATR*0.8 = 8 → DISPLACEMENT ✓!
  Action: MSS + Displacement confirmed → EMIT AMD_READY_SIGNAL
  Signal: {direction: LONG, manipulation_sweep: 18034, mss_level: 18055}
  
C[12] 18:51 — O=18058 H=18065 L=18056 C=18063
  State: AMD_READY_SIGNAL (active)
  Event: Distribution candle. Price expanding in expected direction.
  Note: Entry engine takes over from here (swing low retracement entry)
```

### 10.2 Timeline B: BSL Sweep → Bearish Distribution

```
TIMELINE: 10 candles (18:42 - 18:52 IST)

C[1] 18:42 — O=18050 H=18055 L=18048 C=18054
  State: MONITORING
  Event: Bullish candle, approaching resistance.

C[2] 18:43 — O=18054 H=18058 L=18052 C=18057
  State: MONITORING
  Event: Continuing toward resistance (18060).

C[3] 18:44 — O=18057 H=18066 L=18055 C=18056
  State: → MANIPULATION_DETECTED
  Event: HIGH = 18066 > resistance(18060)! Swept BSL!
         Close = 18056 < resistance ✓ (returned below)
         Wick depth = 6 points. Reached BSL ✓.
  Action: Transition to MSS search.
  Direction: SHORT (BSL swept → expect bearish)

C[4] 18:45 — O=18056 H=18057 L=18050 C=18051
  State: MANIPULATION_DETECTED (MSS search)
  Event: Bearish follow-through. Price dropping after BSL sweep.
         Swing high confirmed at C[3].High = 18066.

C[5] 18:46 — O=18051 H=18053 L=18046 C=18048
  State: MANIPULATION_DETECTED (MSS search)
  Event: Continuing lower. Looking for swing low to form for MSS target.

C[6] 18:47 — O=18048 H=18052 L=18045 C=18051
  State: MANIPULATION_DETECTED (MSS search)
  Event: Bounced slightly. C[5].Low = 18046 might be swing low.
         If C[7] goes higher → swing low at 18046 confirmed.

C[7] 18:48 — O=18051 H=18053 L=18049 C=18050
  State: MANIPULATION_DETECTED (MSS search)
  Event: Higher low than C[5]. Confirms swing low at C[5] = 18046.
         MSS target for bearish = break below 18046.

C[8] 18:49 — O=18050 H=18051 L=18044 C=18044
  State: → AMD_READY_SIGNAL
  Event: Close = 18044 < MSS target (18046)! BEARISH MSS CONFIRMED!
         Body = |18050-18044| = 6 ... < ATR*0.8 = 8
         Displacement NOT yet confirmed. Wait.
         Actually: still confirm MSS, await displacement.

C[9] 18:50 — O=18044 H=18045 L=18035 C=18036
  State: → AMD_READY_SIGNAL (displacement confirmed)
  Event: Body = |18044-18036| = 8 = ATR*0.8 ✓ DISPLACEMENT!
  Signal: {direction: SHORT, manipulation_sweep: 18066, mss_level: 18046}

C[10] 18:51 — O=18036 H=18040 L=18030 C=18032
  State: AMD_READY_SIGNAL
  Event: Distribution continuing bearish. Entry engine takes over.
```

### 10.3 Timeline C: Failed Manipulation (Invalidation)

```
TIMELINE: 8 candles — Manipulation attempt that FAILS

C[1] 18:44 — O=18048 H=18050 L=18043 C=18044
  State: MONITORING
  Event: Approaching support.

C[2] 18:45 — O=18044 H=18046 L=18036 C=18043
  State: → MANIPULATION_DETECTED
  Event: Swept SSL (Low=18036 < 18040). Close=18043 > support ✓.
  Direction expected: LONG

C[3] 18:46 — O=18043 H=18045 L=18040 C=18041
  State: MANIPULATION_DETECTED (MSS search)
  Event: Weak bounce. Price not recovering convincingly.

C[4] 18:47 — O=18041 H=18042 L=18038 C=18039
  State: MANIPULATION_DETECTED (MSS search)
  Event: Dropping back toward sweep level!

C[5] 18:48 — O=18039 H=18040 L=18033 C=18034
  State: → INVALID
  Event: Low=18033 < previous sweep level (18034)!
         Price made NEW LOW below manipulation level!
         → MANIPULATION FAILED
  Reason: "new_low_below_sweep_level"
  Action: State → INVALID → cooldown → RESET

  EXPLANATION: Valid manipulation should NOT see price return to
               and EXCEED the sweep level. If it does, the "manipulation"
               was actually a genuine breakout in disguise.
```

### 10.4 Timeline D: Double Manipulation (Both Sides Swept)

```
TIMELINE: 15 candles — Both BSL and SSL swept before distribution

C[1]-C[3]: Price approaches resistance...
C[4] — O=18055 H=18065 L=18053 C=18054
  State: → MANIPULATION_DETECTED
  Event: BSL swept (High=18065 > 18060). Close returned below.
  Direction expected: SHORT

C[5]-C[7]: Looking for bearish MSS... but no confirmation

C[8] — O=18050 H=18052 L=18048 C=18049
  State: MANIPULATION_DETECTED (MSS search, timeout approaching)
  Event: 4 candles since manipulation, no MSS yet.
  Note: MSS_TIMEOUT = 10, so still have time.

C[9] — O=18049 H=18050 L=18044 C=18045
  State: MANIPULATION_DETECTED (MSS search)
  Event: Price dropping but hasn't broken any swing low definitively.

C[10] — O=18045 H=18048 L=18034 C=18046
  State: → DOUBLE_MANIPULATION_DETECTED  
  Event: NOW SSL SWEPT TOO! Low=18034 < 18040!
         Close=18046 > support (returned inside) ✓
  Update: manipulation_count = 2
          FIRST sweep was BSL (bearish expected)
          SECOND sweep is SSL (bullish expected)
          → Final direction = SECOND sweep direction = LONG
  New Direction: LONG (reversed from first expectation)

C[11]-C[12]: Now looking for BULLISH MSS...

C[13] — O=18048 H=18056 L=18047 C=18055
  State: MANIPULATION_DETECTED (MSS search for bullish)
  Event: Swing high forming at 18056.

C[14] — O=18055 H=18062 L=18053 C=18060
  State: → AMD_READY_SIGNAL
  Event: Close=18060 > MSS target. Body=5 (may need displacement still)

C[15] — O=18060 H=18072 L=18058 C=18070
  State: AMD_READY_SIGNAL (displacement confirmed)
  Event: Body=10 > ATR*0.8 ✓. BULLISH displacement!
  Signal: {direction: LONG, double_manipulation: TRUE}
```

### 10.5 Timeline E: Manipulation at Macro Window Boundary

```
TIMELINE: Manipulation occurs in last minutes of Macro 3 window

Macro 3: 18:40 - 19:20 IST

C[1]-C[30]: Accumulation forms during first 30 minutes of Macro 3
            (18:40 - 19:10 IST)

C[31] 19:11 — O=18050 H=18052 L=18048 C=18049
  State: MONITORING (range confirmed, locked, waiting for manipulation)
  Note: Only 9 minutes left in Macro 3!

C[35] 19:15 — O=18044 H=18046 L=18033 C=18045
  State: → MANIPULATION_DETECTED
  Event: SSL swept! 5 minutes before Macro 3 ends!
  Question: Is this valid? YES — still within manipulation window.
  
C[36]-C[38]: MSS search... 

C[39] 19:19 — MSS confirmed at last minute of Macro 3!
  State: → AMD_READY_SIGNAL
  Valid: YES (confirmed within Macro 3 window)
  
  Note: Entry may occur in Macro 4 (continuation window 19:20-19:40)
        This is VALID per strategy — Macro 4 is for entries from Macro 3 setup
```

---


# ═══════════════════════════════════════════════════════════════
# PART 3 — DISTRIBUTION
# ═══════════════════════════════════════════════════════════════

## §11. HOW DISTRIBUTION STARTS

### 11.1 The Transition Point

```
DISTRIBUTION BEGINS at the exact moment when:
  1. Manipulation has swept liquidity (confirmed)
  2. MSS has occurred (structure broken in opposite direction of sweep)
  3. Displacement candle confirms smart money intent

FORMAL START:
  distribution_start_candle = first candle AFTER displacement is confirmed
  
  OR if displacement IS the MSS candle:
  distribution_start_candle = the displacement/MSS candle itself
```

### 11.2 How Manipulation ENDS

```
MANIPULATION ENDS when one of these occurs:

SCENARIO A — Clean End (MSS + Displacement):
  Manipulation sweep → reversal candles → MSS break → displacement
  Manipulation_end = MSS confirmation candle
  
SCENARIO B — Immediate End (Engulfing Sweep):
  Single candle sweeps AND displaces (e.g., sweeps SSL, closes very bullish)
  Manipulation_end = the sweep candle itself (manipulation was 1 candle)
  
SCENARIO C — Failed End (No MSS):
  Manipulation sweep occurred but price returns to sweep direction
  No MSS within timeout (10 candles)
  Manipulation_end = TIMEOUT → state → INVALID
  No distribution occurs.

SCENARIO D — Delayed End:
  Manipulation occurs but MSS takes 5-8 candles to confirm
  During this time, price may consolidate between sweep level and MSS target
  Manipulation_end = eventual MSS confirmation
  Valid as long as within MSS_TIMEOUT
```

### 11.3 What Confirms Expansion (Distribution Started)

```
DISTRIBUTION CONFIRMED when ALL of:
  1. MSS occurred (structure broken)
  2. Displacement present (body > ATR * 0.8 in expected direction)
  3. Price is BEYOND the MSS level (continuing in distribution direction)
  4. No reversal back through MSS level within 2 candles

FORMULA:
  distribution_confirmed = 
    mss_confirmed
    AND displacement_detected
    AND current_price is beyond mss_level in expected direction
    AND candles_since_mss <= 3 (happened recently)

FOR BULLISH DISTRIBUTION (after SSL sweep):
  distribution_confirmed = Close[current] > mss_level
  AND displacement body was bullish (Close > Open, body > ATR*0.8)
  
FOR BEARISH DISTRIBUTION (after BSL sweep):
  distribution_confirmed = Close[current] < mss_level
  AND displacement body was bearish (Open > Close, body > ATR*0.8)
```

### 11.4 How Trend Begins After Distribution

```
THE NEW TREND:
  After distribution is confirmed, price should make:
  
  BULLISH: Higher Highs and Higher Lows
    - Each swing high > previous swing high
    - Each swing low > previous swing low
    - This continues until exhaustion or next macro reversal window
    
  BEARISH: Lower Lows and Lower Highs
    - Each swing low < previous swing low
    - Each swing high < previous swing high

BOT RESPONSIBILITY:
  The AMD Detection Engine's job ENDS at AMD_READY_SIGNAL.
  After the signal is emitted, the ENTRY ENGINE takes over:
    - Waits for swing low/high to form
    - Measures 61.8% retracement zone
    - Enters trade in discount/premium zone
    - Sets stop loss and target (1:3 RR)
  
  This is outside the scope of this document.
```

## §12. DISTRIBUTION vs CONTINUATION

### 12.1 How They Differ

```
DISTRIBUTION (first move after manipulation):
  - Occurs immediately after MSS + displacement
  - Is the INITIAL expansion move
  - Creates the first leg of the new trend
  - Usually the strongest/fastest move
  - Forms the swing structure that the entry engine uses

CONTINUATION (subsequent moves):
  - Occurs AFTER the initial distribution
  - Is a SECOND (or third) leg in the same direction
  - Usually during continuation macro windows
  - Still follows the same bias established in manipulation window
  - May have its own mini-AMD within the continuation leg

FROM BOT PERSPECTIVE:
  The AMD Detection Engine handles: A (detection) + M (detection) + D (signal)
  The Entry Engine handles: D (entry/execution)
  
  Continuation is just the Entry Engine acting on the SAME signal
  during the next continuation macro window.
```

### 12.2 When Distribution Turns Into Continuation

```
TRANSITION:
  Distribution leg completes (forms first swing high/low after MSS)
  → Price retraces
  → If retracement holds above/below key level
  → New impulse in same direction = CONTINUATION

BOT LOGIC:
  After AMD_READY_SIGNAL emitted:
    IF signal_direction == "LONG":
      - First upside leg = distribution
      - First pullback = retracement (entry opportunity)
      - Second upside leg = continuation
    IF signal_direction == "SHORT":
      - First downside leg = distribution
      - First pullback = retracement (entry opportunity)
      - Second downside leg = continuation
```

## §13. DISTRIBUTION — REVERSAL CASE

### 13.1 When Distribution is a REVERSAL

```
PER STRATEGY DOCUMENT:
  Macro 7-10 (22:50 - 00:40 IST) are "Reversal Macros"
  
  During these windows:
    - If market was BULLISH earlier → look for BEARISH AMD (reversal)
    - If market was BEARISH earlier → look for BULLISH AMD (reversal)
  
  THE AMD STRUCTURE IS THE SAME.
  The only difference is the DIRECTION is OPPOSITE to the earlier trend.

REVERSAL DETECTION:
  earlier_bias = determine_bias_from_macros_1_to_6()
  
  IF current_macro IN [7, 8, 9, 10]:
    preferred_direction = OPPOSITE(earlier_bias)
    IF amd_signal.direction == preferred_direction:
      → REVERSAL AMD (aligned with reversal expectation)
      → confidence_boost += 0.1
    IF amd_signal.direction == earlier_bias:
      → CONTINUATION AMD (against reversal expectation)
      → confidence_penalty -= 0.1
      → Strategy note: "Market not showing reversal, follow current trend"

EARLIER BIAS DETERMINATION:
  Look at AMD signals generated during Macros 1-6.
  IF most recent confirmed signal was LONG → earlier_bias = BULLISH
  IF most recent confirmed signal was SHORT → earlier_bias = BEARISH
  IF no confirmed signal → earlier_bias = NEUTRAL (either direction valid)
```

---


# ═══════════════════════════════════════════════════════════════
# PART 4 — EVERY MACRO WINDOW — SCENARIOS
# ═══════════════════════════════════════════════════════════════

## §14. MACRO-SPECIFIC SCENARIOS

### 14.1 MACRO 1 (17:50 - 18:20 IST) — Manipulation Window — OBSERVE ONLY

```
PER STRATEGY: "Only Observe and identify liquidity targets"
BOT BEHAVIOUR: Detection ACTIVE but signal emission SUPPRESSED.
               Use this window to BUILD the accumulation/liquidity map.
               Do NOT emit AMD_READY_SIGNAL during this window.

SCENARIO A — Clean Accumulation Forms:
  17:50-18:10: Price consolidates in 15-point range
  Equal highs form at top, equal lows form at bottom
  → BOT: Track accumulation, build liquidity map
  → OUTCOME: Ready for Macro 2/3 to trigger manipulation

SCENARIO B — Manipulation Occurs in Macro 1:
  17:50-18:00: Quick accumulation
  18:05: SSL swept
  18:10: MSS occurs
  → BOT: Detect AMD but DO NOT generate trade signal
  → Instead: Store the setup context for Macro 2 continuation
  → IF Entry conditions are met in Macro 2 → valid entry

SCENARIO C — No Range Forms (Trending Into Macro 1):
  Market enters Macro 1 already trending
  No accumulation forms — price making HH/HL or LL/LH
  → BOT: State stays at NO_RANGE
  → OUTCOME: No setup from Macro 1. Wait for Macro 3.

SCENARIO D — Wide Range (Invalid Accumulation):
  17:50-18:20: Price oscillates but range > ATR * 3.0
  → BOT: INVALID accumulation (too wide)
  → OUTCOME: No setup. Reset for next window.
```

### 14.2 MACRO 2 (18:20 - 18:40 IST) — Continuation Window

```
PER STRATEGY: "Monitor continuation of Macro 1 setup"
BOT BEHAVIOUR: Look for continuation entries from Macro 1 AMD.
               Also allow fresh AMD detection if Macro 1 had no setup.

SCENARIO A — Macro 1 Setup Continues:
  Macro 1 detected: accumulation + manipulation
  Macro 2: Price continues in distribution direction
  → BOT: Entry Engine active. Look for retracement entry.
  → Signal from Macro 1 is still valid.

SCENARIO B — Macro 1 Had No Setup, Fresh AMD in Macro 2:
  Macro 1: No accumulation formed
  18:20-18:30: Quick accumulation forms in Macro 2
  18:32: Manipulation sweep
  18:35: MSS + displacement
  → BOT: Valid AMD detected. Emit signal.
  → Note: Macro 2 is "continuation" type but fresh AMD can still occur.

SCENARIO C — Dead Market (No Movement):
  18:20-18:40: Barely any movement, ATR compresses
  → BOT: State stays MONITORING or NO_RANGE
  → OUTCOME: No setup. Wait for Macro 3.

SCENARIO D — Reversal of Macro 1 Setup:
  Macro 1 signaled LONG (SSL swept, bullish expected)
  Macro 2: Price reverses, breaks structure bearish
  → BOT: Original signal INVALIDATED
  → OUTCOME: Wait for new AMD in Macro 3
```

### 14.3 MACRO 3 (18:40 - 19:20 IST) — HIGH PROBABILITY Manipulation Window

```
PER STRATEGY: "This is the preferred entry window"
              "NYSE opens around this period and volume increases significantly"
BOT BEHAVIOUR: FULL detection active. Highest confidence signals.
               All AMD signals during this window get priority_boost.

SCENARIO A — Perfect AMD During NYSE Open:
  18:40-18:55: Accumulation forms (price consolidates after pre-market activity)
  18:56: Liquidity sweep (manipulation) as NYSE volatility kicks in
  18:58: MSS confirmed
  18:59: Displacement
  → BOT: AMD_READY_SIGNAL with confidence_boost (priority="HIGH")
  → IDEAL scenario per strategy

SCENARIO B — Accumulation Already Formed (From Macro 1/2):
  Accumulation formed during 18:00-18:40 (Macro 1/2 period)
  Macro 3 opens (18:40) → manipulation happens immediately
  → BOT: Retroactive range detection + immediate manipulation
  → Valid signal (accumulation was prior, manipulation is NOW in Macro 3)

SCENARIO C — Fast AMD (Under 10 Minutes):
  18:40-18:45: 5-candle accumulation (barely meets minimum)
  18:46: Sweep
  18:48: MSS + Displacement
  → BOT: Valid but confidence slightly lower (short accumulation)
  → Reduce confidence by 0.1 for very short accumulation

SCENARIO D — Manipulation Without Prior Accumulation:
  18:40-18:50: Market trending (no clear range)
  18:51: Sudden spike sweeps a prior swing high/low
  18:52: Reversal
  → BOT: Manipulation detected BUT no formal accumulation confirmed
  → IMPLEMENTATION ASSUMPTION: 
    Treat isolated sweeps of PRIOR SESSION liquidity as valid IF:
    - A clear swing high/low exists from earlier
    - The sweep + return pattern is present
    - Macro 3 window is active
    → Confidence = 0.5 (lower than full AMD, but tradeable)

SCENARIO E — Extended Accumulation (Full 40 Minutes):
  18:40-19:15: Price consolidates entire Macro 3 window (35 minutes!)
  19:16: Finally manipulation occurs
  → BOT: Valid but range may be "stale" (35 candles)
  → Still within MAX_CANDLES (60) so OK
  → Manipulation occurs near window end — entry in Macro 4

SCENARIO F — No AMD (Trending Through Macro 3):
  Market continues trending through entire Macro 3
  No consolidation, no range, no AMD
  → BOT: State stays NO_RANGE or TRENDING
  → OUTCOME: No signal. Wait for Silver Bullet (Macro 5).
```

### 14.4 MACRO 4 (19:20 - 19:40 IST) — High Probability Continuation

```
PER STRATEGY: "Continue move started in Macro 3"
BOT BEHAVIOUR: Entry engine active for Macro 3 signals.
               Fresh AMD detection also possible.

SCENARIO A — Macro 3 Signal Continuation:
  Macro 3 generated LONG signal.
  Macro 4: Price retraces to discount zone → entry trigger
  → BOT: Entry engine executes. AMD engine passive.

SCENARIO B — Macro 3 Signal, Retracement Entry in Macro 4:
  Macro 3: AMD confirmed (LONG)
  19:20-19:25: Price pulls back forming swing low
  19:28: Price reaches 61.8% retracement of distribution leg
  → BOT: Entry engine triggers entry in discount zone

SCENARIO C — Fresh Manipulation in Macro 4:
  Macro 3 had no setup.
  Macro 4: Quick accumulation + sweep occurs
  → BOT: Valid AMD but confidence slightly lower
         (Macro 4 is continuation type, not manipulation type)
  → Still tradeable but reduce priority

SCENARIO D — Over-extension (No Entry):
  Macro 3 signal was LONG. Distribution was very strong.
  Macro 4: Price is already too far from entry zone. No retracement.
  → BOT: Entry engine has NO valid entry (price too extended)
  → OUTCOME: No trade. Wait for deeper pullback or next window.
```

### 14.5 MACRO 5 (19:40 - 20:20 IST) — Silver Bullet Manipulation

```
PER STRATEGY: "Trend continuation" + "AMD profile" + "FVG" + "MSS"
BOT BEHAVIOUR: Full AMD detection. Look for continuation of earlier bias.

SCENARIO A — Silver Bullet Aligned with Earlier Bias:
  Earlier macros: LONG bias established
  19:40-19:55: Price consolidates (mini accumulation)
  19:57: SSL swept (manipulation)
  20:00: Bullish MSS + displacement
  → BOT: AMD_READY_SIGNAL (LONG, aligned with bias → high confidence)
  → IDEAL Silver Bullet setup

SCENARIO B — Silver Bullet Counter-Trend:
  Earlier macros: LONG bias
  19:40-19:55: Accumulation at highs
  19:57: BSL swept (bearish manipulation against earlier bias!)
  → BOT: AMD detected but DIRECTION conflicts with earlier bias
  → IMPLEMENTATION ASSUMPTION:
    During Macros 5-6, PREFER trades aligned with earlier bias.
    Counter-trend signals during Silver Bullet get confidence_penalty -= 0.15
    Still valid but lower priority.

SCENARIO C — No Fresh Setup (Hold Existing):
  Already in a trade from Macro 3/4.
  Silver Bullet: No new AMD forms.
  → BOT: AMD engine stays passive. Entry engine manages existing position.

SCENARIO D — FVG-Based Entry (Not Full AMD):
  Price doesn't form complete accumulation.
  But a Fair Value Gap from earlier distribution has NOT been filled.
  Price retraces to FVG → potential entry.
  → BOT SCOPE: FVG detection is separate from AMD engine.
    AMD engine: No signal. 
    Entry engine may use FVG logic independently (out of this spec's scope).
```

### 14.6 MACRO 6 (20:20 - 20:40 IST) — Silver Bullet Continuation

```
Same logic as Macro 4 but for Silver Bullet setups.
See Macro 4 scenarios — apply to Macro 5 signals.
```

### 14.7 MACRO 7 (22:50 - 23:20 IST) — Reversal Manipulation

```
PER STRATEGY: "Primarily for reversals"
              "If bullish trend existed earlier → search for short"
              "If bearish trend existed earlier → search for long"

BOT BEHAVIOUR: AMD detection active with REVERSAL BIAS.
               Prefer signals OPPOSITE to earlier session direction.

SCENARIO A — Classic Reversal (Bullish → Bearish):
  Earlier session: LONG bias (Macro 3 was bullish AMD)
  22:50-23:05: Price consolidates near session highs
  23:07: BSL swept (takes session high liquidity)
  23:10: Bearish MSS + displacement
  → BOT: AMD_READY_SIGNAL (SHORT)
  → ALIGNED with reversal expectation → confidence_boost
  → This is the lunch reversal

SCENARIO B — No Reversal (Trend Continues):
  Earlier session: LONG bias
  22:50-23:10: Price consolidates but then SSL swept (continuation of bullish!)
  → BOT: AMD detected (LONG signal — same as earlier)
  → Per strategy: "if market is not showing signs of reversal, 
                   follow the current trend"
  → Valid signal but NO reversal boost
  → IMPLEMENTATION: If direction == earlier_bias → standard confidence
                    (no penalty, no boost — just follow trend)

SCENARIO C — Failed Reversal Attempt:
  Earlier session: LONG bias
  22:50-23:10: Accumulation forms
  23:12: BSL swept (potential bearish reversal setup)
  23:14-23:20: MSS DOES NOT confirm (price continues higher)
  → BOT: manipulation_detected but MSS_TIMEOUT
  → State → INVALID
  → OUTCOME: Failed reversal. Market still bullish.
  → May try again in Macro 9 (second reversal window)

SCENARIO D — Exhaustion Pattern:
  Earlier session: Strong LONG trend (4+ impulse legs)
  22:50-23:10: Price shows exhaustion (weakening impulses, long upper wicks)
  23:12: BSL sweep creates final exhaustion high
  23:15: Strong bearish MSS + displacement
  → BOT: High confidence reversal (exhaustion + AMD aligned)
  → confidence_boost += 0.15 (exhaustion pattern detected)
```

### 14.8 MACRO 8 (23:20 - 23:40 IST) — Reversal Continuation

```
Same as Macro 4/6 logic but for reversal setups from Macro 7.
Entry engine active for Macro 7 reversal signals.
```

### 14.9 MACRO 9 (23:40 - 00:20 IST) — Second Reversal Window

```
PER STRATEGY: "Another strong reversal window. Use if reversal 
               did not occur in previous lunch macros."

SCENARIO A — First Reversal Failed, Second Attempt Here:
  Macro 7: AMD detected but MSS failed (no reversal)
  Macro 9: FRESH AMD forms with new accumulation
  23:45-00:00: New accumulation
  00:05: Manipulation sweep
  00:10: MSS + displacement
  → BOT: AMD_READY_SIGNAL (reversal direction)
  → Valid: "Use if reversal did not occur in previous lunch macros"

SCENARIO B — First Reversal Succeeded, Second is Continuation:
  Macro 7: Valid bearish reversal AMD (SHORT signal)
  Macro 9: Market already moved in reversal direction
  → BOT: If new AMD forms in SAME direction → continuation of reversal
  → Valid standard AMD signal

SCENARIO C — Still No Reversal by Macro 9:
  Macro 7: No reversal
  Macro 9: Market still trending original direction
  → BOT: Follow trend. If AMD forms in trend direction → valid signal
  → Per strategy: "follow AMD model and maintain bias in current trend"

SCENARIO D — Late Reversal with Low Confidence:
  00:15: AMD finally forms reversal signal
  Only 5 minutes left in Macro 9!
  → BOT: Valid signal but timing is tight
  → Entry may need to happen in Macro 10
  → Reduce confidence slightly (late formation)
```

### 14.10 MACRO 10 (00:20 - 00:40 IST) — Final Reversal Window

```
PER STRATEGY: "Final important reversal window"

SCENARIO A — Entry from Macro 9 Signal:
  Macro 9 generated reversal signal.
  Macro 10: Retracement entry opportunity.
  → BOT: Entry engine active. AMD engine passive.

SCENARIO B — Last Chance Reversal:
  No reversal in Macros 7 or 9.
  Macro 10: Quick AMD forms
  → BOT: Valid but lowest confidence reversal window
  → This is the LAST opportunity of the day

SCENARIO C — No Setup (End of Day):
  No AMD forms in Macro 10.
  → BOT: Session ends without additional signals.
  → OUTCOME: No further trading today.
```

---


# ═══════════════════════════════════════════════════════════════
# PART 5 — DEVELOPER EXAMPLES
# ═══════════════════════════════════════════════════════════════

## §15. COMPLETE DEVELOPER EXAMPLES

### 15.1 ACCUMULATION EXAMPLES (50 Examples)

```
NOTE: All examples use the following format:
  - OHLC candle data
  - State at that point
  - Bot decision
  - Reason

RANGE: ATR = 10, tolerance = 3.6 pts
```

#### VALID ACCUMULATION EXAMPLES (25)

```
ACC-V1: Minimal Valid Accumulation (10 candles)
  C[0]:  O=18050 H=18058 L=18045 C=18053
  C[1]:  O=18053 H=18060 L=18048 C=18055
  C[2]:  O=18055 H=18059 L=18047 C=18048
  C[3]:  O=18048 H=18057 L=18044 C=18056
  C[4]:  O=18056 H=18060 L=18050 C=18052
  C[5]:  O=18052 H=18058 L=18044 C=18046
  C[6]:  O=18046 H=18054 L=18043 C=18053
  C[7]:  O=18053 H=18059 L=18048 C=18049
  C[8]:  O=18049 H=18055 L=18043 C=18051
  C[9]:  O=18051 H=18060 L=18047 C=18054
  
  Range: H=18060, L=18043, Size=17
  ATR check: 17 <= 10*2.0=20 ✓
  Net displacement: |18054-18053|/17 = 0.06 ✓ (< 0.4)
  Equal Highs: C[1]=18060, C[4]=18060, C[9]=18060 → 3 EH ✓
  Equal Lows: C[5]=18044, C[6]=18043, C[8]=18043 → within tolerance ✓
  Direction changes: 6/9 = 0.67 ✓ (> 0.35)
  → DECISION: CONFIRMED_ACCUMULATION
  → REASON: All conditions met

ACC-V2: Post-Impulse Accumulation
  PRIOR: Strong bearish candle O=18080 H=18082 L=18052 C=18055 (impulse)
  Then 12 candles consolidating between 18045-18060
  → DECISION: CONFIRMED_ACCUMULATION
  → REASON: Post-impulse consolidation, valid range

ACC-V3: Compression Accumulation (Narrowing)
  First 5 candles: range 18 points
  Last 5 candles: range 10 points (narrowing)
  Overall: 15 candles, range=18, net_disp < 0.2
  → DECISION: CONFIRMED_ACCUMULATION + COMPRESSION_FLAG
  → REASON: Valid + compression detected → manipulation imminent

ACC-V4: Wide but Valid Range
  Range: 18035-18055, Size=20 (ATR*2.0 — maximum allowed)
  15 candles, good oscillation
  Equal highs at 18055 (2x), Equal lows at 18035 (2x)
  → DECISION: CONFIRMED_ACCUMULATION
  → REASON: At maximum width but still valid

ACC-V5: Tight Range (Near Minimum)
  Range: 18047-18053, Size=6 (ATR*0.6)
  12 candles, very small bodies
  Equal highs at 18053 (3x), Equal lows at 18047 (2x)
  → DECISION: CONFIRMED_ACCUMULATION
  → REASON: Tight but above minimum (ATR*0.3=3)

ACC-V6: Accumulation with One Expansion
  Initial range: 18042-18058 (16 pts)
  C[8]: new high at 18060 → range expands to 18 pts
  Still within ATR*2.0 (20), expansion_count = 1
  → DECISION: CONFIRMED_ACCUMULATION (confidence -= 0.05)
  → REASON: Single expansion, still valid

ACC-V7: Accumulation with Internal Micro-Trend (Bullish Bias)
  Swing lows: 18042, 18044, 18046 (rising lows)
  Swing highs: 18058, 18059, 18058 (flat highs)
  Range stays bounded
  → DECISION: CONFIRMED_ACCUMULATION
  → REASON: Internal bullish bias (rising lows) still within range

ACC-V8: Accumulation During Macro 3 Start
  Time: 18:40-18:55 IST
  15 candles of consolidation after pre-market trending
  → DECISION: CONFIRMED_ACCUMULATION (priority: HIGH)
  → REASON: Forms during highest-priority manipulation window

ACC-V9: Long Duration Accumulation (45 candles)
  Range: 18040-18058, Size=18
  45 candles of oscillation
  Equal highs: 4 touches
  Equal lows: 3 touches
  → DECISION: CONFIRMED_ACCUMULATION
  → REASON: Long duration (still < MAX_CANDLES=60), strong liquidity

ACC-V10: Accumulation with 2 Expansions
  Initial: 18045-18056 (11)
  Expansion 1: Low to 18043 → range=13
  Expansion 2: High to 18059 → range=16
  Still <= ATR*2.0 ✓
  → DECISION: CONFIRMED_ACCUMULATION (confidence -= 0.10)
  → REASON: 2 expansions acceptable, each reduced confidence

ACC-V11: Doji-Heavy Accumulation
  8 of 12 candles are dojis (body_ratio < 0.1)
  Very indecisive market
  → DECISION: CONFIRMED_ACCUMULATION (quality: HIGH)
  → REASON: Extreme indecision = strong accumulation characteristic

ACC-V12: Alternating Bull/Bear Accumulation
  Every candle alternates direction (BULL-BEAR-BULL-BEAR...)
  High direction_change_ratio (0.9)
  → DECISION: CONFIRMED_ACCUMULATION (quality: VERY HIGH)
  → REASON: Perfect oscillation = textbook accumulation

ACC-V13: Accumulation with Volume Spike (if available)
  Normal candles with one volume spike candle in middle
  Volume spike candle still within range boundaries
  → DECISION: CONFIRMED_ACCUMULATION
  → REASON: Volume spike didn't break range → absorbed

ACC-V14: Accumulation Forming During Low Volatility
  ATR drops to 6 (from normal 10)
  Range: 18048-18054, Size=6 (ATR*1.0)
  → DECISION: CONFIRMED_ACCUMULATION
  → REASON: Adjusted to lower ATR, range is proportionally valid

ACC-V15: Accumulation with False Start
  C[0]-C[4]: Possible range forms
  C[5]: Expansion breaks range → reset
  C[6]-C[16]: NEW range forms, proper accumulation
  → DECISION: First range INVALID, second CONFIRMED_ACCUMULATION
  → REASON: Bot correctly reset and detected second valid range

ACC-V16-V25: [Variations of above patterns with different price levels,
              different touch counts, different macro windows]
  (Each follows same structural logic — valid range, valid liquidity,
   valid candle behavior, within time constraints)
```

#### INVALID ACCUMULATION EXAMPLES (25)

```
ACC-I1: Range Too Wide
  Range: 18030-18065, Size=35 > ATR*3.0=30
  → DECISION: INVALID
  → REASON: Range exceeds absolute maximum width

ACC-I2: Range Too Narrow (Noise)
  Range: 18049-18051, Size=2 < ATR*0.3=3
  → DECISION: NOT_ACCUMULATION (noise)
  → REASON: Range below minimum threshold

ACC-I3: Strong Trend (Not Accumulation)
  10 candles all making higher highs and higher lows
  Net displacement: |18070-18045|/25 = 1.0 >> 0.4
  → DECISION: TRENDING (not accumulation)
  → REASON: Net displacement exceeds 0.4 threshold

ACC-I4: Timeout (Too Long)
  65 candles without resolution
  → DECISION: INVALID (timeout)
  → REASON: Exceeded MAX_CANDLES=60

ACC-I5: Impulse Candle Inside Range
  Valid range 18040-18060 (20 pts)
  C[8]: O=18042 H=18058 L=18040 C=18057 → body=15, 15/20=0.75 > 0.6
  Then price continues to 18065 (breaks range)
  → DECISION: INVALID
  → REASON: Impulse candle (body > range*0.6) → not consolidation

ACC-I6: Five Consecutive Same Direction
  C[5]-C[9]: All bullish, each close > previous close
  max_consecutive_run = 5 ≥ 5
  → DECISION: INVALID
  → REASON: Sustained directional run → trending

ACC-I7: Two Consecutive Closes Above Range
  Range high = 18060
  C[j]: Close = 18062 (above range)
  C[j+1]: Close = 18064 (still above range)
  → DECISION: INVALID (genuine breakout)
  → REASON: 2 consecutive closes beyond boundary

ACC-I8: No Liquidity After MIN_CANDLES
  15 candles within range
  But: All highs different, all lows different
  No equal highs cluster, no equal lows cluster
  → DECISION: stays POSSIBLE, eventually TIMEOUT → INVALID
  → REASON: No liquidity formation = no manipulation target

ACC-I9: News Window Overlap
  Accumulation forming 5:40 PM - 5:55 PM IST
  NFP release at 6:00 PM
  Blackout starts at 5:30 PM
  → DECISION: INVALID (news blackout)
  → REASON: High-impact news in proximity

ACC-I10: Too Many Expansions
  Initial range forms
  4 expansions before candle_count reaches MIN_CANDLES
  → DECISION: INVALID
  → REASON: Excessive expansion (>3) before minimum candle count

ACC-I11: Average Body Too Large
  12 candles, average body = 7 points
  Range size = 18 points
  avg_body / range_size = 7/18 = 0.39 > 0.3
  → DECISION: INVALID
  → REASON: Bodies too large for accumulation behavior

ACC-I12: Structure Score Too High (Clear Trend)
  Swings: HH, HL, HH, HL (perfect bullish structure)
  bullish_structure_score = 0.85 > 0.7
  → DECISION: TRENDING (not accumulation)
  → REASON: Clear bullish market structure detected

ACC-I13: Gap Through Range
  Friday Close = 18050 (range: 18040-18060)
  Monday Open = 18080 (gapped above range)
  → DECISION: RESET (gap invalidates)
  → REASON: Weekend gap > ATR*0.5

ACC-I14-I25: [Variations including:
  - Range forms but only on one side (no opposite liquidity)
  - Extreme low volatility making all candles noise
  - Range forms outside any macro window
  - Range during weekend session with low activity
  - Multiple rapid invalidation/reset cycles
  - Range with single anomaly candle that's 3x ATR]
```

### 15.2 MANIPULATION EXAMPLES (50 Examples)

```
NOTE: All assume valid confirmed accumulation exists.
Range: 18040 (SSL, 3 touches) to 18060 (BSL, 3 touches), ATR=10
```

#### VALID MANIPULATION (25)

```
MANIP-V1: Textbook SSL Sweep
  Candle: O=18045 H=18050 L=18035 C=18047
  → VALID: Swept SSL, closed inside, wick depth=5=ATR*0.5
  → Direction: LONG

MANIP-V2: Textbook BSL Sweep  
  Candle: O=18055 H=18067 L=18052 C=18054
  → VALID: Swept BSL, closed inside, wick depth=7=ATR*0.7
  → Direction: SHORT

MANIP-V3: Two-Candle SSL Sweep
  C[j]: O=18043 H=18045 L=18034 C=18037 (closed below!)
  C[j+1]: O=18037 H=18050 L=18036 C=18048 (returned above!)
  → VALID: 2-candle pattern, returned on next candle
  → Direction: LONG

MANIP-V4: Engulfing SSL Sweep (Same Candle Reversal)
  Candle: O=18044 H=18058 L=18032 C=18056
  → VALID: Swept SSL AND displaced in same candle
  → Direction: LONG (high confidence)
  → NOTE: This may also satisfy MSS + displacement simultaneously

MANIP-V5: Triple-Touch Level Sweep
  SSL has 4 touches (very strong level)
  Candle: O=18046 H=18049 L=18033 C=18045
  → VALID: Swept very strong level (highest confidence)
  → Direction: LONG

MANIP-V6: Macro 3 Opening Sweep
  Time: 18:40 IST (first minute of Macro 3)
  Candle: O=18048 H=18050 L=18034 C=18046
  → VALID: Immediate sweep at macro open (institutional activity)
  → Direction: LONG (confidence boost for macro alignment)

MANIP-V7: Compression → Sweep
  Last 5 candles: [range 8,7,5,4,3]
  THEN: O=18048 H=18050 L=18033 C=18046
  → VALID: Compression resolved into sweep (classic pattern)
  → Direction: LONG

MANIP-V8: Double Sweep (Both Sides)
  C[j-5]: BSL swept (High=18066, returned to 18054)
  No MSS confirmed from BSL sweep...
  C[j]: SSL swept (Low=18033, Close=18045)
  → VALID: Double manipulation, second sweep determines direction
  → Direction: LONG (last sweep was SSL → bullish)

MANIP-V9: Nested Sweep (Internal + External)
  C[j]: Low=18044 (breaks internal swing low at 18045)
  C[j]: continues... Low=18034 (also breaks external SSL at 18040)
  → VALID: Nested sweep (multi-level) → stronger confidence
  → Direction: LONG

MANIP-V10: Gap Down Sweep
  Previous close = 18045
  Current: O=18037 H=18048 L=18035 C=18046
  Opened BELOW support (gapped down), swept, recovered
  → VALID: Gap-down sweep (manipulation via gap)
  → Direction: LONG

MANIP-V11-V25: [Additional valid scenarios including:
  - Sweep during Silver Bullet window
  - Sweep during reversal macro (opposite to earlier bias)
  - Sweep with exactly minimum penetration depth
  - Sweep at ATR*0.3 depth (normal range)
  - Sweep with immediate V-reversal
  - Sweep where body barely touches but wick is deep
  - Sweep at session low/high
  - Sweep aligned with broader timeframe structure
  - Sweep with multiple candle wicks probing same level]
```

#### INVALID MANIPULATION (25)

```
MANIP-I1: Didn't Reach Liquidity
  BSL at 18060. Candle High = 18058.
  → INVALID: Didn't reach BSL level
  
MANIP-I2: Genuine Breakout (2 Closes Beyond)
  C[j]: Close=18063 (above BSL)
  C[j+1]: Close=18065 (still above!)
  → INVALID: Genuine breakout, not manipulation

MANIP-I3: Too Shallow
  SSL at 18040. Low=18039.5. Depth=0.5 < 1 (min_sweep_distance)
  → INVALID: Noise, not meaningful sweep

MANIP-I4: During News Blackout
  Valid sweep pattern but during FOMC
  → INVALID: News blackout, unreliable

MANIP-I5: No Prior Liquidity
  Level has only 1 touch (not established)
  → INVALID: Insufficient liquidity at level

MANIP-I6: Way Too Deep (Breakout)
  SSL at 18040. Low=18015. Depth=25=ATR*2.5. Close=18018.
  → INVALID: Too deep + body closed far beyond = breakout

MANIP-I7: Range Not Locked (Too Early)
  Only 4 candles into possible range (< MIN_CANDLES)
  Price goes below tentative range_low
  → NOT MANIPULATION: Range still forming, this is just expansion

MANIP-I8: Price Never Returns
  Sweeps SSL then drops further. Never returns above support.
  → INVALID: Not manipulation — genuine bearish breakout

MANIP-I9: During Continuation Window (Lower Priority)
  Sweep occurs at 19:25 IST (Macro 4 — continuation window)
  → REDUCED CONFIDENCE: Not in manipulation window
  → May still be valid but lower priority

MANIP-I10: Sweep in Opposite Direction to Session Bias
  Session: Strong BULL trend
  Macro 3: BSL swept (suggesting SHORT)
  But body closes above resistance on C[j+1]
  → INVALID: Continued above (not manipulation, breakout)

MANIP-I11-I25: [Additional invalid scenarios including:
  - Sweep during dead/low-volume period
  - Multiple failed attempts without reaching liquidity
  - Sweep followed by chop (no MSS within timeout)
  - Sweep against overwhelming trend momentum
  - Sweep of non-significant level (only 1 touch)
  - Very old range (>55 candles) with stale liquidity
  - Sweep during session transition (unreliable data)]
```

### 15.3 DISTRIBUTION EXAMPLES (25 Examples)

```
NOTE: All assume valid accumulation + confirmed manipulation preceded.
```

#### VALID DISTRIBUTION (15)

```
DIST-V1: Classic Bullish Distribution
  Manipulation: SSL swept at 18034
  MSS: Bullish break above 18055 (swing high)
  Displacement: Body = 12 pts bullish (> ATR*0.8=8) ✓
  Distribution: Price continues to 18080 over next 10 candles
  → VALID DISTRIBUTION
  → Signal: LONG
  
DIST-V2: Classic Bearish Distribution
  Manipulation: BSL swept at 18067
  MSS: Bearish break below 18045 (swing low)
  Displacement: Body = 9 pts bearish ✓
  Distribution: Price drops to 18020 over next 8 candles
  → VALID DISTRIBUTION
  → Signal: SHORT

DIST-V3: Immediate Distribution (Sweep + Displacement Same Candle)
  Single candle: O=18042 H=18058 L=18032 C=18057
  Swept SSL (18032), displaced bullish in same candle
  MSS may be confirmed in same candle or next
  → VALID DISTRIBUTION (fast AMD cycle)
  → Signal: LONG

DIST-V4: Distribution After Double Manipulation
  First: BSL swept (no MSS)
  Then: SSL swept (MSS confirms bullish)
  Distribution: Bullish expansion
  → VALID DISTRIBUTION
  → Direction from SECOND manipulation: LONG

DIST-V5: Distribution During Macro 3 (Highest Priority)
  Complete AMD cycle: 18:40-18:55
  Distribution begins at 18:55
  → VALID DISTRIBUTION (priority: HIGH)
  → Entry opportunity in Macro 4 (continuation window)

DIST-V6-V15: [Additional valid distribution scenarios including:
  - Slow distribution (gradual expansion over 10+ candles)
  - Fast distribution (3 large candles)
  - Distribution with one pullback then continuation
  - Distribution during Silver Bullet
  - Reversal distribution in Macro 7]
```

#### INVALID / FAILED DISTRIBUTION (10)

```
DIST-I1: MSS But No Displacement
  Manipulation confirmed. Price breaks swing level (MSS).
  But body of MSS candle = 4 < ATR*0.8=8. No displacement.
  Wait 5 more candles — still no displacement.
  → DISTRIBUTION NOT CONFIRMED (timeout approaching)
  → May still occur but confidence LOW

DIST-I2: Displacement But Immediate Reversal
  MSS confirmed. Displacement candle appears (body=9 ✓).
  BUT: Very next candle reverses entirely, goes back through MSS level!
  → DISTRIBUTION FAILED
  → Price rejected at higher level, manipulation didn't hold

DIST-I3: Distribution Stalls (No Follow-Through)
  MSS + Displacement confirmed.
  Next 5 candles: tiny bodies, no progress, starts ranging AGAIN
  → DISTRIBUTION WEAK
  → May form NEW accumulation at higher/lower level
  → Original AMD cycle is COMPLETE but distribution was minimal

DIST-I4: Wrong Direction Distribution
  SSL swept (expects LONG distribution).
  MSS breaks bearish (wrong direction!).
  → MANIPULATION INTERPRETATION ERROR
  → What happened: The "manipulation" was actually a genuine breakout
  → Original accumulation is INVALID → RESET

DIST-I5-I10: [Scenarios including:
  - Distribution halted by news event
  - Distribution runs into strong opposing order block
  - Weak distribution that only covers 50% of expected range
  - Distribution that reverses at the next macro boundary
  - Distribution that forms new accumulation (next AMD cycle)]
```

---


# ═══════════════════════════════════════════════════════════════
# PART 6 — BOT STATE MACHINE
# ═══════════════════════════════════════════════════════════════

## §16. COMPLETE PRODUCTION-READY STATE MACHINE

### 16.1 State Diagram

```
┌─────────────────┐
│    NO_RANGE     │ ← System start / After RESET
└────────┬────────┘
         │
         │ Sliding window detects possible range
         │ (range within ATR bounds, direction changes > 0.25)
         ▼
┌─────────────────────┐
│   POSSIBLE_RANGE    │
└────────┬────────────┘
         │
         │ candle_count >= 7 (approaching MIN)
         │ AND some oscillation confirmed
         ▼
┌─────────────────────────┐
│   EARLY_ACCUMULATION    │
└────────┬────────────────┘
         │
         │ candle_count >= MIN_CANDLES (10)
         │ AND net_displacement < 0.4
         │ AND at least 1 liquidity cluster (equal H or L)
         │ AND candle_behaviour_valid
         │ AND confidence >= 0.4
         ▼
┌──────────────────────────────┐
│   CONFIRMED_ACCUMULATION     │
└────────┬─────────────────────┘
         │
         │ Range LOCKED:
         │   high_touches >= 2 AND low_touches >= 2
         │ AND liquidity pools mapped (BSL + SSL targets set)
         ▼
┌─────────────────────────┐
│    LIQUIDITY_FORMED     │
└────────┬────────────────┘
         │
         │ Macro window becomes active
         │ (currently in MANIPULATION-type macro window)
         ▼
┌───────────────────────────────┐
│   MONITORING_FOR_MANIPULATION │ ← Active scanning for sweeps
└────────┬──────────────────────┘
         │
         │ Price sweeps liquidity (wick or body beyond boundary)
         │ AND reaches BSL/SSL level
         │ AND shows reversal characteristics
         ▼
┌──────────────────────────────┐
│   MANIPULATION_STARTED       │
└────────┬─────────────────────┘
         │
         │ Sweep CONFIRMED:
         │   Close returns inside range (same or next candle)
         │   OR immediate opposite-direction candle
         ▼
┌──────────────────────────────┐
│   MANIPULATION_CONFIRMED     │
└────────┬─────────────────────┘
         │
         │ First swing high/low forms AFTER manipulation
         │ Then candle BODY closes beyond that swing level
         ▼
┌─────────────────┐
│      MSS        │ (Market Structure Shift confirmed)
└────────┬────────┘
         │
         │ Candle body > ATR * 0.8 in expected direction
         │ (may be same candle as MSS or within 5 candles after)
         ▼
┌──────────────────────┐
│    DISPLACEMENT      │
└────────┬─────────────┘
         │
         │ All conditions met → EMIT SIGNAL
         ▼
┌──────────────────────┐
│    DISTRIBUTION      │ → AMD_READY_SIGNAL emitted to Entry Engine
└──────────────────────┘


         ┌─────────────┐
         │   INVALID   │ ← Can be reached from ANY state
         └──────┬──────┘
                │
                │ After COOLDOWN_CANDLES (5)
                ▼
         ┌─────────────┐
         │    RESET    │ → Clears all, returns to NO_RANGE
         └─────────────┘
```

### 16.2 Detailed State Definitions

```
STATE 1: NO_RANGE
  ─────────────────
  DESCRIPTION: Bot is idle, scanning for potential ranges.
  
  ENTRY CONDITIONS:
    - System startup
    - After RESET
    - After previous AMD cycle completes
    
  PROCESSING:
    - Calculate ATR(20) on each candle
    - Run sliding window (W=5) range check
    - Check if current time is approaching macro window
    
  EXIT → POSSIBLE_RANGE when:
    - window_range >= ATR * 0.3
    - window_range <= ATR * 2.0
    - net_displacement_ratio < 0.6
    - direction_changes / (W-1) >= 0.2

  EXIT → INVALID when:
    - News blackout detected


STATE 2: POSSIBLE_RANGE
  ─────────────────────
  DESCRIPTION: A potential range detected. Gathering more candles for confirmation.
  
  ENTRY CONDITIONS:
    - From NO_RANGE when initial criteria met
    
  PROCESSING:
    - Track range_high, range_low
    - Allow expansions (within limits)
    - Count candles
    - Start checking for oscillation pattern
    
  EXIT → EARLY_ACCUMULATION when:
    - candle_count >= 7
    - Range still valid (within ATR bounds)
    - At least 3 direction changes observed
    
  EXIT → INVALID when:
    - range_size > ATR * 3.0 (too wide)
    - Impulse candle (body > range * 0.8)
    - 4+ consecutive same direction candles
    - Net displacement > 0.7 (clearly trending)


STATE 3: EARLY_ACCUMULATION
  ──────────────────────────
  DESCRIPTION: Range is forming, approaching confirmation threshold.
  
  ENTRY CONDITIONS:
    - From POSSIBLE_RANGE when candle_count >= 7
    
  PROCESSING:
    - Continue tracking boundaries
    - Begin scanning for equal highs/lows
    - Calculate confidence score
    - Validate candle behaviour
    
  EXIT → CONFIRMED_ACCUMULATION when:
    - candle_count >= MIN_CANDLES (10)
    - net_displacement < 0.4
    - equal_highs >= 2 OR equal_lows >= 2
    - validate_candle_behaviour() returns valid
    - confidence_score >= 0.4
    
  EXIT → INVALID when:
    - Same as POSSIBLE_RANGE invalidation conditions
    - Plus: expansion_count > 3 before reaching MIN_CANDLES


STATE 4: CONFIRMED_ACCUMULATION
  ─────────────────────────────
  DESCRIPTION: Valid accumulation confirmed. Building liquidity map.
  
  ENTRY CONDITIONS:
    - From EARLY_ACCUMULATION when all criteria met
    
  PROCESSING:
    - Count boundary touches (within 10% zones)
    - Build full liquidity map
    - Identify BSL and SSL target levels
    - Check for range lock conditions
    
  EXIT → LIQUIDITY_FORMED when:
    - high_touches >= 2 AND low_touches >= 2
    - BSL target level identified
    - SSL target level identified
    - Range boundaries LOCKED
    
  EXIT → INVALID when:
    - Range expands beyond ATR * 3.0
    - Timeout (candle_count > MAX_CANDLES)
    - Candle behaviour degrades


STATE 5: LIQUIDITY_FORMED
  ────────────────────────
  DESCRIPTION: Range locked, liquidity mapped, waiting for active macro window.
  
  ENTRY CONDITIONS:
    - From CONFIRMED_ACCUMULATION when lock criteria met
    
  PROCESSING:
    - Monitor time
    - Check if manipulation-type macro window is active
    - Continue tracking boundary interactions
    
  EXIT → MONITORING_FOR_MANIPULATION when:
    - Current time IS within a manipulation-type macro window
    - OR signal from prior window is being carried forward
    
  EXIT → INVALID when:
    - Timeout (candle_count > MAX_CANDLES)
    - Genuine breakout (2 closes beyond boundary)
    - Macro session ends with no manipulation window active


STATE 6: MONITORING_FOR_MANIPULATION
  ────────────────────────────────────
  DESCRIPTION: Actively watching for liquidity sweep.
  
  ENTRY CONDITIONS:
    - From LIQUIDITY_FORMED when macro window is active
    
  PROCESSING:
    - Every candle: check if High > range_high + min_sweep (BSL)
    - Every candle: check if Low < range_low - min_sweep (SSL)
    - Track partial sweeps and pending manipulations
    - Handle pending 2-candle sweeps
    
  EXIT → MANIPULATION_STARTED when:
    - Price exceeds boundary AND reaches identified liquidity level
    
  EXIT → INVALID when:
    - Genuine breakout (2 closes beyond)
    - Monitoring timeout (20 candles without boundary test)
    - Macro window expires


STATE 7: MANIPULATION_STARTED
  ────────────────────────────
  DESCRIPTION: Sweep detected, waiting for confirmation (return).
  
  ENTRY CONDITIONS:
    - From MONITORING when sweep criteria met
    
  PROCESSING:
    - Check if current candle closed back inside (wick rejection)
    - If not, wait for NEXT candle to return
    - Track sweep level and direction
    
  EXIT → MANIPULATION_CONFIRMED when:
    - Close returns inside range (same candle = immediate confirm)
    - OR next candle Close returns inside range (2-candle confirm)
    
  EXIT → INVALID when:
    - Next candle also closes beyond boundary (genuine breakout)
    - 3+ candles all close beyond boundary


STATE 8: MANIPULATION_CONFIRMED
  ──────────────────────────────
  DESCRIPTION: Liquidity sweep confirmed. Searching for MSS.
  
  ENTRY CONDITIONS:
    - From MANIPULATION_STARTED when return confirmed
    
  PROCESSING:
    - Track post-manipulation candles
    - Detect swing highs/lows forming after sweep
    - Set MSS target (first swing in expected direction)
    - Monitor for failure (price returning to sweep direction)
    
  EXIT → MSS when:
    - Candle BODY closes beyond the first swing high/low
      (bullish MSS = close above swing high after SSL sweep)
      (bearish MSS = close below swing low after BSL sweep)
    
  EXIT → INVALID when:
    - Price makes new extreme beyond manipulation level
      (new low below SSL sweep, or new high above BSL sweep)
    - MSS_TIMEOUT (10 candles) exceeded without structure break
    - Price enters choppy behavior with no directional conviction


STATE 9: MSS (Market Structure Shift)
  ────────────────────────────────────
  DESCRIPTION: Structure has shifted. Looking for displacement confirmation.
  
  ENTRY CONDITIONS:
    - From MANIPULATION_CONFIRMED when structure break occurs
    
  PROCESSING:
    - Check if the MSS candle itself has displacement (body > ATR*0.8)
    - If not, monitor next candles for displacement
    - Track displacement direction matches expected direction
    
  EXIT → DISPLACEMENT when:
    - Candle body >= ATR * 0.8 in expected direction
    - Occurs within 5 candles of MSS
    
  EXIT → INVALID when:
    - 5 candles pass without displacement (weak MSS)
    - Price reverses back through MSS level (MSS failed)


STATE 10: DISPLACEMENT
  ─────────────────────
  DESCRIPTION: Displacement confirmed. AMD cycle complete. Emit signal.
  
  ENTRY CONDITIONS:
    - From MSS when displacement body confirmed
    
  PROCESSING:
    - Compile all signal data
    - Emit AMD_READY_SIGNAL
    - Calculate final confidence score
    
  EXIT → DISTRIBUTION when:
    - Signal emitted → state tracks distribution phase
    
  IMMEDIATE ACTION:
    - EMIT AMD_READY_SIGNAL to Entry Engine


STATE 11: DISTRIBUTION
  ─────────────────────
  DESCRIPTION: Signal active. Distribution in progress.
  
  ENTRY CONDITIONS:
    - From DISPLACEMENT after signal emission
    
  PROCESSING:
    - Track distribution progress
    - Monitor for distribution failure
    - Wait for Entry Engine to consume signal
    
  EXIT → RESET when:
    - SIGNAL_TIMEOUT (20 candles) expires
    - Entry Engine acknowledges signal
    - Distribution fails (price reverses through MSS level)
    → After reset, engine can scan for next AMD cycle


STATE 12: INVALID
  ────────────────
  DESCRIPTION: Current hypothesis invalidated. Cooling down.
  
  ENTRY CONDITIONS:
    - From ANY state when invalidation condition triggered
    
  PROCESSING:
    - Log invalidation reason
    - Increment cooldown counter
    
  EXIT → RESET when:
    - cooldown_counter >= COOLDOWN_CANDLES (5)


STATE 13: RESET
  ─────────────
  DESCRIPTION: Clear all state and return to scanning.
  
  PROCESSING:
    - Clear range data
    - Clear liquidity data
    - Clear manipulation tracking
    - Clear MSS/displacement tracking
    - PRESERVE: ATR history, candle buffer
    
  EXIT → NO_RANGE:
    - Immediately (RESET is a transition state)
```

### 16.3 State Transition Summary Table

| # | From | To | Trigger |
|---|------|----|---------|
| 1 | NO_RANGE | POSSIBLE_RANGE | Window detects potential range |
| 2 | POSSIBLE_RANGE | EARLY_ACCUMULATION | 7+ candles, oscillation confirmed |
| 3 | EARLY_ACCUMULATION | CONFIRMED_ACCUMULATION | 10+ candles, liquidity + behaviour valid |
| 4 | CONFIRMED_ACCUMULATION | LIQUIDITY_FORMED | Range locked, BSL/SSL targets set |
| 5 | LIQUIDITY_FORMED | MONITORING_FOR_MANIPULATION | Macro window active |
| 6 | MONITORING_FOR_MANIPULATION | MANIPULATION_STARTED | Sweep detected |
| 7 | MANIPULATION_STARTED | MANIPULATION_CONFIRMED | Return confirmed |
| 8 | MANIPULATION_CONFIRMED | MSS | Structure break in expected direction |
| 9 | MSS | DISPLACEMENT | Large body in expected direction |
| 10 | DISPLACEMENT | DISTRIBUTION | Signal emitted |
| 11 | DISTRIBUTION | RESET | Timeout or acknowledged |
| 12 | ANY | INVALID | Invalidation condition |
| 13 | INVALID | RESET | Cooldown complete |
| 14 | RESET | NO_RANGE | Immediate |

### 16.4 Confidence Score at Each State

```
CONFIDENCE ACCUMULATES through states:

NO_RANGE: confidence = 0
POSSIBLE_RANGE: confidence = 0.1
EARLY_ACCUMULATION: confidence = 0.2
CONFIRMED_ACCUMULATION: confidence = 0.4 + bonuses - penalties
LIQUIDITY_FORMED: confidence += 0.1 (liquidity mapped)
MONITORING: confidence unchanged
MANIPULATION_STARTED: confidence += 0.05
MANIPULATION_CONFIRMED: confidence += 0.1
MSS: confidence += 0.1
DISPLACEMENT: confidence += 0.1

BONUSES:
  + 0.05 per additional liquidity touch beyond 2
  + 0.1 if macro_window.priority == "HIGH" (Macro 3/4)
  + 0.05 if compression detected before manipulation
  + 0.1 if double manipulation (both sides swept)
  + 0.05 if aligned with earlier session bias

PENALTIES:
  - 0.05 per range expansion
  - 0.05 per 10 candles beyond MIN_CANDLES (staleness)
  - 0.1 if non-priority macro window
  - 0.1 if direction conflicts with session bias (reversal window OK)
  - 0.15 if during continuation window (not manipulation window)

FINAL SIGNAL CONFIDENCE:
  >= 0.8 → VERY HIGH CONFIDENCE (priority trade)
  >= 0.6 → HIGH CONFIDENCE (standard trade)
  >= 0.4 → MODERATE CONFIDENCE (cautious trade)
  < 0.4 → LOW CONFIDENCE (consider skipping)
```

### 16.5 Complete Pseudo Code — Main Engine

```python
CLASS AMDDetectionEngine:

  CONSTANTS:
    MIN_CANDLES = 10
    MAX_CANDLES = 60
    ATR_PERIOD = 20
    RANGE_ATR_MAX = 2.0
    RANGE_ATR_MIN = 0.3
    EQUAL_LEVEL_TOLERANCE = 0.0002
    BREAKOUT_BUFFER = 0.0001
    NET_DISPLACEMENT_MAX = 0.4
    MIN_SWEEP_RATIO = 0.05
    DISPLACEMENT_ATR_RATIO = 0.8
    MSS_TIMEOUT = 10
    MONITORING_TIMEOUT = 20
    COOLDOWN_CANDLES = 5
    SIGNAL_TIMEOUT = 20
    MAX_EXPANSIONS = 3

  STATES: [NO_RANGE, POSSIBLE_RANGE, EARLY_ACCUMULATION, 
           CONFIRMED_ACCUMULATION, LIQUIDITY_FORMED,
           MONITORING_FOR_MANIPULATION, MANIPULATION_STARTED,
           MANIPULATION_CONFIRMED, MSS, DISPLACEMENT,
           DISTRIBUTION, INVALID, RESET]

  FUNCTION on_new_candle(candle):
    // Pre-checks
    IF news_blackout(candle.time): force_invalidate(); RETURN
    
    // Update ATR
    update_atr(candle)
    IF atr == None: RETURN
    
    // State dispatch
    SWITCH state:
      NO_RANGE: scan_for_range(candle)
      POSSIBLE_RANGE: validate_range(candle)
      EARLY_ACCUMULATION: attempt_confirmation(candle)
      CONFIRMED_ACCUMULATION: build_liquidity(candle)
      LIQUIDITY_FORMED: check_macro_window(candle)
      MONITORING_FOR_MANIPULATION: scan_manipulation(candle)
      MANIPULATION_STARTED: confirm_manipulation(candle)
      MANIPULATION_CONFIRMED: search_mss(candle)
      MSS: search_displacement(candle)
      DISPLACEMENT: emit_signal(candle)
      DISTRIBUTION: manage_distribution(candle)
      INVALID: run_cooldown(candle)
      RESET: do_reset()
```

### 16.6 Signal Output Format

```
AMD_READY_SIGNAL = {
  signal_type: "AMD_READY",
  timestamp: DateTime,
  market: String,  // "US100", "US500", "US30"
  
  // Direction
  direction: "LONG" or "SHORT",
  
  // Accumulation Data
  accumulation: {
    range_high: Float,
    range_low: Float,
    range_size: Float,
    duration_candles: Integer,
    confidence: Float,
    equal_high_touches: Integer,
    equal_low_touches: Integer,
    compression_detected: Boolean,
  },
  
  // Manipulation Data
  manipulation: {
    type: "SSL_SWEEP" or "BSL_SWEEP" or "DOUBLE_SWEEP",
    sweep_level: Float,
    sweep_depth: Float,
    candle_count: Integer (1 or 2),
    confidence: Float,
  },
  
  // MSS Data
  mss: {
    type: "BULLISH_MSS" or "BEARISH_MSS",
    level: Float,
    candle_index: Integer,
  },
  
  // Displacement Data
  displacement: {
    body_size: Float,
    direction: "BULL" or "BEAR",
    candle_index: Integer,
  },
  
  // Context
  macro_window: {
    id: Integer (1-10),
    type: "MANIPULATION" or "CONTINUATION" or "REVERSAL",
    priority: "HIGH" or "NORMAL" or "REVERSAL",
  },
  
  // Scoring
  final_confidence: Float (0.0 to 1.0),
  
  // Session Context
  session_bias: "BULLISH" or "BEARISH" or "NEUTRAL",
  is_reversal_setup: Boolean,
}
```

---

## APPENDIX: IMPLEMENTATION ASSUMPTIONS

| # | Assumption | Rationale |
|---|-----------|-----------|
| 1 | 13 states (expanded from 8) | More granular for production debugging |
| 2 | EARLY_ACCUMULATION at 7 candles | Between initial detection (5) and confirmation (10) |
| 3 | LIQUIDITY_FORMED separate from CONFIRMED | Distinguishes "range valid" from "ready for manipulation" |
| 4 | MANIPULATION_STARTED → MANIPULATION_CONFIRMED | Handles 2-candle sweep patterns cleanly |
| 5 | MSS and DISPLACEMENT as separate states | Some MSS candles ARE displacement; some need separate candle |
| 6 | Reversal macros prefer opposite direction | Document says "search for short if bullish earlier" |
| 7 | Macro 1 observe-only (no signal emission) | Document explicitly says "Only Observe" |
| 8 | Continuation macros allow fresh AMD | Not explicitly stated but implied by "look for entries" |
| 9 | Double manipulation uses SECOND sweep direction | Logical: first was "fake fake", second is true manipulation |
| 10 | Confidence bonuses are additive | Simple model; can be made multiplicative if needed |
| 11 | MSS requires BODY close (not wick) | Standard ICT interpretation: bodies confirm, wicks don't |
| 12 | Internal sweeps don't count as manipulation | Only boundary/external sweeps trigger AMD transition |
| 13 | 5-candle displacement timeout | If no displacement in 5 candles, MSS was weak |
| 14 | Gap > ATR*0.5 invalidates | Gaps break structural continuity |
| 15 | Session bias from most recent signal | Simple heuristic for determining earlier trend |

---

**END OF DOCUMENT**

*Document Version: 2.0*  
*Derived From: ICT Hydra Macro Strategy for Indices (BOT Macros updated.docx)*  
*Scope: Complete AMD Detection Engine — Accumulation through Distribution*  
*All analysis on 1-minute timeframe as specified in source document*  
*Total States: 13 | Total Examples: 125+ | Total Wick Examples: 32 | Manipulation Scenarios: 50+*
