# ICT ACCUMULATION DETECTION ENGINE
## Developer Specification Document v1.0

**Document Type:** Software Implementation Specification  
**Target:** Trading Bot — Accumulation Phase Detection  
**Timeframe:** 1-Minute Chart ONLY  
**Markets:** NASDAQ (US100), S&P 500 (US500), Dow Jones (US30)  
**Source:** ICT Hydra Macro Strategy Document  

---

## TABLE OF CONTENTS

1. [Definition of Accumulation](#1-definition-of-accumulation)
2. [Range Detection Algorithm](#2-range-detection-algorithm)
3. [Mathematical Conditions](#3-mathematical-conditions)
4. [Candle Behaviour Inside Accumulation](#4-candle-behaviour-inside-accumulation)
5. [Internal Liquidity Detection](#5-internal-liquidity-detection)
6. [Expansion and Manipulation Detection](#6-expansion-and-manipulation-detection)
7. [State Machine](#7-state-machine)
8. [Edge Cases](#8-edge-cases)
9. [Complete Detection Algorithm](#9-complete-detection-algorithm)
10. [Pseudo Code](#10-pseudo-code)

---


## 1. DEFINITION OF ACCUMULATION

### 1.1 Mathematical Definition

**Accumulation** is a contiguous set of 1-minute candles `C[i], C[i+1], ..., C[i+N-1]` where:

```
Let Range_High = max(High[i], High[i+1], ..., High[i+N-1])
Let Range_Low  = min(Low[i], Low[i+1], ..., Low[i+N-1])
Let Range_Size = Range_High - Range_Low
```

**Accumulation exists when ALL of the following are TRUE:**

| # | Condition | Formula |
|---|-----------|---------|
| 1 | Price is confined within a bounded range | `∀ j ∈ [i, i+N-1]: Low[j] >= Range_Low AND High[j] <= Range_High` |
| 2 | Range size is constrained (not trending) | `Range_Size <= ATR(20) * 2.0` |
| 3 | Minimum duration met | `N >= MIN_CANDLES` (see §1.3) |
| 4 | No sustained directional movement | `abs(Close[i+N-1] - Close[i]) / Range_Size < 0.4` |
| 5 | Liquidity forms on at least one side | `Equal_Highs >= 2 OR Equal_Lows >= 2` (see §5) |
| 6 | No breakout has occurred | `No candle closes beyond Range_High + BUFFER or Range_Low - BUFFER` |

### 1.2 What Accumulation IS (Programmatic)

- A **sideways consolidation** where price oscillates between defined boundaries
- A **liquidity-building** phase where repeated touches to similar levels create resting orders
- A **precursor** to the Manipulation phase (the "M" in AMD)
- Detectable as **low net displacement** over the duration of the range

### 1.3 Configuration Constants

```
TIMEFRAME           = 1 minute
MIN_CANDLES         = 10        // Minimum candles to form accumulation
MAX_CANDLES         = 60        // Maximum candles before invalidation (range too old)
ATR_PERIOD          = 20        // Period for ATR calculation
RANGE_ATR_MAX       = 2.0       // Max range as multiple of ATR
RANGE_ATR_MIN       = 0.3       // Min range as multiple of ATR (filter noise)
EQUAL_LEVEL_TOLERANCE = 0.0002  // Tolerance for "equal" highs/lows (as % of price)
BREAKOUT_BUFFER     = 0.0001    // Buffer beyond range for breakout confirmation (as % of price)
NET_DISPLACEMENT_MAX = 0.4      // Max |Close_last - Close_first| / Range_Size
BODY_DOMINANCE_MIN  = 0         // No minimum body requirement during accumulation
```

> **IMPLEMENTATION ASSUMPTION:** The document states accumulation "consolidates within a range and builds liquidity on both sides." The MIN_CANDLES=10 and MAX_CANDLES=60 are derived from the 20-minute macro window (20 candles on 1-min chart). A range lasting less than 10 candles is insufficient for liquidity to form. A range exceeding 60 candles (1 hour) without resolution likely spans multiple macro windows and should be re-evaluated.

### 1.4 What BEGINS Accumulation

Accumulation begins when:
```
TRIGGER: After a significant move (impulse leg), price begins to consolidate.

Formally:
  Let impulse = max(range of candles[i-5..i-1]) where any single candle body > ATR * 0.8
  After impulse, if next MIN_CANDLES candles all remain within:
    High <= impulse_high + ATR * 0.3
    Low  >= impulse_low  - ATR * 0.3
  THEN: Accumulation BEGINS at candle[i]
```

OR (alternative trigger — no prior impulse required):
```
  If last MIN_CANDLES candles satisfy:
    max(Highs) - min(Lows) <= ATR * RANGE_ATR_MAX
    AND abs(Close[last] - Close[first]) / (max(Highs) - min(Lows)) < NET_DISPLACEMENT_MAX
  THEN: Accumulation BEGINS at first candle of the window
```

### 1.5 What ENDS Accumulation

Accumulation ends when ONE of the following occurs:

| Event | Condition | Next State |
|-------|-----------|------------|
| Manipulation (sweep) | Price pierces Range_High or Range_Low by > BREAKOUT_BUFFER but closes back inside | → MANIPULATION_DETECTED |
| Genuine Breakout | Price closes beyond range + BUFFER AND next candle also closes beyond | → INVALID (not AMD) |
| Timeout | N > MAX_CANDLES without resolution | → INVALID |
| Range Expansion | Range_Size exceeds ATR * 3.0 | → INVALID |

### 1.6 What INVALIDATES Accumulation

```
INVALID IF:
  1. Range_Size < ATR * RANGE_ATR_MIN              // Too tight, just noise
  2. Range_Size > ATR * 3.0                         // Too wide, trending
  3. N > MAX_CANDLES                                // Too old
  4. Two consecutive closes outside range           // Genuine breakout (not manipulation)
  5. Single candle body > Range_Size * 0.8          // Impulse inside range = not consolidation
  6. Active during high-impact news window          // NFP, FOMC, CPI, GDP
  7. Not within any defined Macro Window            // Only detect during macro times
```

---


## 2. RANGE DETECTION ALGORITHM

### 2.1 Range Creation Process

The range is built **incrementally** as each new 1-minute candle arrives.

```
ALGORITHM: Range Building

INPUT: Stream of 1-minute candles
OUTPUT: Range object {High, Low, Start_Index, End_Index, Status}

STEP 1: Initialize
  range_high = High[start_candle]
  range_low  = Low[start_candle]
  candle_count = 1

STEP 2: For each new candle C[n]:
  IF C[n].High > range_high:
    tentative_new_high = C[n].High
    IF (tentative_new_high - range_low) <= ATR * RANGE_ATR_MAX:
      range_high = tentative_new_high    // Range expands upward (allowed)
    ELSE:
      → Range exceeded max size → INVALID
      
  IF C[n].Low < range_low:
    tentative_new_low = C[n].Low
    IF (range_high - tentative_new_low) <= ATR * RANGE_ATR_MAX:
      range_low = tentative_new_low      // Range expands downward (allowed)
    ELSE:
      → Range exceeded max size → INVALID

  candle_count += 1
```

### 2.2 When Does the Range START?

```
RANGE START DETECTION:

Method: Sliding Window Approach

For each candle C[i], look back at window of size W = MIN_CANDLES:
  window_high = max(High[i-W+1 .. i])
  window_low  = min(Low[i-W+1 .. i])
  window_range = window_high - window_low
  net_move = abs(Close[i] - Close[i-W+1])
  
  IF window_range <= ATR(20) * RANGE_ATR_MAX
  AND window_range >= ATR(20) * RANGE_ATR_MIN
  AND net_move / window_range < NET_DISPLACEMENT_MAX
  THEN:
    RANGE STARTS at candle C[i-W+1]
    range_high = window_high
    range_low = window_low
```

### 2.3 When Does the Range STOP EXPANDING?

```
RANGE LOCK CONDITION:

The range is "locked" (boundaries finalized) when:
  candle_count >= MIN_CANDLES
  AND at least 2 touches to range_high zone (within EQUAL_LEVEL_TOLERANCE)
  AND at least 2 touches to range_low zone (within EQUAL_LEVEL_TOLERANCE)

Once locked:
  range_high and range_low are FIXED
  Any candle exceeding these levels is treated as:
    - Manipulation (if wick only, or single close that returns)
    - Breakout (if consecutive closes beyond)
```

### 2.4 When is the Range LOCKED?

```
LOCK CRITERIA (ALL must be true):
  1. candle_count >= MIN_CANDLES
  2. touches_to_high_zone >= 2
  3. touches_to_low_zone >= 2
  4. No single candle in the range has body > Range_Size * 0.6

Where "touch to high zone" means:
  High[j] >= range_high - (Range_Size * 0.1)

Where "touch to low zone" means:
  Low[j] <= range_low + (Range_Size * 0.1)
```

### 2.5 When is the Range INVALID?

```
INVALIDATION CONDITIONS:
  1. candle_count > MAX_CANDLES without lock → TIMEOUT
  2. Range_Size > ATR * 3.0 → TOO_WIDE
  3. Range_Size < ATR * RANGE_ATR_MIN → TOO_NARROW (noise)
  4. Two consecutive candles close above range_high + BUFFER → GENUINE_BREAKOUT_UP
  5. Two consecutive candles close below range_low - BUFFER → GENUINE_BREAKOUT_DOWN
  6. A single candle body > Range_Size * 0.8 appears → IMPULSE_INVALIDATION
```

### 2.6 Equal Highs/Lows Treatment

```
EQUAL HIGHS DETECTION:
  Two highs H[a] and H[b] are "equal" if:
    |H[a] - H[b]| / price <= EQUAL_LEVEL_TOLERANCE
    AND a != b
    AND |a - b| >= 2 (at least 2 candles apart)

EQUAL LOWS DETECTION:
  Two lows L[a] and L[b] are "equal" if:
    |L[a] - L[b]| / price <= EQUAL_LEVEL_TOLERANCE
    AND a != b
    AND |a - b| >= 2

SIGNIFICANCE:
  Equal highs = Buy-Side Liquidity (BSL) pool
  Equal lows = Sell-Side Liquidity (SSL) pool
  
  More equal touches = stronger liquidity pool = higher probability manipulation target
  
  liquidity_strength = number_of_equal_touches * weight_factor
  where weight_factor = 1.0 (each additional touch adds equal weight)
```

### 2.7 Expanding Ranges

```
EXPANDING RANGE HANDLING:

IF range is NOT yet locked:
  Allow expansion up to ATR * RANGE_ATR_MAX
  Each expansion resets the confirmation counter for that side
  
IF range IS locked:
  Any price beyond range = potential manipulation or breakout
  DO NOT expand the range after lock

SPECIAL CASE - Gradual Expansion:
  IF range expands more than 3 times before lock:
    Confidence score decreases by 0.15 per expansion
    IF confidence < 0.3 → INVALID (price is trending, not consolidating)
```

### 2.8 Nested Ranges

```
NESTED RANGE HANDLING:

Definition: A smaller range forms INSIDE an already-detected accumulation range.

Detection:
  IF within an active accumulation range:
    A sub-range forms where:
      sub_range_high < range_high
      sub_range_low > range_low
      sub_range_size < range_size * 0.5
      sub_range candle_count >= 5
      
Action:
  Track nested range as MICRO_ACCUMULATION
  The nested range's boundaries become INTERNAL LIQUIDITY
  Nested range equal highs/lows = micro liquidity pools
  
Priority:
  Always use the OUTER (parent) range for AMD classification
  Nested ranges provide additional precision for liquidity targets
```

### 2.9 Overlapping Ranges

```
OVERLAPPING RANGE HANDLING:

Definition: A new potential range begins before the previous range resolves.

Rule:
  IF current_range is ACTIVE and a new potential range is detected:
    IF new_range is INSIDE current_range:
      → Treat as nested (see §2.8)
    IF new_range OVERLAPS but extends beyond current_range:
      → Invalidate current_range
      → Start tracking new_range as primary
    IF new_range is AFTER current_range resolved:
      → Normal: track as new independent range
```

### 2.10 Numerical Example

```
EXAMPLE: NASDAQ 1-minute chart

ATR(20) = 5.0 points
RANGE_ATR_MAX = 2.0 → Max allowed range = 10.0 points
EQUAL_LEVEL_TOLERANCE = 0.0002 → at price 18000 = 3.6 points tolerance

Candle stream (simplified):
  C[0]: O=18050 H=18055 L=18045 C=18052
  C[1]: O=18052 H=18058 L=18048 C=18050
  C[2]: O=18050 H=18056 L=18046 C=18054
  C[3]: O=18054 H=18057 L=18047 C=18048
  C[4]: O=18048 H=18055 L=18044 C=18053
  C[5]: O=18053 H=18058 L=18049 C=18051
  C[6]: O=18051 H=18056 L=18045 C=18047
  C[7]: O=18047 H=18054 L=18043 C=18052
  C[8]: O=18052 H=18057 L=18048 C=18050
  C[9]: O=18050 H=18058 L=18046 C=18055
  C[10]: O=18055 H=18059 L=18050 C=18051
  C[11]: O=18051 H=18057 L=18044 C=18053

After 12 candles:
  range_high = 18059 (from C[10])
  range_low  = 18043 (from C[7])
  Range_Size = 16 points
  
  Check: 16 > ATR*2 = 10? YES → But wait...
  
  Actually let's recalculate with realistic ATR for NASDAQ:
  ATR(20) on 1-min NASDAQ ≈ 8-15 points typically
  
  Let ATR = 10 points
  Max range = 10 * 2.0 = 20 points
  Range_Size = 16 points ≤ 20 → VALID
  
  Net displacement = |18053 - 18052| = 1 point
  Net displacement ratio = 1/16 = 0.0625 < 0.4 → VALID
  
  Equal Highs: C[1]=18058, C[5]=18058, C[9]=18058 → 3 equal highs → BSL formed
  Equal Lows: C[4]=18044, C[11]=18044 → 2 equal lows → SSL formed
  
  RESULT: ACCUMULATION CONFIRMED
    - BSL at 18058 (3 touches)
    - SSL at 18043-18044 (2 touches)
    - Range: 18043 to 18059
    - Duration: 12 candles
    - Liquidity on BOTH sides → High quality accumulation
```

---


## 3. MATHEMATICAL CONDITIONS

### 3.1 Primary Detection Formula

```
ACCUMULATION_DETECTED = TRUE if ALL conditions are satisfied:

CONDITION 1 — Range Containment:
  ∀ j ∈ [start, end]:
    High[j] <= Range_High + BREAKOUT_BUFFER_ABS
    Low[j]  >= Range_Low  - BREAKOUT_BUFFER_ABS
  
  Where BREAKOUT_BUFFER_ABS = price * BREAKOUT_BUFFER

CONDITION 2 — Range Size Validity:
  ATR(20) * RANGE_ATR_MIN <= Range_Size <= ATR(20) * RANGE_ATR_MAX
  
  Expanded: 0.3 * ATR <= (Range_High - Range_Low) <= 2.0 * ATR

CONDITION 3 — Minimum Duration:
  candle_count >= MIN_CANDLES
  → N >= 10

CONDITION 4 — Low Net Displacement:
  |Close[end] - Close[start]| / Range_Size < NET_DISPLACEMENT_MAX
  → |Close[end] - Close[start]| / Range_Size < 0.4

CONDITION 5 — Liquidity Formation:
  equal_highs_count >= 2 OR equal_lows_count >= 2
  
  Where equal_highs_count = count of distinct candles where:
    |High[j] - max_high_cluster| / price <= EQUAL_LEVEL_TOLERANCE
    AND distance between any two qualifying candles >= 2

CONDITION 6 — No Breakout:
  NOT (∃ j, j+1 where Close[j] > Range_High + BUFFER AND Close[j+1] > Range_High + BUFFER)
  AND NOT (∃ j, j+1 where Close[j] < Range_Low - BUFFER AND Close[j+1] < Range_Low - BUFFER)

CONDITION 7 — Macro Window Active:
  current_time ∈ ANY_DEFINED_MACRO_WINDOW (see §3.3)

CONDITION 8 — No News Filter:
  current_date NOT IN high_impact_news_dates
```

### 3.2 Confidence Score Formula

```
CONFIDENCE SCORE (0.0 to 1.0):

score = base_score 
        + liquidity_bonus 
        + duration_bonus 
        + compression_bonus 
        - expansion_penalty 
        - displacement_penalty

Where:
  base_score = 0.4 (if all primary conditions met)
  
  liquidity_bonus = min(0.2, (equal_highs + equal_lows - 2) * 0.05)
    → More liquidity touches = higher confidence
    
  duration_bonus = min(0.15, (candle_count - MIN_CANDLES) * 0.01)
    → Longer accumulation = more liquidity built
    
  compression_bonus = min(0.15, (1 - Range_Size / (ATR * RANGE_ATR_MAX)) * 0.2)
    → Tighter range relative to ATR = more compressed = higher quality
    
  expansion_penalty = expansion_count * 0.05
    → Each range expansion before lock reduces confidence
    
  displacement_penalty = (|Close[end] - Close[start]| / Range_Size) * 0.3
    → Higher net displacement = lower confidence (trending)

THRESHOLDS:
  score >= 0.7 → HIGH confidence accumulation
  score >= 0.5 → MEDIUM confidence accumulation  
  score >= 0.4 → LOW confidence accumulation
  score < 0.4 → INVALID (do not classify as accumulation)
```

### 3.3 Macro Window Time Conditions (IST)

```
MACRO_WINDOWS = [
  {id: 1,  start: "17:50", end: "18:20", type: "MANIPULATION",  priority: "NORMAL"},
  {id: 2,  start: "18:20", end: "18:40", type: "CONTINUATION",  priority: "NORMAL"},
  {id: 3,  start: "18:40", end: "19:20", type: "MANIPULATION",  priority: "HIGH"},
  {id: 4,  start: "19:20", end: "19:40", type: "CONTINUATION",  priority: "HIGH"},
  {id: 5,  start: "19:40", end: "20:20", type: "MANIPULATION",  priority: "NORMAL"},
  {id: 6,  start: "20:20", end: "20:40", type: "CONTINUATION",  priority: "NORMAL"},
  {id: 7,  start: "22:50", end: "23:20", type: "MANIPULATION",  priority: "REVERSAL"},
  {id: 8,  start: "23:20", end: "23:40", type: "CONTINUATION",  priority: "REVERSAL"},
  {id: 9,  start: "23:40", end: "00:20", type: "MANIPULATION",  priority: "REVERSAL"},
  {id: 10, start: "00:20", end: "00:40", type: "CONTINUATION",  priority: "REVERSAL"},
]

RULE: Accumulation detection should BEGIN during MANIPULATION windows.
      The accumulation may have FORMED in the minutes leading up to or within the window.
      
LOOKBACK: When a manipulation macro window opens, scan back up to 20 candles 
          to check if accumulation already formed.
```

### 3.4 Combined Decision Formula

```
FINAL DECISION:

trade_signal = NONE

IF ACCUMULATION_DETECTED == TRUE
AND confidence_score >= 0.5
AND macro_window.type == "MANIPULATION"
AND news_filter == CLEAR
THEN:
  state = ACCUMULATION_CONFIRMED
  WAIT for manipulation (see §6)
  
IF state == ACCUMULATION_CONFIRMED AND manipulation_detected:
  state = MANIPULATION_DETECTED
  WAIT for MSS (see §6.3)
  
IF state == MANIPULATION_DETECTED AND MSS_confirmed:
  state = READY_FOR_ENTRY
  → Pass to Entry Engine (not in scope of this document)
```

---


## 4. CANDLE BEHAVIOUR INSIDE ACCUMULATION

### 4.1 Expected Candle Characteristics

During a valid accumulation, candles exhibit **specific measurable properties**:

```
PROPERTY 1 — Small Bodies Relative to Range:
  Average_Body = mean(|Close[j] - Open[j]|) for j in [start..end]
  Expected: Average_Body <= Range_Size * 0.3
  
  Rationale: Large bodies indicate directional intent (trending), 
             not consolidation.

PROPERTY 2 — Wick Presence (Both Sides):
  upper_wick[j] = High[j] - max(Open[j], Close[j])
  lower_wick[j] = min(Open[j], Close[j]) - Low[j]
  
  avg_upper_wick = mean(upper_wick[j]) for j in [start..end]
  avg_lower_wick = mean(lower_wick[j]) for j in [start..end]
  
  Expected: Both avg_upper_wick > 0 AND avg_lower_wick > 0
  → Two-sided wicks indicate indecision (characteristic of accumulation)

PROPERTY 3 — Alternating Direction:
  direction[j] = 1 if Close[j] > Open[j] else -1
  direction_changes = count of j where direction[j] != direction[j-1]
  direction_change_ratio = direction_changes / (candle_count - 1)
  
  Expected: direction_change_ratio >= 0.35
  → Frequent direction changes = no dominant trend = consolidation

PROPERTY 4 — No Sustained Runs:
  max_consecutive_same_direction = max length of consecutive same-direction candles
  
  Expected: max_consecutive_same_direction <= 4
  → More than 4 same-direction candles suggests trending, not accumulating
```

### 4.2 Required Calculations

| Calculation | Formula | Purpose | Required? |
|-------------|---------|---------|-----------|
| ATR(20) | `mean(TrueRange[i-19..i])` where `TR = max(H-L, |H-prevC|, |L-prevC|)` | Range size normalization | **YES — Critical** |
| Average Body Size | `mean(\|Close - Open\|)` over range | Detect consolidation vs trend | **YES** |
| Body-to-Range Ratio | `avg_body / Range_Size` | Quality filter | **YES** |
| Direction Change Ratio | `direction_changes / (N-1)` | Confirm oscillation | **YES** |
| Range Compression Ratio | `Range_Size / ATR(20)` | Classify tightness | **YES** |
| Standard Deviation of Closes | `stdev(Close[start..end])` | Measure dispersion | **OPTIONAL — useful for confidence** |
| Volume | N/A | Not available in spec | **NO — not required** |

### 4.3 ATR Calculation (Mandatory)

```
FUNCTION calculate_ATR(candles[], period=20):
  IF len(candles) < period + 1:
    RETURN None  // Insufficient data
    
  true_ranges = []
  FOR i = 1 to len(candles)-1:
    tr = max(
      candles[i].High - candles[i].Low,
      abs(candles[i].High - candles[i-1].Close),
      abs(candles[i].Low - candles[i-1].Close)
    )
    true_ranges.append(tr)
  
  // Use Simple Moving Average for first ATR, then EMA
  atr = mean(true_ranges[0:period])
  FOR i = period to len(true_ranges)-1:
    atr = (atr * (period - 1) + true_ranges[i]) / period
    
  RETURN atr
```

### 4.4 Candle Validation Inside Range

```
FUNCTION validate_candle_behaviour(candles[], range_high, range_low):
  range_size = range_high - range_low
  N = len(candles)
  
  // Check 1: Average body size
  bodies = [abs(c.Close - c.Open) for c in candles]
  avg_body = mean(bodies)
  IF avg_body > range_size * 0.3:
    RETURN {valid: FALSE, reason: "Bodies too large — trending"}
  
  // Check 2: Max single candle body
  max_body = max(bodies)
  IF max_body > range_size * 0.6:
    RETURN {valid: FALSE, reason: "Single impulse candle detected"}
  
  // Check 3: Direction changes
  directions = [1 if c.Close > c.Open else -1 for c in candles]
  changes = sum(1 for i in 1..N-1 if directions[i] != directions[i-1])
  change_ratio = changes / (N - 1)
  IF change_ratio < 0.25:
    RETURN {valid: FALSE, reason: "Too directional — insufficient oscillation"}
  
  // Check 4: Max consecutive same direction
  max_run = calculate_max_run(directions)
  IF max_run > 5:
    RETURN {valid: FALSE, reason: "Sustained directional run detected"}
  
  RETURN {valid: TRUE, reason: None}
```

### 4.5 What is NOT Needed

| Calculation | Why Not Needed |
|-------------|---------------|
| Volume Profile | Document does not mention volume-based analysis |
| VWAP | Not referenced in strategy |
| Bollinger Bands | Not part of ICT methodology |
| RSI/MACD/Stochastic | No oscillator-based confirmation mentioned |
| Fibonacci (during accumulation) | Fibonacci applies AFTER manipulation, not during accumulation |
| Higher timeframe analysis | Document explicitly states "all analysis on 1-minute timeframe only" |

---


## 5. INTERNAL LIQUIDITY DETECTION

### 5.1 What is Liquidity (Mathematical Definition)

```
LIQUIDITY = a price level where multiple market participants have resting orders.

In the context of accumulation:
  - Equal Highs = cluster of stop-loss orders from short sellers (Buy-Side Liquidity / BSL)
  - Equal Lows = cluster of stop-loss orders from long buyers (Sell-Side Liquidity / SSL)

The bot identifies liquidity as REPEATED PRICE TOUCHES to similar levels.
```

### 5.2 Equal Highs Detection Algorithm

```
FUNCTION detect_equal_highs(candles[], tolerance_pct):
  
  tolerance_abs = candles[last].Close * tolerance_pct
  highs = [(i, c.High) for i, c in enumerate(candles)]
  
  // Sort highs descending
  sorted_highs = sort(highs, key=value, descending=True)
  
  clusters = []
  used = set()
  
  FOR each (idx, value) in sorted_highs:
    IF idx in used: CONTINUE
    
    cluster = [(idx, value)]
    used.add(idx)
    
    FOR each (idx2, value2) in sorted_highs:
      IF idx2 in used: CONTINUE
      IF abs(value - value2) <= tolerance_abs:
        IF abs(idx - idx2) >= 2:  // Must be at least 2 candles apart
          cluster.append((idx2, value2))
          used.add(idx2)
    
    IF len(cluster) >= 2:
      clusters.append({
        level: mean([v for (_, v) in cluster]),
        touches: len(cluster),
        indices: [i for (i, _) in cluster],
        type: "BSL"  // Buy-Side Liquidity
      })
  
  RETURN clusters
```

### 5.3 Equal Lows Detection Algorithm

```
FUNCTION detect_equal_lows(candles[], tolerance_pct):
  
  tolerance_abs = candles[last].Close * tolerance_pct
  lows = [(i, c.Low) for i, c in enumerate(candles)]
  
  // Sort lows ascending
  sorted_lows = sort(lows, key=value, ascending=True)
  
  clusters = []
  used = set()
  
  FOR each (idx, value) in sorted_lows:
    IF idx in used: CONTINUE
    
    cluster = [(idx, value)]
    used.add(idx)
    
    FOR each (idx2, value2) in sorted_lows:
      IF idx2 in used: CONTINUE
      IF abs(value - value2) <= tolerance_abs:
        IF abs(idx - idx2) >= 2:
          cluster.append((idx2, value2))
          used.add(idx2)
    
    IF len(cluster) >= 2:
      clusters.append({
        level: mean([v for (_, v) in cluster]),
        touches: len(cluster),
        indices: [i for (i, _) in cluster],
        type: "SSL"  // Sell-Side Liquidity
      })
  
  RETURN clusters
```

### 5.4 Minor Swing High/Low Detection

```
DEFINITION:
  Swing High at index j: High[j] > High[j-1] AND High[j] > High[j+1]
  Swing Low at index j:  Low[j] < Low[j-1] AND Low[j] < Low[j+1]
  
  (Using 1-candle lookback/lookahead for 1-minute chart speed)

FUNCTION detect_swing_highs(candles[]):
  swings = []
  FOR j = 1 to len(candles) - 2:
    IF candles[j].High > candles[j-1].High AND candles[j].High > candles[j+1].High:
      swings.append({index: j, level: candles[j].High, type: "SWING_HIGH"})
  RETURN swings

FUNCTION detect_swing_lows(candles[]):
  swings = []
  FOR j = 1 to len(candles) - 2:
    IF candles[j].Low < candles[j-1].Low AND candles[j].Low < candles[j+1].Low:
      swings.append({index: j, level: candles[j].Low, type: "SWING_LOW"})
  RETURN swings
```

### 5.5 Internal Liquidity Pools

```
INTERNAL LIQUIDITY = swing highs and swing lows WITHIN the accumulation range
                     that are NOT at the range boundaries.

FUNCTION detect_internal_liquidity(candles[], range_high, range_low):
  range_size = range_high - range_low
  buffer = range_size * 0.15  // 15% from boundaries = internal
  
  internal_zone_high = range_high - buffer
  internal_zone_low = range_low + buffer
  
  swing_highs = detect_swing_highs(candles)
  swing_lows = detect_swing_lows(candles)
  
  internal_bsl = [sh for sh in swing_highs 
                  if sh.level < internal_zone_high AND sh.level > internal_zone_low]
  internal_ssl = [sl for sl in swing_lows 
                  if sl.level > internal_zone_low AND sl.level < internal_zone_high]
  
  RETURN {
    internal_bsl: internal_bsl,
    internal_ssl: internal_ssl,
    boundary_bsl: [sh for sh in swing_highs if sh.level >= internal_zone_high],
    boundary_ssl: [sl for sl in swing_lows if sl.level <= internal_zone_low]
  }

SIGNIFICANCE:
  - boundary_bsl/ssl → These are the PRIMARY manipulation targets
  - internal_bsl/ssl → These provide micro-structure understanding
  - More boundary liquidity = higher probability of manipulation at that level
```

### 5.6 Numerical Example — Liquidity Detection

```
EXAMPLE:

Accumulation Range: High = 18060, Low = 18040
Range_Size = 20 points
Price ≈ 18050
tolerance_abs = 18050 * 0.0002 = 3.61 points

Candle Highs within range:
  C[0].H = 18058, C[3].H = 18059, C[6].H = 18057, C[9].H = 18058, C[12].H = 18060

Clustering Equal Highs (tolerance = 3.61):
  |18058 - 18059| = 1 ≤ 3.61 → EQUAL
  |18058 - 18057| = 1 ≤ 3.61 → EQUAL  
  |18058 - 18058| = 0 ≤ 3.61 → EQUAL
  |18058 - 18060| = 2 ≤ 3.61 → EQUAL
  
  Cluster: level = mean(18058, 18059, 18057, 18058, 18060) = 18058.4
  Touches: 5
  Type: BSL (Buy-Side Liquidity)

Candle Lows within range:
  C[1].L = 18042, C[4].L = 18041, C[7].L = 18043, C[10].L = 18040

Clustering Equal Lows (tolerance = 3.61):
  |18042 - 18041| = 1 ≤ 3.61 → EQUAL
  |18042 - 18043| = 1 ≤ 3.61 → EQUAL
  |18042 - 18040| = 2 ≤ 3.61 → EQUAL
  
  Cluster: level = mean(18042, 18041, 18043, 18040) = 18041.5
  Touches: 4
  Type: SSL (Sell-Side Liquidity)

RESULT:
  BSL at ~18058.4 (5 touches) → Strong buy-side liquidity
  SSL at ~18041.5 (4 touches) → Strong sell-side liquidity
  
  Both sides have liquidity → HIGH QUALITY accumulation
  Manipulation can target EITHER side
```

---


## 6. EXPANSION AND MANIPULATION DETECTION

### 6.1 How the Bot Knows Accumulation Has FINISHED

```
ACCUMULATION ENDS when price LEAVES the range with intent.

Three possible exits:
  1. MANIPULATION (sweep + return) → AMD continues → TRADE SETUP
  2. GENUINE BREAKOUT (sustained move beyond range) → No AMD → INVALID
  3. TIMEOUT → Range too old → INVALID

The critical distinction: Did price SWEEP liquidity and return, or did it BREAK OUT?
```

### 6.2 Manipulation Detection Algorithm

```
DEFINITION:
  Manipulation = Price moves BEYOND the accumulation range boundary 
                 to SWEEP liquidity, then RETURNS inside the range 
                 or reverses direction.

FORMAL CONDITIONS FOR UPSIDE MANIPULATION (sweep of BSL):
  1. At least one candle's High exceeds Range_High
     → High[j] > Range_High + min_sweep_distance
     → where min_sweep_distance = Range_Size * 0.05 (at least 5% beyond range)
  
  2. The sweep is followed by a CLOSE back inside the range OR a bearish close
     → Close[j] <= Range_High (wick rejection)
     OR Close[j+1] < Range_High (next candle returns)
     OR Close[j+1] < Open[j+1] AND Close[j+1] < Close[j] (bearish follow-through)
  
  3. The sweep reaches into the BSL zone identified in §5
     → High[j] >= BSL_level (price actually took the liquidity)

FORMAL CONDITIONS FOR DOWNSIDE MANIPULATION (sweep of SSL):
  1. At least one candle's Low goes below Range_Low
     → Low[j] < Range_Low - min_sweep_distance
  
  2. The sweep is followed by a CLOSE back inside the range OR a bullish close
     → Close[j] >= Range_Low (wick rejection)
     OR Close[j+1] > Range_Low (next candle returns)
     OR Close[j+1] > Open[j+1] AND Close[j+1] > Close[j] (bullish follow-through)
  
  3. The sweep reaches into the SSL zone
     → Low[j] <= SSL_level

FUNCTION detect_manipulation(candles[], range_high, range_low, bsl_level, ssl_level):
  range_size = range_high - range_low
  min_sweep = range_size * 0.05
  
  FOR j = 0 to len(candles) - 2:
    // Check upside manipulation (BSL sweep)
    IF candles[j].High > range_high + min_sweep:
      IF candles[j].High >= bsl_level:  // Actually reached liquidity
        IF candles[j].Close <= range_high:  // Wick rejection
          RETURN {type: "UPSIDE_MANIPULATION", index: j, sweep_level: candles[j].High}
        ELIF j+1 < len(candles) AND candles[j+1].Close < range_high:
          RETURN {type: "UPSIDE_MANIPULATION", index: j, sweep_level: candles[j].High}
    
    // Check downside manipulation (SSL sweep)
    IF candles[j].Low < range_low - min_sweep:
      IF candles[j].Low <= ssl_level:  // Actually reached liquidity
        IF candles[j].Close >= range_low:  // Wick rejection
          RETURN {type: "DOWNSIDE_MANIPULATION", index: j, sweep_level: candles[j].Low}
        ELIF j+1 < len(candles) AND candles[j+1].Close > range_low:
          RETURN {type: "DOWNSIDE_MANIPULATION", index: j, sweep_level: candles[j].Low}
  
  RETURN None  // No manipulation detected yet
```

### 6.3 Differentiating: Pullback vs Manipulation vs Genuine Breakout

```
DECISION TREE:

Price moves beyond Range_High (or below Range_Low):
│
├── Q1: Did price reach the identified liquidity level?
│   ├── NO → Likely a NORMAL PULLBACK or partial move
│   │         Action: Continue monitoring, do not classify as manipulation
│   │
│   └── YES → Continue to Q2
│
├── Q2: How many candles CLOSED beyond the range?
│   ├── 0 candles closed beyond (wick only) → MANIPULATION (high confidence)
│   │
│   ├── 1 candle closed beyond → Check Q3
│   │
│   └── 2+ candles closed beyond → GENUINE BREAKOUT
│       Action: INVALIDATE accumulation
│
├── Q3: Did the candle after the sweep show reversal?
│   ├── YES (opposite direction close, or close back inside range)
│   │   → MANIPULATION (medium-high confidence)
│   │
│   └── NO (continuation in breakout direction)
│       → Wait one more candle
│       ├── Returns → MANIPULATION (medium confidence)
│       └── Continues → GENUINE BREAKOUT → INVALIDATE
│
└── ADDITIONAL FILTER — Displacement Check:
    After the sweep, is there a DISPLACEMENT candle?
    Displacement = candle with body > ATR * 0.8 moving AWAY from the sweep direction
    
    IF displacement present after sweep → CONFIRMED MANIPULATION
    IF no displacement within 3 candles → UNCERTAIN, reduce confidence
```

### 6.4 Market Structure Shift (MSS) Detection

```
DEFINITION:
  MSS = the first break of the most recent swing structure in the 
        OPPOSITE direction of the manipulation sweep.

AFTER UPSIDE MANIPULATION (BSL swept → expect bearish MSS):
  bearish_mss = TRUE when:
    Close[k] < most_recent_swing_low.level
    WHERE most_recent_swing_low formed AFTER the manipulation candle
    AND Close[k] is a candle BODY close (not just wick)

AFTER DOWNSIDE MANIPULATION (SSL swept → expect bullish MSS):
  bullish_mss = TRUE when:
    Close[k] > most_recent_swing_high.level
    WHERE most_recent_swing_high formed AFTER the manipulation candle
    AND Close[k] is a candle BODY close (not just wick)

FUNCTION detect_mss(candles[], manipulation_index, manipulation_type):
  post_manipulation_candles = candles[manipulation_index + 1 : ]
  
  IF manipulation_type == "DOWNSIDE_MANIPULATION":
    // SSL swept → look for bullish MSS
    // Find swing highs after manipulation
    swing_highs = detect_swing_highs(post_manipulation_candles)
    IF len(swing_highs) == 0: RETURN None
    
    target_level = swing_highs[0].level  // First swing high after manipulation
    
    FOR candle in post_manipulation_candles[swing_highs[0].index + 1 :]:
      IF candle.Close > target_level:
        RETURN {type: "BULLISH_MSS", level: target_level, confirmed: TRUE}
  
  IF manipulation_type == "UPSIDE_MANIPULATION":
    // BSL swept → look for bearish MSS
    swing_lows = detect_swing_lows(post_manipulation_candles)
    IF len(swing_lows) == 0: RETURN None
    
    target_level = swing_lows[0].level  // First swing low after manipulation
    
    FOR candle in post_manipulation_candles[swing_lows[0].index + 1 :]:
      IF candle.Close < target_level:
        RETURN {type: "BEARISH_MSS", level: target_level, confirmed: TRUE}
  
  RETURN None  // MSS not yet confirmed
```

### 6.5 Displacement Detection

```
DEFINITION:
  Displacement = a candle (or consecutive candles) showing strong directional 
                 momentum AWAY from the manipulation, confirming smart money intent.

DISPLACEMENT CONDITIONS:
  candle_body = abs(Close[k] - Open[k])
  
  displacement_present = TRUE if:
    candle_body >= ATR(20) * 0.8
    AND direction is OPPOSITE to manipulation sweep direction
    AND candle occurs within 5 candles after manipulation

  For DOWNSIDE manipulation → displacement is BULLISH:
    Close[k] > Open[k] AND (Close[k] - Open[k]) >= ATR * 0.8
    
  For UPSIDE manipulation → displacement is BEARISH:
    Open[k] > Close[k] AND (Open[k] - Close[k]) >= ATR * 0.8
```

### 6.6 Complete Manipulation Classification Table

| Scenario | Liquidity Reached? | Closes Beyond Range | Displacement After? | Classification |
|----------|-------------------|--------------------|--------------------|----------------|
| Wick above range, close inside | YES | 0 | YES | **CONFIRMED MANIPULATION** |
| Wick above range, close inside | YES | 0 | NO | **PROBABLE MANIPULATION** (wait) |
| Close above range, next returns | YES | 1 | YES | **CONFIRMED MANIPULATION** |
| Close above range, next returns | YES | 1 | NO | **UNCERTAIN** (wait 2 more candles) |
| 2+ closes above range | YES | 2+ | N/A | **GENUINE BREAKOUT** → Invalid |
| Price goes above range | NO | Any | Any | **PARTIAL MOVE** → Continue monitoring |
| Wick below range, close inside | YES | 0 | YES | **CONFIRMED MANIPULATION** |
| 2+ closes below range | YES | 2+ | N/A | **GENUINE BREAKOUT** → Invalid |

---


## 7. STATE MACHINE

### 7.1 State Diagram (Text)

```
                    ┌─────────────┐
                    │  NO_RANGE   │ ← Initial state / After reset
                    └──────┬──────┘
                           │
                           │ Sliding window detects range containment
                           │ AND candle_count >= MIN_CANDLES/2
                           ▼
               ┌───────────────────────┐
               │  POSSIBLE_ACCUMULATION │
               └───────────┬───────────┘
                           │
                           │ All §3.1 conditions met
                           │ AND confidence >= 0.4
                           │ AND liquidity detected on ≥ 1 side
                           ▼
              ┌────────────────────────────┐
              │  CONFIRMED_ACCUMULATION    │
              └────────────┬───────────────┘
                           │
                           │ Range locked (§2.4)
                           │ AND macro window active
                           ▼
                  ┌─────────────────┐
                  │   MONITORING    │ ← Waiting for manipulation
                  └────────┬────────┘
                           │
                           │ Price sweeps liquidity (§6.2)
                           │ AND returns / reverses
                           ▼
            ┌──────────────────────────────┐
            │   MANIPULATION_DETECTED      │
            └──────────────┬───────────────┘
                           │
                           │ MSS confirmed (§6.4)
                           │ AND displacement present (§6.5)
                           ▼
                ┌───────────────────────┐
                │   AMD_READY_SIGNAL    │ → Output to Entry Engine
                └───────────────────────┘


         ┌─────────┐
         │ INVALID │ ← Can be reached from ANY state
         └────┬────┘
              │
              │ After cooldown period (5 candles)
              ▼
         ┌─────────┐
         │  RESET  │ → Returns to NO_RANGE
         └─────────┘
```

### 7.2 State Definitions and Transitions

```
STATE: NO_RANGE
  Description: No accumulation activity detected. Bot is scanning for new ranges.
  
  ENTRY CONDITIONS:
    - System startup
    - After RESET
    - After previous AMD cycle completed
    
  EXIT CONDITIONS:
    → POSSIBLE_ACCUMULATION: when sliding window (size MIN_CANDLES/2 = 5) shows:
      - window_range <= ATR * RANGE_ATR_MAX
      - window_range >= ATR * RANGE_ATR_MIN
      - direction_change_ratio >= 0.25
      
  ACTIONS:
    - Continuously calculate ATR(20)
    - Run sliding window range check every new candle
    - Check if current time is within or approaching a macro window

---

STATE: POSSIBLE_ACCUMULATION
  Description: A potential range has been identified but not yet confirmed.
  
  ENTRY CONDITIONS:
    - From NO_RANGE when initial range criteria met
    
  EXIT CONDITIONS:
    → CONFIRMED_ACCUMULATION: when ALL of:
      - candle_count >= MIN_CANDLES (10)
      - Range_Size within [ATR*0.3, ATR*2.0]
      - net_displacement_ratio < 0.4
      - equal_highs >= 2 OR equal_lows >= 2
      - candle_behaviour_valid (§4.4)
      - confidence_score >= 0.4
      
    → INVALID: when ANY of:
      - Range_Size > ATR * 3.0
      - Single candle body > Range_Size * 0.8
      - Two consecutive closes outside range
      - candle_count > MAX_CANDLES without confirmation
      
  ACTIONS:
    - Continue tracking range_high, range_low
    - Count equal highs/lows
    - Calculate confidence score each candle
    - Allow range expansion (within limits)

---

STATE: CONFIRMED_ACCUMULATION
  Description: Valid accumulation range confirmed. Building liquidity profiles.
  
  ENTRY CONDITIONS:
    - From POSSIBLE_ACCUMULATION when all confirmation criteria met
    
  EXIT CONDITIONS:
    → MONITORING: when ALL of:
      - Range is LOCKED (§2.4)
      - At least one clear liquidity pool identified
      - Current time is within a MANIPULATION macro window
      
    → INVALID: when ANY of:
      - Range exceeds ATR * 3.0 (unexpected expansion)
      - candle_count > MAX_CANDLES
      - candle_behaviour degrades (avg_body exceeds thresholds)
      - 5 consecutive candles in same direction (trending started)
      
  ACTIONS:
    - Lock range boundaries when criteria met
    - Build complete liquidity map (BSL, SSL, internal liquidity)
    - Calculate final confidence score
    - Log accumulation characteristics for analysis

---

STATE: MONITORING
  Description: Accumulation confirmed and locked. Actively watching for manipulation.
  
  ENTRY CONDITIONS:
    - From CONFIRMED_ACCUMULATION when range locked during macro window
    
  EXIT CONDITIONS:
    → MANIPULATION_DETECTED: when manipulation criteria met (§6.2):
      - Price pierces range boundary
      - Reaches identified liquidity level
      - Shows reversal characteristics (wick rejection or next-candle return)
      
    → INVALID: when:
      - 2+ candle closes beyond range (genuine breakout)
      - Macro window expires without manipulation
      - 20 candles pass in monitoring without any boundary test
      
  ACTIONS:
    - Track every candle that approaches range boundaries
    - Check for manipulation on each new candle
    - Monitor time remaining in macro window
    - Alert if price approaches liquidity levels

---

STATE: MANIPULATION_DETECTED
  Description: Liquidity sweep occurred. Waiting for MSS confirmation.
  
  ENTRY CONDITIONS:
    - From MONITORING when manipulation criteria confirmed
    
  EXIT CONDITIONS:
    → AMD_READY_SIGNAL: when ALL of:
      - MSS confirmed (§6.4)
      - Displacement present (§6.5) — body > ATR * 0.8 in reversal direction
      
    → INVALID: when:
      - Price returns to sweep direction (manipulation failed)
        Specifically: 2 candles close beyond manipulation high/low
      - 10 candles pass without MSS
      - Price makes new high above BSL sweep (for upside manip)
        OR new low below SSL sweep (for downside manip)
      
  ACTIONS:
    - Track swing highs/lows forming after manipulation
    - Check for MSS on each candle
    - Check for displacement candle
    - Determine trade direction:
      - SSL swept → expect BULLISH outcome
      - BSL swept → expect BEARISH outcome

---

STATE: AMD_READY_SIGNAL
  Description: Full AMD sequence detected. Signal ready for Entry Engine.
  
  ENTRY CONDITIONS:
    - From MANIPULATION_DETECTED when MSS + displacement confirmed
    
  EXIT CONDITIONS:
    → RESET: after signal is emitted and consumed by Entry Engine
    → RESET: after 20 candles if no entry taken
    
  OUTPUT SIGNAL:
    {
      signal_type: "AMD_READY",
      direction: "LONG" or "SHORT",
      accumulation_range: {high, low, duration},
      manipulation: {type, sweep_level, index},
      mss: {type, level, confirmed},
      liquidity_swept: {level, touches, type},
      confidence: score,
      macro_window: current_macro_id,
      timestamp: current_time
    }

---

STATE: INVALID
  Description: Current accumulation hypothesis invalidated.
  
  ENTRY CONDITIONS:
    - From ANY state when invalidation criteria met
    
  EXIT CONDITIONS:
    → RESET: after cooldown_period (5 candles)
    
  ACTIONS:
    - Log reason for invalidation
    - Clear all tracking variables
    - Wait cooldown period before rescanning

---

STATE: RESET
  Description: Transition state that clears all data and returns to scanning.
  
  ENTRY CONDITIONS:
    - From INVALID after cooldown
    - From AMD_READY_SIGNAL after signal consumed
    
  EXIT CONDITIONS:
    → NO_RANGE: immediately
    
  ACTIONS:
    - Clear range_high, range_low
    - Clear liquidity arrays
    - Clear swing tracking
    - Reset candle_count = 0
    - Reset confidence_score = 0
    - Maintain ATR (do NOT reset ATR history)
```

### 7.3 State Transition Summary Table

| From State | To State | Trigger |
|-----------|----------|---------|
| NO_RANGE | POSSIBLE_ACCUMULATION | Sliding window detects range containment |
| POSSIBLE_ACCUMULATION | CONFIRMED_ACCUMULATION | All confirmation criteria met |
| POSSIBLE_ACCUMULATION | INVALID | Invalidation condition triggered |
| CONFIRMED_ACCUMULATION | MONITORING | Range locked + macro window active |
| CONFIRMED_ACCUMULATION | INVALID | Range expands or degrades |
| MONITORING | MANIPULATION_DETECTED | Liquidity sweep + return detected |
| MONITORING | INVALID | Breakout or timeout |
| MANIPULATION_DETECTED | AMD_READY_SIGNAL | MSS + Displacement confirmed |
| MANIPULATION_DETECTED | INVALID | Manipulation fails or times out |
| AMD_READY_SIGNAL | RESET | Signal consumed or timeout |
| INVALID | RESET | Cooldown period elapsed |
| RESET | NO_RANGE | Immediate |

---


## 8. EDGE CASES

### 8.1 Very Small Ranges

```
CONDITION: Range_Size < ATR * RANGE_ATR_MIN (< ATR * 0.3)

PROBLEM: Could be noise, spread fluctuation, or very low-volume period.

BOT BEHAVIOUR:
  - DO NOT classify as accumulation
  - State remains NO_RANGE
  - Log as "noise_range_rejected"
  - Continue scanning for larger range formation
  
EXAMPLE:
  ATR = 10 points, RANGE_ATR_MIN = 0.3
  If detected range = 2 points → 2 < 10*0.3 = 3 → REJECTED
```

### 8.2 Very Large Ranges

```
CONDITION: Range_Size > ATR * 3.0

PROBLEM: Price is likely trending within a large channel, not accumulating.

BOT BEHAVIOUR:
  - DO NOT classify as accumulation
  - IF range was previously confirmed and expanded beyond limit:
    → State → INVALID
  - IF detected fresh:
    → State remains NO_RANGE
    
IMPLEMENTATION NOTE:
  Large ranges CAN contain nested accumulations.
  If a large range is detected, scan WITHIN it for sub-ranges.
  
EXAMPLE:
  ATR = 10 points, max allowed = 30 points
  If range = 35 points → INVALID as single accumulation
  But check if 12-point sub-range exists within it → possible nested accumulation
```

### 8.3 High Volatility Periods

```
CONDITION: ATR is abnormally high (e.g., > 2x its 100-period average)

PROBLEM: 
  - Ranges form and break quickly
  - False manipulation signals increase
  - Normal candle noise exceeds normal range boundaries

BOT BEHAVIOUR:
  - Increase MIN_CANDLES dynamically: MIN_CANDLES = base_min * (current_ATR / avg_ATR)
  - Increase EQUAL_LEVEL_TOLERANCE proportionally
  - Require higher confidence threshold: min_confidence = 0.6 (instead of 0.5)
  - Reduce maximum position size (handled by Entry Engine, out of scope)

FORMULA:
  volatility_multiplier = current_ATR / SMA(ATR, 100)
  IF volatility_multiplier > 2.0:
    adjusted_MIN_CANDLES = ceil(MIN_CANDLES * volatility_multiplier * 0.5)
    adjusted_TOLERANCE = EQUAL_LEVEL_TOLERANCE * volatility_multiplier
    min_confidence_threshold = 0.6
```

### 8.4 Low Volatility Periods

```
CONDITION: ATR is abnormally low (e.g., < 0.5x its 100-period average)

PROBLEM:
  - Ranges may be very tight (but valid)
  - Manipulation sweeps will be small
  - Risk that genuine accumulation is missed because it doesn't meet minimum range

BOT BEHAVIOUR:
  - Decrease RANGE_ATR_MIN dynamically (allow tighter ranges)
  - Decrease min_sweep_distance for manipulation detection
  - Keep MIN_CANDLES the same (duration requirement unchanged)
  
FORMULA:
  IF volatility_multiplier < 0.5:
    adjusted_RANGE_ATR_MIN = RANGE_ATR_MIN * 0.5
    adjusted_min_sweep = range_size * 0.03 (instead of 0.05)
```

### 8.5 Weekend Gaps

```
CONDITION: Price gap between Friday close and Sunday/Monday open

PROBLEM:
  - A range detected on Friday may have a gap through it on Monday
  - Pre-existing accumulation becomes invalid

BOT BEHAVIOUR:
  - On session open (after gap > ATR * 0.5):
    → INVALIDATE all active accumulation tracking
    → State → RESET
    → Begin fresh scanning from new price level
    
  - Gap detection:
    IF |candle[current].Open - candle[previous].Close| > ATR * 0.5:
      gap_detected = TRUE
      RESET all states
      
  - DO NOT carry forward any range from before the gap
```

### 8.6 News Candles

```
CONDITION: High-impact news event (NFP, FOMC, CPI, GDP)

DERIVED FROM DOCUMENT: "DO NOT TRADE during high-impact news events"

BOT BEHAVIOUR:
  - Maintain a news calendar (external input required)
  - 30 minutes BEFORE news: freeze all new accumulation detection
  - DURING news: 
    → State = NO_RANGE (forced)
    → All existing tracking INVALIDATED
  - 30 minutes AFTER news:
    → Resume normal scanning
    → Use post-news ATR (recalculate, as ATR will spike)
    
  News blackout window = [news_time - 30min, news_time + 30min]
  
  IF current_time IN news_blackout_window:
    detection_enabled = FALSE
    RETURN immediately from all detection functions
```

### 8.7 Equal Highs/Lows (Edge Cases)

```
CASE 1: Exactly at range boundary
  IF equal highs ARE the range_high → They define the BSL target
  Treatment: Normal — these are the primary manipulation targets

CASE 2: All highs are equal (flat top)
  IF every candle high within 1% of each other:
  → Very strong BSL
  → confidence_bonus += 0.1
  
CASE 3: Equal levels but only 1 candle apart
  IF |index_a - index_b| < 2:
  → DO NOT count as separate liquidity touches
  → They are the same price action event
  
CASE 4: More than 5 equal touches
  → Cap liquidity_bonus at 0.2 (diminishing returns)
  → But flag as "strong_liquidity_magnet" for manipulation targeting
```

### 8.8 Fake Breakouts (False Manipulation)

```
CONDITION: Price briefly exits range but does NOT reach liquidity level

PROBLEM: Looks like manipulation but didn't actually sweep liquidity

BOT BEHAVIOUR:
  - IF price exceeds range boundary BUT does NOT reach identified liquidity level:
    → DO NOT classify as manipulation
    → Classify as "PARTIAL_SWEEP" or "FALSE_BREAKOUT"
    → State remains MONITORING
    → Range boundaries remain unchanged
    
  - IF this happens more than 2 times:
    → Increase confidence that TRUE manipulation will occur soon
    → Log "liquidity tease — building more orders at boundary"

DETECTION:
  fake_breakout = (High[j] > range_high) AND (High[j] < bsl_level) AND (Close[j] < range_high)
  → This is NOT manipulation, just a false breakout
```

### 8.9 Double Manipulations

```
CONDITION: Price sweeps liquidity on BOTH sides of the range

SEQUENCE:
  1. First sweep: SSL taken (price goes below range_low, returns)
  2. Second sweep: BSL taken (price goes above range_high, returns)
  OR vice versa.

BOT BEHAVIOUR:
  - Track manipulation_count per accumulation
  - IF manipulation_count == 1:
    → Normal flow → wait for MSS
    → IF MSS does not confirm within 10 candles:
      → State remains MONITORING (wait for second manipulation)
      
  - IF manipulation_count == 2:
    → The SECOND manipulation is the "true" manipulation
    → Use the SECOND sweep's direction for trade direction
    → Higher confidence (double sweep = more liquidity taken)
    → confidence_bonus += 0.15 for double manipulation
    
  - IF manipulation_count > 2:
    → Range is being chopped → INVALID
    → Market is not following AMD → RESET

TRADE DIRECTION AFTER DOUBLE MANIPULATION:
  - Last sweep was SSL (downside) → Trade direction = LONG
  - Last sweep was BSL (upside) → Trade direction = SHORT
```

### 8.10 Nested Accumulations

```
CONDITION: A second accumulation forms inside or immediately after the first

BOT BEHAVIOUR:
  Case A — Nested INSIDE:
    - Smaller range within confirmed accumulation
    - Track as micro-structure
    - Primary accumulation remains the OUTER range
    - Inner range provides more precise liquidity levels
    
  Case B — Sequential (after first resolves):
    - First accumulation led to manipulation but no MSS
    - Price returns and forms NEW accumulation
    - RESET first accumulation
    - Track new accumulation independently
    - This is a fresh AMD cycle attempt

RULE: Never track more than 2 accumulation objects simultaneously.
      If 3rd detected → invalidate oldest, keep 2 most recent.
```

### 8.11 Range Expansion Before Confirmation

```
CONDITION: Range keeps expanding (new highs/lows) before reaching MIN_CANDLES

BOT BEHAVIOUR:
  - Allow up to 3 expansions before candle_count reaches MIN_CANDLES
  - Each expansion reduces confidence by 0.05
  - IF more than 3 expansions before lock:
    → State = INVALID (price is trending, not consolidating)
    → RESET and rescan
    
  - After each expansion, RE-VALIDATE:
    → Is new Range_Size still <= ATR * RANGE_ATR_MAX?
    → Is net displacement still < 0.4?
    → Are candle behaviours still valid?
    
  IF all still valid after expansion → continue tracking
  IF any fail → INVALID
```

---


## 9. COMPLETE DETECTION ALGORITHM

### 9.1 High-Level Flow

```
┌──────────────────────────────────────────────────────────────────┐
│                    ACCUMULATION DETECTION ENGINE                   │
│                         Main Loop (per candle)                     │
└──────────────────────────────────────────────────────────────────┘

NEW 1-MINUTE CANDLE ARRIVES
         │
         ▼
┌─────────────────────┐     NO
│ Is detection enabled?│────────────► SKIP (news blackout or outside hours)
└─────────┬───────────┘
          │ YES
          ▼
┌─────────────────────┐
│ Update ATR(20)      │
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│ Check current state │
└─────────┬───────────┘
          │
          ├── NO_RANGE ──────────────► Run Range Scanner (§9.2)
          │
          ├── POSSIBLE_ACCUMULATION ─► Run Confirmation Engine (§9.3)
          │
          ├── CONFIRMED_ACCUMULATION ► Run Lock & Liquidity Engine (§9.4)
          │
          ├── MONITORING ────────────► Run Manipulation Scanner (§9.5)
          │
          ├── MANIPULATION_DETECTED ─► Run MSS Engine (§9.6)
          │
          ├── AMD_READY_SIGNAL ──────► Emit Signal (§9.7)
          │
          ├── INVALID ───────────────► Run Cooldown (§9.8)
          │
          └── RESET ─────────────────► Clear & Return to NO_RANGE
```

### 9.2 Step 1: Range Scanner

```
FUNCTION range_scanner(candles[], atr):
  
  // Sliding window approach
  W = MIN_CANDLES // 2  // = 5 (half of minimum to detect early)
  
  IF len(candles) < W:
    RETURN NO_CHANGE
  
  window = candles[last W candles]
  window_high = max(c.High for c in window)
  window_low = min(c.Low for c in window)
  window_range = window_high - window_low
  
  // Check range size validity
  IF window_range < atr * RANGE_ATR_MIN:
    RETURN NO_CHANGE  // Too small, noise
    
  IF window_range > atr * RANGE_ATR_MAX:
    RETURN NO_CHANGE  // Too large, trending
  
  // Check net displacement
  net_disp = abs(window[last].Close - window[0].Close) / window_range
  IF net_disp >= NET_DISPLACEMENT_MAX:
    RETURN NO_CHANGE  // Directional movement
  
  // Check direction changes
  directions = [1 if c.Close > c.Open else -1 for c in window]
  changes = count(directions[i] != directions[i-1] for i in 1..W-1)
  IF changes / (W-1) < 0.25:
    RETURN NO_CHANGE  // Too directional
  
  // Range detected!
  TRANSITION to POSSIBLE_ACCUMULATION
  SET range_high = window_high
  SET range_low = window_low
  SET start_index = index of window[0]
  SET candle_count = W
  
  RETURN STATE_CHANGED
```

### 9.3 Step 2: Confirmation Engine

```
FUNCTION confirmation_engine(new_candle, state_data, atr):
  
  // Update range if candle within limits
  IF new_candle.High > state_data.range_high:
    new_range = new_candle.High - state_data.range_low
    IF new_range > atr * RANGE_ATR_MAX:
      TRANSITION to INVALID("range_exceeded_max")
      RETURN
    state_data.range_high = new_candle.High
    state_data.expansion_count += 1
    
  IF new_candle.Low < state_data.range_low:
    new_range = state_data.range_high - new_candle.Low
    IF new_range > atr * RANGE_ATR_MAX:
      TRANSITION to INVALID("range_exceeded_max")
      RETURN
    state_data.range_low = new_candle.Low
    state_data.expansion_count += 1
  
  state_data.candle_count += 1
  
  // Check for impulse invalidation
  candle_body = abs(new_candle.Close - new_candle.Open)
  range_size = state_data.range_high - state_data.range_low
  IF candle_body > range_size * 0.8:
    TRANSITION to INVALID("impulse_inside_range")
    RETURN
  
  // Check expansion limit
  IF state_data.expansion_count > 3 AND state_data.candle_count < MIN_CANDLES:
    TRANSITION to INVALID("excessive_expansion")
    RETURN
  
  // Check timeout
  IF state_data.candle_count > MAX_CANDLES:
    TRANSITION to INVALID("timeout")
    RETURN
  
  // Check for breakout (two consecutive closes outside)
  IF last_two_closes_outside_range(state_data):
    TRANSITION to INVALID("genuine_breakout")
    RETURN
  
  // Check if confirmation criteria met
  IF state_data.candle_count >= MIN_CANDLES:
    // Run all confirmation checks
    equal_highs = detect_equal_highs(state_data.candles, EQUAL_LEVEL_TOLERANCE)
    equal_lows = detect_equal_lows(state_data.candles, EQUAL_LEVEL_TOLERANCE)
    
    liquidity_present = (len(equal_highs) > 0 AND equal_highs[0].touches >= 2) 
                     OR (len(equal_lows) > 0 AND equal_lows[0].touches >= 2)
    
    IF NOT liquidity_present:
      RETURN  // Keep waiting, not yet confirmed
    
    // Validate candle behaviour
    behaviour = validate_candle_behaviour(state_data.candles, state_data.range_high, state_data.range_low)
    IF NOT behaviour.valid:
      TRANSITION to INVALID(behaviour.reason)
      RETURN
    
    // Calculate confidence
    confidence = calculate_confidence(state_data, equal_highs, equal_lows, atr)
    IF confidence < 0.4:
      RETURN  // Keep waiting, confidence too low
    
    // All criteria met!
    state_data.equal_highs = equal_highs
    state_data.equal_lows = equal_lows
    state_data.confidence = confidence
    TRANSITION to CONFIRMED_ACCUMULATION
```

### 9.4 Step 3: Lock & Liquidity Engine

```
FUNCTION lock_and_liquidity_engine(new_candle, state_data, atr, current_time):
  
  range_size = state_data.range_high - state_data.range_low
  
  // Count touches to boundaries
  high_zone_threshold = state_data.range_high - (range_size * 0.1)
  low_zone_threshold = state_data.range_low + (range_size * 0.1)
  
  IF new_candle.High >= high_zone_threshold:
    state_data.high_touches += 1
  IF new_candle.Low <= low_zone_threshold:
    state_data.low_touches += 1
  
  state_data.candle_count += 1
  
  // Check if range should be locked
  range_locked = (
    state_data.candle_count >= MIN_CANDLES
    AND state_data.high_touches >= 2
    AND state_data.low_touches >= 2
  )
  
  IF range_locked:
    state_data.locked = TRUE
    
    // Build complete liquidity map
    state_data.liquidity_map = detect_internal_liquidity(
      state_data.candles, state_data.range_high, state_data.range_low
    )
    
    // Determine primary BSL and SSL levels
    IF len(state_data.equal_highs) > 0:
      state_data.bsl_target = state_data.equal_highs[0].level  // Strongest cluster
    ELSE:
      state_data.bsl_target = state_data.range_high
      
    IF len(state_data.equal_lows) > 0:
      state_data.ssl_target = state_data.equal_lows[0].level
    ELSE:
      state_data.ssl_target = state_data.range_low
    
    // Check macro window
    IF is_within_macro_window(current_time, "MANIPULATION"):
      TRANSITION to MONITORING
    ELSE:
      // Range locked but not in macro window yet
      // Stay in CONFIRMED_ACCUMULATION until macro window opens
      // But set flag for immediate transition when window opens
      state_data.ready_for_monitoring = TRUE
  
  // Invalidation checks
  IF state_data.candle_count > MAX_CANDLES:
    TRANSITION to INVALID("timeout_before_lock")
    RETURN
```

### 9.5 Step 4: Manipulation Scanner

```
FUNCTION manipulation_scanner(new_candle, prev_candle, state_data, atr):
  
  range_size = state_data.range_high - state_data.range_low
  min_sweep = range_size * 0.05
  
  // ═══════════════════════════════════════════
  // CHECK UPSIDE MANIPULATION (BSL sweep)
  // ═══════════════════════════════════════════
  IF new_candle.High > state_data.range_high + min_sweep:
    
    // Did it reach BSL?
    reached_bsl = new_candle.High >= state_data.bsl_target
    
    IF reached_bsl:
      // Check for wick rejection (close inside range)
      IF new_candle.Close <= state_data.range_high:
        state_data.manipulation = {
          type: "UPSIDE_MANIPULATION",
          sweep_level: new_candle.High,
          candle_index: current_index,
          direction_after: "SHORT"  // Expect bearish move
        }
        TRANSITION to MANIPULATION_DETECTED
        RETURN
      ELSE:
        // Candle closed above range — wait for next candle
        state_data.pending_manipulation = {
          type: "UPSIDE",
          sweep_candle: new_candle,
          index: current_index
        }
        RETURN
    ELSE:
      // Partial sweep — not manipulation
      state_data.partial_sweeps += 1
      RETURN
  
  // Check if previous pending manipulation confirms
  IF state_data.pending_manipulation != None:
    IF state_data.pending_manipulation.type == "UPSIDE":
      IF new_candle.Close < state_data.range_high:  // Returned inside
        state_data.manipulation = {
          type: "UPSIDE_MANIPULATION",
          sweep_level: state_data.pending_manipulation.sweep_candle.High,
          candle_index: state_data.pending_manipulation.index,
          direction_after: "SHORT"
        }
        state_data.pending_manipulation = None
        TRANSITION to MANIPULATION_DETECTED
        RETURN
      ELIF new_candle.Close > state_data.pending_manipulation.sweep_candle.High:
        // Continuation above — genuine breakout
        TRANSITION to INVALID("genuine_breakout_up")
        RETURN
  
  // ═══════════════════════════════════════════
  // CHECK DOWNSIDE MANIPULATION (SSL sweep)
  // ═══════════════════════════════════════════
  IF new_candle.Low < state_data.range_low - min_sweep:
    
    reached_ssl = new_candle.Low <= state_data.ssl_target
    
    IF reached_ssl:
      IF new_candle.Close >= state_data.range_low:
        state_data.manipulation = {
          type: "DOWNSIDE_MANIPULATION",
          sweep_level: new_candle.Low,
          candle_index: current_index,
          direction_after: "LONG"
        }
        TRANSITION to MANIPULATION_DETECTED
        RETURN
      ELSE:
        state_data.pending_manipulation = {
          type: "DOWNSIDE",
          sweep_candle: new_candle,
          index: current_index
        }
        RETURN
    ELSE:
      state_data.partial_sweeps += 1
      RETURN
  
  // Check pending downside
  IF state_data.pending_manipulation != None:
    IF state_data.pending_manipulation.type == "DOWNSIDE":
      IF new_candle.Close > state_data.range_low:
        state_data.manipulation = {
          type: "DOWNSIDE_MANIPULATION",
          sweep_level: state_data.pending_manipulation.sweep_candle.Low,
          candle_index: state_data.pending_manipulation.index,
          direction_after: "LONG"
        }
        state_data.pending_manipulation = None
        TRANSITION to MANIPULATION_DETECTED
        RETURN
      ELIF new_candle.Close < state_data.pending_manipulation.sweep_candle.Low:
        TRANSITION to INVALID("genuine_breakout_down")
        RETURN
  
  // Timeout check
  state_data.monitoring_candles += 1
  IF state_data.monitoring_candles > 20:
    TRANSITION to INVALID("monitoring_timeout_no_manipulation")
```

### 9.6 Step 5: MSS Engine

```
FUNCTION mss_engine(new_candle, state_data, atr):
  
  state_data.post_manipulation_candles.append(new_candle)
  post_count = len(state_data.post_manipulation_candles)
  
  // Timeout check
  IF post_count > 10:
    TRANSITION to INVALID("mss_timeout")
    RETURN
  
  // Failure check — price continues in manipulation direction
  IF state_data.manipulation.type == "DOWNSIDE_MANIPULATION":
    IF new_candle.Low < state_data.manipulation.sweep_level:
      TRANSITION to INVALID("manipulation_failed_new_low")
      RETURN
  IF state_data.manipulation.type == "UPSIDE_MANIPULATION":
    IF new_candle.High > state_data.manipulation.sweep_level:
      TRANSITION to INVALID("manipulation_failed_new_high")
      RETURN
  
  // ═══════════════════════════════════════════
  // DETECT MSS
  // ═══════════════════════════════════════════
  
  IF state_data.manipulation.direction_after == "LONG":
    // Looking for BULLISH MSS (break above swing high)
    
    // Find swing highs in post-manipulation candles
    swing_highs = detect_swing_highs(state_data.post_manipulation_candles)
    
    IF len(swing_highs) > 0 AND NOT state_data.mss_target_set:
      state_data.mss_target = swing_highs[0].level
      state_data.mss_target_set = TRUE
    
    IF state_data.mss_target_set:
      IF new_candle.Close > state_data.mss_target:  // BODY close above swing high
        // MSS CONFIRMED!
        // Now check for displacement
        candle_body = new_candle.Close - new_candle.Open
        IF candle_body >= atr * 0.8:  // This IS the displacement candle
          state_data.mss = {type: "BULLISH_MSS", level: state_data.mss_target}
          state_data.displacement = TRUE
          TRANSITION to AMD_READY_SIGNAL
          RETURN
        ELSE:
          // MSS confirmed but displacement not yet
          state_data.mss = {type: "BULLISH_MSS", level: state_data.mss_target}
          state_data.awaiting_displacement = TRUE
    
    // Check displacement if MSS already confirmed
    IF state_data.awaiting_displacement:
      candle_body = new_candle.Close - new_candle.Open  // Positive = bullish
      IF candle_body >= atr * 0.8:
        state_data.displacement = TRUE
        TRANSITION to AMD_READY_SIGNAL
        RETURN
  
  IF state_data.manipulation.direction_after == "SHORT":
    // Looking for BEARISH MSS (break below swing low)
    
    swing_lows = detect_swing_lows(state_data.post_manipulation_candles)
    
    IF len(swing_lows) > 0 AND NOT state_data.mss_target_set:
      state_data.mss_target = swing_lows[0].level
      state_data.mss_target_set = TRUE
    
    IF state_data.mss_target_set:
      IF new_candle.Close < state_data.mss_target:
        candle_body = new_candle.Open - new_candle.Close  // Positive = bearish
        IF candle_body >= atr * 0.8:
          state_data.mss = {type: "BEARISH_MSS", level: state_data.mss_target}
          state_data.displacement = TRUE
          TRANSITION to AMD_READY_SIGNAL
          RETURN
        ELSE:
          state_data.mss = {type: "BEARISH_MSS", level: state_data.mss_target}
          state_data.awaiting_displacement = TRUE
    
    IF state_data.awaiting_displacement:
      candle_body = new_candle.Open - new_candle.Close
      IF candle_body >= atr * 0.8:
        state_data.displacement = TRUE
        TRANSITION to AMD_READY_SIGNAL
        RETURN
```

### 9.7 Step 6: Signal Emission

```
FUNCTION emit_amd_signal(state_data):
  
  signal = {
    signal_type: "AMD_READY",
    timestamp: current_time,
    
    // Direction
    direction: state_data.manipulation.direction_after,  // "LONG" or "SHORT"
    
    // Accumulation Data
    accumulation: {
      range_high: state_data.range_high,
      range_low: state_data.range_low,
      range_size: state_data.range_high - state_data.range_low,
      duration_candles: state_data.candle_count,
      start_index: state_data.start_index,
      confidence: state_data.confidence,
      bsl_level: state_data.bsl_target,
      ssl_level: state_data.ssl_target,
      equal_high_touches: state_data.equal_highs[0].touches if exists else 0,
      equal_low_touches: state_data.equal_lows[0].touches if exists else 0
    },
    
    // Manipulation Data
    manipulation: {
      type: state_data.manipulation.type,
      sweep_level: state_data.manipulation.sweep_level,
      sweep_candle_index: state_data.manipulation.candle_index
    },
    
    // MSS Data
    mss: {
      type: state_data.mss.type,
      level: state_data.mss.level
    },
    
    // Context
    macro_window: get_current_macro_window(current_time),
    market: current_market_symbol
  }
  
  EMIT signal to Entry Engine
  
  // Start timeout counter
  state_data.signal_emitted_at = current_candle_index
  // Will RESET after 20 candles if no entry taken
```

### 9.8 Step 7: Cooldown & Reset

```
FUNCTION handle_cooldown(state_data):
  state_data.cooldown_counter += 1
  
  IF state_data.cooldown_counter >= 5:  // 5 candle cooldown
    TRANSITION to RESET
    
FUNCTION handle_reset():
  CLEAR all state_data
  PRESERVE atr_history  // Do not reset ATR
  PRESERVE candle_history  // Keep candle buffer
  TRANSITION to NO_RANGE
```

---


## 10. PSEUDO CODE

### 10.1 Main Engine Class Structure

```python
# ═══════════════════════════════════════════════════════════════
# ICT ACCUMULATION DETECTION ENGINE — PRODUCTION PSEUDO CODE
# Timeframe: 1-Minute
# Markets: US100, US500, US30
# ═══════════════════════════════════════════════════════════════


# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

CONSTANTS:
  TIMEFRAME = "1min"
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
  MAX_EXPANSIONS_BEFORE_LOCK = 3
  CONFIDENCE_MIN = 0.4
  SIGNAL_TIMEOUT = 20

# ═══════════════════════════════════════════════════════════════
# ENUMERATIONS
# ═══════════════════════════════════════════════════════════════

ENUM State:
  NO_RANGE
  POSSIBLE_ACCUMULATION
  CONFIRMED_ACCUMULATION
  MONITORING
  MANIPULATION_DETECTED
  AMD_READY_SIGNAL
  INVALID
  RESET

ENUM ManipulationType:
  UPSIDE_MANIPULATION    // BSL swept
  DOWNSIDE_MANIPULATION  // SSL swept

ENUM MSSType:
  BULLISH_MSS
  BEARISH_MSS

ENUM TradeDirection:
  LONG
  SHORT
```


```python
# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

STRUCT Candle:
  timestamp: DateTime
  open: Float
  high: Float
  low: Float
  close: Float

STRUCT LiquidityCluster:
  level: Float
  touches: Integer
  indices: List[Integer]
  type: String  // "BSL" or "SSL"

STRUCT SwingPoint:
  index: Integer
  level: Float
  type: String  // "SWING_HIGH" or "SWING_LOW"

STRUCT Manipulation:
  type: ManipulationType
  sweep_level: Float
  candle_index: Integer
  direction_after: TradeDirection

STRUCT MSS:
  type: MSSType
  level: Float
  confirmed: Boolean

STRUCT AccumulationState:
  state: State
  range_high: Float
  range_low: Float
  start_index: Integer
  candle_count: Integer
  expansion_count: Integer
  high_touches: Integer
  low_touches: Integer
  locked: Boolean
  confidence: Float
  candles: List[Candle]
  equal_highs: List[LiquidityCluster]
  equal_lows: List[LiquidityCluster]
  bsl_target: Float
  ssl_target: Float
  manipulation: Manipulation or None
  pending_manipulation: Object or None
  mss: MSS or None
  mss_target: Float
  mss_target_set: Boolean
  awaiting_displacement: Boolean
  displacement: Boolean
  post_manipulation_candles: List[Candle]
  monitoring_candles: Integer
  partial_sweeps: Integer
  cooldown_counter: Integer
  signal_emitted_at: Integer
  ready_for_monitoring: Boolean

STRUCT AMDSignal:
  signal_type: String
  timestamp: DateTime
  direction: TradeDirection
  accumulation_range_high: Float
  accumulation_range_low: Float
  accumulation_range_size: Float
  accumulation_duration: Integer
  accumulation_confidence: Float
  manipulation_type: ManipulationType
  manipulation_sweep_level: Float
  mss_type: MSSType
  mss_level: Float
  bsl_level: Float
  ssl_level: Float
  macro_window_id: Integer
  market: String
```


```python
# ═══════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════

FUNCTION calculate_atr(candles: List[Candle], period: Integer = 20) -> Float:
  IF len(candles) < period + 1:
    RETURN None
  
  true_ranges = []
  FOR i = 1 TO len(candles) - 1:
    tr = max(
      candles[i].high - candles[i].low,
      abs(candles[i].high - candles[i-1].close),
      abs(candles[i].low - candles[i-1].close)
    )
    true_ranges.append(tr)
  
  atr = mean(true_ranges[len(true_ranges) - period : ])
  RETURN atr


FUNCTION is_within_macro_window(time: DateTime, window_type: String) -> Boolean:
  macro_windows = [
    {start: "17:50", end: "18:20", type: "MANIPULATION"},
    {start: "18:20", end: "18:40", type: "CONTINUATION"},
    {start: "18:40", end: "19:20", type: "MANIPULATION"},
    {start: "19:20", end: "19:40", type: "CONTINUATION"},
    {start: "19:40", end: "20:20", type: "MANIPULATION"},
    {start: "20:20", end: "20:40", type: "CONTINUATION"},
    {start: "22:50", end: "23:20", type: "MANIPULATION"},
    {start: "23:20", end: "23:40", type: "CONTINUATION"},
    {start: "23:40", end: "00:20", type: "MANIPULATION"},
    {start: "00:20", end: "00:40", type: "CONTINUATION"},
  ]
  
  FOR window IN macro_windows:
    IF time >= window.start AND time <= window.end:
      IF window_type == None OR window.type == window_type:
        RETURN TRUE
  
  RETURN FALSE


FUNCTION is_news_blackout(time: DateTime, news_calendar: List) -> Boolean:
  FOR event IN news_calendar:
    IF event.type IN ["NFP", "FOMC", "CPI", "GDP"]:
      blackout_start = event.time - 30 minutes
      blackout_end = event.time + 30 minutes
      IF time >= blackout_start AND time <= blackout_end:
        RETURN TRUE
  RETURN FALSE


FUNCTION detect_swing_highs(candles: List[Candle]) -> List[SwingPoint]:
  swings = []
  FOR j = 1 TO len(candles) - 2:
    IF candles[j].high > candles[j-1].high AND candles[j].high > candles[j+1].high:
      swings.append(SwingPoint(index=j, level=candles[j].high, type="SWING_HIGH"))
  RETURN swings


FUNCTION detect_swing_lows(candles: List[Candle]) -> List[SwingPoint]:
  swings = []
  FOR j = 1 TO len(candles) - 2:
    IF candles[j].low < candles[j-1].low AND candles[j].low < candles[j+1].low:
      swings.append(SwingPoint(index=j, level=candles[j].low, type="SWING_LOW"))
  RETURN swings
```


```python
FUNCTION detect_equal_highs(candles: List[Candle], tolerance_pct: Float) -> List[LiquidityCluster]:
  price = candles[len(candles)-1].close
  tolerance_abs = price * tolerance_pct
  
  highs = [(i, c.high) FOR i, c IN enumerate(candles)]
  sorted_highs = sort(highs, key=lambda x: x[1], descending=TRUE)
  
  clusters = []
  used = set()
  
  FOR (idx, value) IN sorted_highs:
    IF idx IN used:
      CONTINUE
    
    cluster = [(idx, value)]
    used.add(idx)
    
    FOR (idx2, value2) IN sorted_highs:
      IF idx2 IN used:
        CONTINUE
      IF abs(value - value2) <= tolerance_abs AND abs(idx - idx2) >= 2:
        cluster.append((idx2, value2))
        used.add(idx2)
    
    IF len(cluster) >= 2:
      avg_level = mean([v FOR (_, v) IN cluster])
      clusters.append(LiquidityCluster(
        level=avg_level,
        touches=len(cluster),
        indices=[i FOR (i, _) IN cluster],
        type="BSL"
      ))
  
  // Sort by touches descending (strongest first)
  clusters = sort(clusters, key=lambda x: x.touches, descending=TRUE)
  RETURN clusters


FUNCTION detect_equal_lows(candles: List[Candle], tolerance_pct: Float) -> List[LiquidityCluster]:
  price = candles[len(candles)-1].close
  tolerance_abs = price * tolerance_pct
  
  lows = [(i, c.low) FOR i, c IN enumerate(candles)]
  sorted_lows = sort(lows, key=lambda x: x[1], ascending=TRUE)
  
  clusters = []
  used = set()
  
  FOR (idx, value) IN sorted_lows:
    IF idx IN used:
      CONTINUE
    
    cluster = [(idx, value)]
    used.add(idx)
    
    FOR (idx2, value2) IN sorted_lows:
      IF idx2 IN used:
        CONTINUE
      IF abs(value - value2) <= tolerance_abs AND abs(idx - idx2) >= 2:
        cluster.append((idx2, value2))
        used.add(idx2)
    
    IF len(cluster) >= 2:
      avg_level = mean([v FOR (_, v) IN cluster])
      clusters.append(LiquidityCluster(
        level=avg_level,
        touches=len(cluster),
        indices=[i FOR (i, _) IN cluster],
        type="SSL"
      ))
  
  clusters = sort(clusters, key=lambda x: x.touches, descending=TRUE)
  RETURN clusters


FUNCTION calculate_confidence(state: AccumulationState, 
                              equal_highs: List, 
                              equal_lows: List, 
                              atr: Float) -> Float:
  range_size = state.range_high - state.range_low
  
  // Base score
  score = 0.4
  
  // Liquidity bonus
  total_touches = 0
  IF len(equal_highs) > 0:
    total_touches += equal_highs[0].touches
  IF len(equal_lows) > 0:
    total_touches += equal_lows[0].touches
  liquidity_bonus = min(0.2, (total_touches - 2) * 0.05)
  score += liquidity_bonus
  
  // Duration bonus
  duration_bonus = min(0.15, (state.candle_count - MIN_CANDLES) * 0.01)
  score += duration_bonus
  
  // Compression bonus (tighter range = better)
  compression_ratio = 1.0 - (range_size / (atr * RANGE_ATR_MAX))
  compression_bonus = min(0.15, compression_ratio * 0.2)
  score += max(0, compression_bonus)
  
  // Expansion penalty
  expansion_penalty = state.expansion_count * 0.05
  score -= expansion_penalty
  
  // Displacement penalty
  net_disp = abs(state.candles[-1].close - state.candles[0].close) / range_size
  displacement_penalty = net_disp * 0.3
  score -= displacement_penalty
  
  // Clamp to [0, 1]
  score = max(0.0, min(1.0, score))
  RETURN score


FUNCTION validate_candle_behaviour(candles: List[Candle], 
                                    range_high: Float, 
                                    range_low: Float) -> {valid: Boolean, reason: String}:
  range_size = range_high - range_low
  N = len(candles)
  
  // Average body check
  bodies = [abs(c.close - c.open) FOR c IN candles]
  avg_body = mean(bodies)
  IF avg_body > range_size * 0.3:
    RETURN {valid: FALSE, reason: "avg_body_too_large"}
  
  // Max single body check
  max_body = max(bodies)
  IF max_body > range_size * 0.6:
    RETURN {valid: FALSE, reason: "single_impulse_candle"}
  
  // Direction change ratio
  directions = [1 IF c.close > c.open ELSE -1 FOR c IN candles]
  changes = sum(1 FOR i IN 1..N-1 IF directions[i] != directions[i-1])
  change_ratio = changes / (N - 1)
  IF change_ratio < 0.25:
    RETURN {valid: FALSE, reason: "too_directional"}
  
  // Max consecutive run
  max_run = 1
  current_run = 1
  FOR i = 1 TO N-1:
    IF directions[i] == directions[i-1]:
      current_run += 1
      max_run = max(max_run, current_run)
    ELSE:
      current_run = 1
  IF max_run > 5:
    RETURN {valid: FALSE, reason: "sustained_directional_run"}
  
  RETURN {valid: TRUE, reason: None}
```


```python
# ═══════════════════════════════════════════════════════════════
# MAIN ENGINE CLASS
# ═══════════════════════════════════════════════════════════════

CLASS AccumulationDetectionEngine:

  CONSTRUCTOR(market: String, news_calendar: List):
    self.market = market
    self.news_calendar = news_calendar
    self.candle_buffer = []          // Rolling buffer of last 100 candles
    self.atr = None
    self.state = AccumulationState()
    self.state.state = State.NO_RANGE
    self.signals = []                // Output queue

  # ─────────────────────────────────────────────────────────────
  # MAIN ENTRY POINT — Called every new 1-minute candle
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION on_new_candle(candle: Candle):
    
    // Add to buffer
    self.candle_buffer.append(candle)
    IF len(self.candle_buffer) > 100:
      self.candle_buffer.pop(0)
    
    // Pre-checks
    IF is_news_blackout(candle.timestamp, self.news_calendar):
      self._force_invalidate("news_blackout")
      RETURN
    
    // Update ATR
    self.atr = calculate_atr(self.candle_buffer, ATR_PERIOD)
    IF self.atr == None:
      RETURN  // Insufficient data
    
    // State machine dispatch
    SWITCH self.state.state:
      
      CASE State.NO_RANGE:
        self._scan_for_range(candle)
      
      CASE State.POSSIBLE_ACCUMULATION:
        self._run_confirmation(candle)
      
      CASE State.CONFIRMED_ACCUMULATION:
        self._run_lock_and_liquidity(candle)
      
      CASE State.MONITORING:
        self._scan_manipulation(candle)
      
      CASE State.MANIPULATION_DETECTED:
        self._run_mss_engine(candle)
      
      CASE State.AMD_READY_SIGNAL:
        self._manage_signal(candle)
      
      CASE State.INVALID:
        self._run_cooldown()
      
      CASE State.RESET:
        self._reset()
```


```python
  # ─────────────────────────────────────────────────────────────
  # STATE: NO_RANGE → Scan for potential range
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _scan_for_range(candle: Candle):
    W = MIN_CANDLES // 2  // = 5
    
    IF len(self.candle_buffer) < W:
      RETURN
    
    window = self.candle_buffer[-W:]
    window_high = max(c.high FOR c IN window)
    window_low = min(c.low FOR c IN window)
    window_range = window_high - window_low
    
    // Size checks
    IF window_range < self.atr * RANGE_ATR_MIN:
      RETURN
    IF window_range > self.atr * RANGE_ATR_MAX:
      RETURN
    
    // Net displacement check
    net_disp = abs(window[-1].close - window[0].close)
    IF window_range > 0 AND net_disp / window_range >= NET_DISPLACEMENT_MAX:
      RETURN
    
    // Direction oscillation check
    directions = [1 IF c.close > c.open ELSE -1 FOR c IN window]
    changes = sum(1 FOR i IN 1..W-1 IF directions[i] != directions[i-1])
    IF changes / (W - 1) < 0.25:
      RETURN
    
    // Potential range found!
    self.state.state = State.POSSIBLE_ACCUMULATION
    self.state.range_high = window_high
    self.state.range_low = window_low
    self.state.start_index = len(self.candle_buffer) - W
    self.state.candle_count = W
    self.state.candles = list(window)
    self.state.expansion_count = 0
    self.state.locked = FALSE

  # ─────────────────────────────────────────────────────────────
  # STATE: POSSIBLE_ACCUMULATION → Confirm or invalidate
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _run_confirmation(candle: Candle):
    self.state.candles.append(candle)
    self.state.candle_count += 1
    
    range_size = self.state.range_high - self.state.range_low
    
    // Update range boundaries (allow expansion within limits)
    IF candle.high > self.state.range_high:
      new_range = candle.high - self.state.range_low
      IF new_range > self.atr * RANGE_ATR_MAX:
        self._transition_invalid("range_exceeded_max_on_expansion")
        RETURN
      self.state.range_high = candle.high
      self.state.expansion_count += 1
      range_size = new_range
    
    IF candle.low < self.state.range_low:
      new_range = self.state.range_high - candle.low
      IF new_range > self.atr * RANGE_ATR_MAX:
        self._transition_invalid("range_exceeded_max_on_expansion")
        RETURN
      self.state.range_low = candle.low
      self.state.expansion_count += 1
      range_size = new_range
    
    // Impulse candle check
    candle_body = abs(candle.close - candle.open)
    IF range_size > 0 AND candle_body > range_size * 0.8:
      self._transition_invalid("impulse_candle_inside_range")
      RETURN
    
    // Expansion limit
    IF self.state.expansion_count > MAX_EXPANSIONS_BEFORE_LOCK:
      IF self.state.candle_count < MIN_CANDLES:
        self._transition_invalid("excessive_expansion_before_min_candles")
        RETURN
    
    // Timeout
    IF self.state.candle_count > MAX_CANDLES:
      self._transition_invalid("timeout")
      RETURN
    
    // Breakout check (2 consecutive closes outside)
    IF self.state.candle_count >= 2:
      prev = self.state.candles[-2]
      buffer_abs = candle.close * BREAKOUT_BUFFER
      IF prev.close > self.state.range_high + buffer_abs AND candle.close > self.state.range_high + buffer_abs:
        self._transition_invalid("genuine_breakout_up")
        RETURN
      IF prev.close < self.state.range_low - buffer_abs AND candle.close < self.state.range_low - buffer_abs:
        self._transition_invalid("genuine_breakout_down")
        RETURN
    
    // Confirmation check (only after minimum candles)
    IF self.state.candle_count >= MIN_CANDLES:
      
      // Detect liquidity
      equal_highs = detect_equal_highs(self.state.candles, EQUAL_LEVEL_TOLERANCE)
      equal_lows = detect_equal_lows(self.state.candles, EQUAL_LEVEL_TOLERANCE)
      
      has_liquidity = (len(equal_highs) > 0 AND equal_highs[0].touches >= 2) \
                   OR (len(equal_lows) > 0 AND equal_lows[0].touches >= 2)
      
      IF NOT has_liquidity:
        RETURN  // Keep waiting
      
      // Validate behaviour
      behaviour = validate_candle_behaviour(
        self.state.candles, self.state.range_high, self.state.range_low
      )
      IF NOT behaviour.valid:
        self._transition_invalid(behaviour.reason)
        RETURN
      
      // Calculate confidence
      confidence = calculate_confidence(self.state, equal_highs, equal_lows, self.atr)
      IF confidence < CONFIDENCE_MIN:
        RETURN  // Keep waiting
      
      // CONFIRMED!
      self.state.state = State.CONFIRMED_ACCUMULATION
      self.state.equal_highs = equal_highs
      self.state.equal_lows = equal_lows
      self.state.confidence = confidence
      self.state.high_touches = 0
      self.state.low_touches = 0
```


```python
  # ─────────────────────────────────────────────────────────────
  # STATE: CONFIRMED_ACCUMULATION → Lock range, build liquidity
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _run_lock_and_liquidity(candle: Candle):
    self.state.candles.append(candle)
    self.state.candle_count += 1
    
    range_size = self.state.range_high - self.state.range_low
    high_zone = self.state.range_high - (range_size * 0.1)
    low_zone = self.state.range_low + (range_size * 0.1)
    
    // Count boundary touches
    IF candle.high >= high_zone:
      self.state.high_touches += 1
    IF candle.low <= low_zone:
      self.state.low_touches += 1
    
    // Timeout check
    IF self.state.candle_count > MAX_CANDLES:
      self._transition_invalid("timeout_in_confirmed")
      RETURN
    
    // Check for lock
    IF NOT self.state.locked:
      IF self.state.high_touches >= 2 AND self.state.low_touches >= 2:
        self.state.locked = TRUE
        
        // Set BSL and SSL targets
        IF len(self.state.equal_highs) > 0:
          self.state.bsl_target = self.state.equal_highs[0].level
        ELSE:
          self.state.bsl_target = self.state.range_high
        
        IF len(self.state.equal_lows) > 0:
          self.state.ssl_target = self.state.equal_lows[0].level
        ELSE:
          self.state.ssl_target = self.state.range_low
    
    // Transition to MONITORING if locked and in macro window
    IF self.state.locked:
      IF is_within_macro_window(candle.timestamp, None):  // Any macro window
        self.state.state = State.MONITORING
        self.state.monitoring_candles = 0
        self.state.partial_sweeps = 0
        self.state.pending_manipulation = None

  # ─────────────────────────────────────────────────────────────
  # STATE: MONITORING → Watch for manipulation
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _scan_manipulation(candle: Candle):
    self.state.candles.append(candle)
    self.state.monitoring_candles += 1
    
    range_size = self.state.range_high - self.state.range_low
    min_sweep = range_size * MIN_SWEEP_RATIO
    buffer_abs = candle.close * BREAKOUT_BUFFER
    
    // ── Check pending manipulation from previous candle ──
    IF self.state.pending_manipulation != None:
      pending = self.state.pending_manipulation
      
      IF pending.type == "UPSIDE":
        IF candle.close < self.state.range_high:  // Returned inside
          self.state.manipulation = Manipulation(
            type=ManipulationType.UPSIDE_MANIPULATION,
            sweep_level=pending.sweep_candle.high,
            candle_index=pending.index,
            direction_after=TradeDirection.SHORT
          )
          self.state.pending_manipulation = None
          self.state.state = State.MANIPULATION_DETECTED
          self.state.post_manipulation_candles = []
          self.state.mss_target_set = FALSE
          self.state.awaiting_displacement = FALSE
          RETURN
        ELIF candle.close > pending.sweep_candle.high:
          self._transition_invalid("genuine_breakout_up_confirmed")
          RETURN
      
      IF pending.type == "DOWNSIDE":
        IF candle.close > self.state.range_low:  // Returned inside
          self.state.manipulation = Manipulation(
            type=ManipulationType.DOWNSIDE_MANIPULATION,
            sweep_level=pending.sweep_candle.low,
            candle_index=pending.index,
            direction_after=TradeDirection.LONG
          )
          self.state.pending_manipulation = None
          self.state.state = State.MANIPULATION_DETECTED
          self.state.post_manipulation_candles = []
          self.state.mss_target_set = FALSE
          self.state.awaiting_displacement = FALSE
          RETURN
        ELIF candle.close < pending.sweep_candle.low:
          self._transition_invalid("genuine_breakout_down_confirmed")
          RETURN
    
    // ── Check for UPSIDE sweep (BSL) ──
    IF candle.high > self.state.range_high + min_sweep:
      IF candle.high >= self.state.bsl_target:  // Reached liquidity
        IF candle.close <= self.state.range_high:  // Wick rejection
          self.state.manipulation = Manipulation(
            type=ManipulationType.UPSIDE_MANIPULATION,
            sweep_level=candle.high,
            candle_index=len(self.state.candles) - 1,
            direction_after=TradeDirection.SHORT
          )
          self.state.state = State.MANIPULATION_DETECTED
          self.state.post_manipulation_candles = []
          self.state.mss_target_set = FALSE
          self.state.awaiting_displacement = FALSE
          RETURN
        ELSE:
          // Closed above — pending
          self.state.pending_manipulation = {
            type: "UPSIDE",
            sweep_candle: candle,
            index: len(self.state.candles) - 1
          }
          RETURN
      ELSE:
        self.state.partial_sweeps += 1
    
    // ── Check for DOWNSIDE sweep (SSL) ──
    IF candle.low < self.state.range_low - min_sweep:
      IF candle.low <= self.state.ssl_target:  // Reached liquidity
        IF candle.close >= self.state.range_low:  // Wick rejection
          self.state.manipulation = Manipulation(
            type=ManipulationType.DOWNSIDE_MANIPULATION,
            sweep_level=candle.low,
            candle_index=len(self.state.candles) - 1,
            direction_after=TradeDirection.LONG
          )
          self.state.state = State.MANIPULATION_DETECTED
          self.state.post_manipulation_candles = []
          self.state.mss_target_set = FALSE
          self.state.awaiting_displacement = FALSE
          RETURN
        ELSE:
          self.state.pending_manipulation = {
            type: "DOWNSIDE",
            sweep_candle: candle,
            index: len(self.state.candles) - 1
          }
          RETURN
      ELSE:
        self.state.partial_sweeps += 1
    
    // ── Check for genuine breakout (2 closes beyond) ──
    IF self.state.candle_count >= 2:
      prev = self.state.candles[-2]
      IF prev.close > self.state.range_high + buffer_abs AND candle.close > self.state.range_high + buffer_abs:
        self._transition_invalid("genuine_breakout_up_double_close")
        RETURN
      IF prev.close < self.state.range_low - buffer_abs AND candle.close < self.state.range_low - buffer_abs:
        self._transition_invalid("genuine_breakout_down_double_close")
        RETURN
    
    // ── Timeout ──
    IF self.state.monitoring_candles > MONITORING_TIMEOUT:
      self._transition_invalid("monitoring_timeout")
```


```python
  # ─────────────────────────────────────────────────────────────
  # STATE: MANIPULATION_DETECTED → Find MSS + Displacement
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _run_mss_engine(candle: Candle):
    self.state.post_manipulation_candles.append(candle)
    post_count = len(self.state.post_manipulation_candles)
    
    // Timeout
    IF post_count > MSS_TIMEOUT:
      self._transition_invalid("mss_timeout")
      RETURN
    
    // Failure: price continues in sweep direction
    IF self.state.manipulation.type == ManipulationType.DOWNSIDE_MANIPULATION:
      IF candle.low < self.state.manipulation.sweep_level:
        self._transition_invalid("new_low_after_manipulation")
        RETURN
    
    IF self.state.manipulation.type == ManipulationType.UPSIDE_MANIPULATION:
      IF candle.high > self.state.manipulation.sweep_level:
        self._transition_invalid("new_high_after_manipulation")
        RETURN
    
    // ── Looking for BULLISH MSS (after SSL sweep) ──
    IF self.state.manipulation.direction_after == TradeDirection.LONG:
      
      // Find swing high target for MSS
      IF NOT self.state.mss_target_set:
        swing_highs = detect_swing_highs(self.state.post_manipulation_candles)
        IF len(swing_highs) > 0:
          self.state.mss_target = swing_highs[0].level
          self.state.mss_target_set = TRUE
      
      // Check MSS break
      IF self.state.mss_target_set AND NOT self.state.awaiting_displacement:
        IF candle.close > self.state.mss_target:  // Body close above swing high
          body = candle.close - candle.open
          IF body >= self.atr * DISPLACEMENT_ATR_RATIO:
            // MSS + Displacement in same candle
            self.state.mss = MSS(type=MSSType.BULLISH_MSS, level=self.state.mss_target, confirmed=TRUE)
            self.state.displacement = TRUE
            self.state.state = State.AMD_READY_SIGNAL
            self._emit_signal()
            RETURN
          ELSE:
            // MSS confirmed, awaiting displacement
            self.state.mss = MSS(type=MSSType.BULLISH_MSS, level=self.state.mss_target, confirmed=TRUE)
            self.state.awaiting_displacement = TRUE
      
      // Check displacement if MSS already confirmed
      IF self.state.awaiting_displacement:
        body = candle.close - candle.open
        IF body >= self.atr * DISPLACEMENT_ATR_RATIO:
          self.state.displacement = TRUE
          self.state.state = State.AMD_READY_SIGNAL
          self._emit_signal()
          RETURN
    
    // ── Looking for BEARISH MSS (after BSL sweep) ──
    IF self.state.manipulation.direction_after == TradeDirection.SHORT:
      
      IF NOT self.state.mss_target_set:
        swing_lows = detect_swing_lows(self.state.post_manipulation_candles)
        IF len(swing_lows) > 0:
          self.state.mss_target = swing_lows[0].level
          self.state.mss_target_set = TRUE
      
      IF self.state.mss_target_set AND NOT self.state.awaiting_displacement:
        IF candle.close < self.state.mss_target:  // Body close below swing low
          body = candle.open - candle.close  // Bearish body (positive value)
          IF body >= self.atr * DISPLACEMENT_ATR_RATIO:
            self.state.mss = MSS(type=MSSType.BEARISH_MSS, level=self.state.mss_target, confirmed=TRUE)
            self.state.displacement = TRUE
            self.state.state = State.AMD_READY_SIGNAL
            self._emit_signal()
            RETURN
          ELSE:
            self.state.mss = MSS(type=MSSType.BEARISH_MSS, level=self.state.mss_target, confirmed=TRUE)
            self.state.awaiting_displacement = TRUE
      
      IF self.state.awaiting_displacement:
        body = candle.open - candle.close
        IF body >= self.atr * DISPLACEMENT_ATR_RATIO:
          self.state.displacement = TRUE
          self.state.state = State.AMD_READY_SIGNAL
          self._emit_signal()
          RETURN

  # ─────────────────────────────────────────────────────────────
  # STATE: AMD_READY_SIGNAL → Emit and manage
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _manage_signal(candle: Candle):
    // Signal already emitted; wait for timeout then reset
    candles_since_signal = current_index - self.state.signal_emitted_at
    IF candles_since_signal > SIGNAL_TIMEOUT:
      self.state.state = State.RESET

  FUNCTION _emit_signal():
    signal = AMDSignal(
      signal_type="AMD_READY",
      timestamp=current_time(),
      direction=self.state.manipulation.direction_after,
      accumulation_range_high=self.state.range_high,
      accumulation_range_low=self.state.range_low,
      accumulation_range_size=self.state.range_high - self.state.range_low,
      accumulation_duration=self.state.candle_count,
      accumulation_confidence=self.state.confidence,
      manipulation_type=self.state.manipulation.type,
      manipulation_sweep_level=self.state.manipulation.sweep_level,
      mss_type=self.state.mss.type,
      mss_level=self.state.mss.level,
      bsl_level=self.state.bsl_target,
      ssl_level=self.state.ssl_target,
      macro_window_id=get_current_macro_id(current_time()),
      market=self.market
    )
    self.signals.append(signal)
    self.state.signal_emitted_at = current_index

  # ─────────────────────────────────────────────────────────────
  # STATE: INVALID → Cooldown
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _run_cooldown():
    self.state.cooldown_counter += 1
    IF self.state.cooldown_counter >= COOLDOWN_CANDLES:
      self.state.state = State.RESET

  # ─────────────────────────────────────────────────────────────
  # STATE: RESET → Clear all
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _reset():
    self.state = AccumulationState()
    self.state.state = State.NO_RANGE
    // NOTE: Do NOT clear self.candle_buffer or self.atr

  # ─────────────────────────────────────────────────────────────
  # UTILITY
  # ─────────────────────────────────────────────────────────────
  
  FUNCTION _transition_invalid(reason: String):
    self.state.state = State.INVALID
    self.state.cooldown_counter = 0
    LOG("Accumulation invalidated: " + reason)
  
  FUNCTION _force_invalidate(reason: String):
    IF self.state.state != State.NO_RANGE:
      self._transition_invalid(reason)
    // If already NO_RANGE, do nothing
```


```python
# ═══════════════════════════════════════════════════════════════
# USAGE EXAMPLE
# ═══════════════════════════════════════════════════════════════

// Initialize engine
engine = AccumulationDetectionEngine(
  market="US100",
  news_calendar=load_news_calendar()
)

// Main loop — called by data feed
FUNCTION on_candle_received(candle: Candle):
  engine.on_new_candle(candle)
  
  // Check for signals
  IF len(engine.signals) > 0:
    signal = engine.signals.pop(0)
    
    // Pass to Entry Engine
    entry_engine.process_amd_signal(signal)
    
    // Signal contains all information needed:
    // - Direction (LONG/SHORT)
    // - Accumulation range (for premium/discount calculation)
    // - Manipulation level (for stop-loss reference)
    // - MSS level (for entry timing)
    // - Confidence (for position sizing)
```

---

## APPENDIX A: IMPLEMENTATION ASSUMPTIONS

The following assumptions were made where the source document was ambiguous:

| # | Assumption | Rationale |
|---|-----------|-----------|
| 1 | MIN_CANDLES = 10 | 20-minute macro / 2 = minimum meaningful consolidation |
| 2 | MAX_CANDLES = 60 | 1 hour max; exceeding this spans multiple macros |
| 3 | RANGE_ATR_MAX = 2.0 | Range must be bounded; 2x ATR allows normal oscillation |
| 4 | RANGE_ATR_MIN = 0.3 | Below this is market noise/spread |
| 5 | EQUAL_LEVEL_TOLERANCE = 0.02% | Accounts for minor price differences at same level |
| 6 | NET_DISPLACEMENT_MAX = 0.4 | If price moves >40% of range, it's trending not consolidating |
| 7 | Displacement = body > 0.8 * ATR | Strong candle indicating institutional participation |
| 8 | MSS requires BODY close | Wicks don't confirm structure breaks per ICT methodology |
| 9 | 2 consecutive closes = breakout | Single close could be manipulation; two confirms direction |
| 10 | 5-candle cooldown after invalidation | Prevents immediate re-detection of same invalid structure |
| 11 | Confidence threshold 0.4 minimum | Below this, too many false positives expected |
| 12 | Macro window is required | Document states all entries must be during macro windows |

---

## APPENDIX B: COMPLETE VALID/INVALID CASE EXAMPLES

### VALID CASE — Perfect Accumulation → Manipulation → Signal

```
Timeline (IST): 6:40 PM - 7:10 PM (Macro 3 - High Priority)
Market: NASDAQ (US100)
ATR(20) = 12 points

Candle Flow:
  6:40 PM: O=18500 H=18508 L=18495 C=18503  ← Range begins
  6:41 PM: O=18503 H=18510 L=18498 C=18499
  6:42 PM: O=18499 H=18509 L=18494 C=18506
  6:43 PM: O=18506 H=18511 L=18497 C=18498  ← Equal high forming ~18510
  6:44 PM: O=18498 H=18505 L=18493 C=18502
  6:45 PM: O=18502 H=18510 L=18496 C=18497  ← 3rd touch at ~18510 (BSL)
  6:46 PM: O=18497 H=18504 L=18492 C=18500
  6:47 PM: O=18500 H=18507 L=18493 C=18494  ← Equal low forming ~18493
  6:48 PM: O=18494 H=18503 L=18492 C=18501  ← 2nd touch at ~18492 (SSL)
  6:49 PM: O=18501 H=18509 L=18495 C=18498
  
  // At this point (10 candles):
  // range_high = 18511, range_low = 18492, Range_Size = 19
  // 19 <= ATR*2 = 24 ✓
  // Net displacement = |18498 - 18503| / 19 = 0.26 < 0.4 ✓
  // Equal Highs at ~18510 (3 touches) ✓
  // Equal Lows at ~18492 (2 touches) ✓
  // → STATE: CONFIRMED_ACCUMULATION → MONITORING
  
  6:50 PM: O=18498 H=18505 L=18494 C=18501  ← Still inside
  6:51 PM: O=18501 H=18514 L=18499 C=18503  ← Wick above BSL! High=18514 > 18510
           // Reached BSL (18514 > 18510) ✓
           // Close inside range (18503 < 18511) ✓
           // → MANIPULATION DETECTED (UPSIDE)
           // → Direction = SHORT
  
  6:52 PM: O=18503 H=18504 L=18495 C=18496  ← Swing low forming
  6:53 PM: O=18496 H=18499 L=18490 C=18492  ← Lower
  6:54 PM: O=18492 H=18495 L=18488 C=18489  ← New low (swing low at 18495 from 6:52)
           // Swing low at 18495 (from 6:52 candle)
           // Close at 18489 < 18495 → BEARISH MSS CONFIRMED!
           // Body = |18492 - 18489| = 3... not enough for displacement
           // awaiting_displacement = TRUE
  
  6:55 PM: O=18489 H=18490 L=18478 C=18479  ← DISPLACEMENT!
           // Body = 18489 - 18479 = 10 (bearish)
           // 10 >= ATR * 0.8 = 9.6 ✓
           // → DISPLACEMENT CONFIRMED
           // → STATE: AMD_READY_SIGNAL
  
OUTPUT SIGNAL:
  {
    signal_type: "AMD_READY",
    direction: SHORT,
    accumulation_range_high: 18511,
    accumulation_range_low: 18492,
    accumulation_range_size: 19,
    accumulation_duration: 12,
    accumulation_confidence: 0.72,
    manipulation_type: UPSIDE_MANIPULATION,
    manipulation_sweep_level: 18514,
    mss_type: BEARISH_MSS,
    mss_level: 18495,
    bsl_level: 18510,
    ssl_level: 18492,
    macro_window_id: 3,
    market: "US100"
  }
```

### INVALID CASE 1 — Range Too Wide (Trending)

```
ATR = 10 points
Candles over 8 minutes: price moves from 18500 to 18530
Range = 30 points > ATR * 2.0 = 20
→ REJECTED: Never enters POSSIBLE_ACCUMULATION
```

### INVALID CASE 2 — No Liquidity Formed

```
ATR = 10 points, Range = 15 points (valid size)
Duration = 15 candles (valid)
But: All highs are different levels, all lows are different levels
No equal highs cluster, no equal lows cluster
→ REJECTED: Stays in POSSIBLE_ACCUMULATION, never confirms
→ Eventually times out (MAX_CANDLES) → INVALID
```

### INVALID CASE 3 — Genuine Breakout (Not Manipulation)

```
Valid accumulation confirmed: Range 18500-18520, BSL at 18518
Monitoring state:
  Candle A: H=18522, C=18521 (close above range)
  Candle B: C=18525 (another close above range)
→ Two consecutive closes above → GENUINE BREAKOUT
→ INVALID (this is not AMD, price broke out legitimately)
```

### INVALID CASE 4 — During News Event

```
NFP release at 6:00 PM IST
Blackout: 5:30 PM - 6:30 PM
At 5:50 PM: Macro 1 begins, accumulation detected
→ BUT news_blackout = TRUE
→ FORCE INVALIDATE all tracking
→ No trades until 6:30 PM
```

### INVALID CASE 5 — Manipulation Without MSS

```
Valid accumulation, manipulation detected (SSL swept)
Post-manipulation: 10 candles pass, no swing high broken
→ MSS_TIMEOUT
→ INVALID
→ Possible: market is going to make new lows (sweep was not manipulation, it was genuine)
```

---

## APPENDIX C: SIGNAL OUTPUT INTERFACE

```
// The Accumulation Detection Engine outputs AMDSignal objects.
// The Entry Engine (separate module) consumes these signals.

INTERFACE AccumulationEngine_Output:
  
  METHOD get_signals() -> List[AMDSignal]:
    // Returns all pending signals
    // Caller should process and acknowledge
    
  METHOD acknowledge_signal(signal_id: String):
    // Marks signal as consumed
    // Engine can reset after acknowledgment
    
  METHOD get_current_state() -> State:
    // Returns current state for monitoring/dashboard
    
  METHOD get_accumulation_data() -> AccumulationState or None:
    // Returns full state data if in active tracking
    // Returns None if in NO_RANGE or RESET
```

---

## APPENDIX D: CONFIGURATION TUNING GUIDE

```
FOR MORE SENSITIVE DETECTION (more signals, more false positives):
  - Decrease MIN_CANDLES (e.g., 7)
  - Increase EQUAL_LEVEL_TOLERANCE (e.g., 0.0003)
  - Decrease CONFIDENCE_MIN (e.g., 0.35)
  - Increase NET_DISPLACEMENT_MAX (e.g., 0.5)

FOR LESS SENSITIVE DETECTION (fewer signals, higher quality):
  - Increase MIN_CANDLES (e.g., 15)
  - Decrease EQUAL_LEVEL_TOLERANCE (e.g., 0.00015)
  - Increase CONFIDENCE_MIN (e.g., 0.6)
  - Decrease NET_DISPLACEMENT_MAX (e.g., 0.3)
  - Require liquidity on BOTH sides (not just one)

RECOMMENDED STARTING VALUES (from this specification):
  MIN_CANDLES = 10
  MAX_CANDLES = 60
  RANGE_ATR_MAX = 2.0
  RANGE_ATR_MIN = 0.3
  EQUAL_LEVEL_TOLERANCE = 0.0002
  NET_DISPLACEMENT_MAX = 0.4
  CONFIDENCE_MIN = 0.5  // Start at 0.5 for balanced detection
  
BACKTESTING RECOMMENDATION:
  Run on 3 months of 1-minute data for each market (US100, US500, US30)
  Track: signals generated, signals that led to valid entries, win rate
  Adjust constants based on signal-to-noise ratio
```

---

**END OF DOCUMENT**

*Document Version: 1.0*  
*Derived From: ICT Hydra Macro Strategy for Indices*  
*Scope: Accumulation Detection ONLY (Entry Engine is a separate specification)*  
*All analysis on 1-minute timeframe as specified in source document*
