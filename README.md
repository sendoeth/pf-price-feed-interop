# Post Fiat Canonical Price Feed Interop Pack

Deterministic price resolution protocol that eliminates private interpretation of `(symbol, timestamp)` pairs across producers, auditors, and consumers.

**Problem**: When two systems resolve the same signal at the same timestamp, they can arrive at different prices — one uses Binance spot, the other uses CoinGecko, one interpolates, the other rejects stale data. The resulting "same signal, different price" discrepancy breaks resolution scoring, karma accounting, and trust verification.

**Solution**: A canonical quote schema + deterministic resolution policy that every participant applies identically. Given the same inputs and policy, every implementation MUST produce the same resolved price.

## Quick Start

```bash
# Replay all 8 fixture cases
python3 resolve_quotes.py

# Single case
python3 resolve_quotes.py --case PFQ-006

# JSON output with attestation chain
python3 resolve_quotes.py --json --attest

# Validate fixtures
python3 resolve_quotes.py --validate

# Build attestation chain example
python3 resolve_quotes.py --chain attestation_example.json

# Run tests
python3 -m pytest tests/ -v
```

## Files

| File | Purpose |
|------|---------|
| `quote_schema.json` | JSON Schema for normalized price quotes (symbol, timestamp, source, price, confidence, attestation_hash, source_tier, feed_version) |
| `resolution_policy.json` | Deterministic rules: 24h attribution path, source hierarchy, staleness, tolerance, interpolation, dispute fallback |
| `resolve_quotes.py` | Resolution engine + fixture replay runner + attestation chain builder |
| `attestation_example.json` | Two consecutive BTC quotes linked by SHA-256 hash chain |
| `replay_output.json` | Full replay transcript (8/8 MATCH) |
| `fixtures/PFQ-*.json` | 8 fixture cases covering every resolution path |
| `tests/test_resolve_quotes.py` | 133 tests across 20 classes |

## Quote Schema

Every price observation is a single `(symbol, timestamp)` record from a named source:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `quote_id` | string | yes | Unique identifier |
| `symbol` | string | yes | Uppercase asset symbol (`^[A-Z][A-Z0-9_]{1,19}$`) |
| `timestamp` | date-time | yes | ISO-8601 UTC observation time |
| `source` | object | yes | `source_id`, `source_type`, `priority` |
| `price` | number | yes | Observed price in USD (> 0) |
| `confidence` | number | yes | Source reliability 0.0-1.0 |
| `attestation_hash` | string | yes | SHA-256 hash of canonical fields (`quote_id\|symbol\|timestamp\|source_id\|price\|confidence`) |
| `source_tier` | enum | yes | Source reliability tier: `tier_1_exchange`, `tier_2_aggregator`, `tier_3_api`, `tier_4_manual`, `tier_5_interpolated` |
| `feed_version` | string | yes | Semantic version of feed spec this quote conforms to (e.g. `1.0.0`) |
| `staleness_seconds` | number | no | Seconds between requested and actual observation |
| `interpolated` | boolean | no | True if interpolated between neighbors |
| `attestation` | object | no | Full hash-chain attestation linking to previous quote |

## Resolution Policy

The resolution policy defines 6 deterministic dimensions for one **24-hour attribution path**: every signal emitted at time T is resolved against the price at T+24h using these rules.

### 0. Attribution Path

Fixed 24-hour window. A signal's outcome is determined by the canonical price at exactly T+24h after emission. The rules below govern how that T+24h price is resolved when the exact observation is missing, stale, or disputed.

### 1. Source Hierarchy

Ordered list of price sources by priority. Three selection rules:
- **`first_valid`** (default): Highest-priority non-stale source wins
- **`median_of_top_n`**: Median of top N non-stale sources (outlier resistance)
- **`weighted_average`**: Confidence-weighted mean

### 2. Staleness

Maximum acceptable age of a price observation. Default: 7200s (2 hours).

When all sources are stale, `stale_action` determines the path:
- **`reject`**: No resolution, signal skipped
- **`fallback_source`**: Try next source in hierarchy
- **`interpolate`**: Attempt interpolation between neighbors

### 3. Tolerance

Maximum acceptable deviation between sources. Default: 2.0%.

When sources disagree beyond tolerance, `deviation_action` determines the path:
- **`use_primary`**: Accept highest-priority source despite disagreement
- **`use_median`**: Use median of all available sources
- **`dispute`**: Enter the dispute fallback chain

### 4. Interpolation

Rules for computing a price when no exact-timestamp observation exists:
- **`linear`**: Time-weighted between bracketing neighbors
- **`nearest`**: Closest observation by time

Interpolated prices receive a confidence penalty (default: 0.5x).

Maximum gap between neighbors: 14,400s (4 hours).

### 5. Dispute Fallback Chain

Ordered steps when sources disagree or all are stale:
```
["next_source", "interpolate", "use_median", "mark_unresolved"]
```
First successful step wins. `mark_unresolved` is terminal.

## Fixture Cases

| Case | Title | Status | Path Tested |
|------|-------|--------|-------------|
| PFQ-001 | Primary source fresh | RESOLVED | `first_valid` — direct selection |
| PFQ-002 | Primary stale, secondary fresh | RESOLVED | `fallback_source` — cascade |
| PFQ-003 | All sources stale, reject | UNRESOLVED | `stale_action=reject` |
| PFQ-004 | Linear interpolation | RESOLVED | `stale_action=interpolate` with confidence penalty |
| PFQ-005 | Within tolerance boundary | RESOLVED | Deviation 0.95% < 2.0% threshold |
| PFQ-006 | Conflicting sources | RESOLVED_DISPUTE | Deviation 3.33% > 2.0%, `use_median` fallback |
| PFQ-007 | Interpolation gap exceeded | UNRESOLVED | Gap 28,800s > 3,600s max, chain exhausted |
| PFQ-008 | Median of top 3 | RESOLVED | `median_of_top_n` outlier elimination |

## Hash-Chained Attestation

Every quote can carry a SHA-256 attestation linking it to the previous record:

```
canonical_string = "quote_id|symbol|timestamp|source_id|price(2dp)|confidence(6dp)"
hash = SHA-256(canonical_string)
```

Genesis record uses `previous_hash = "0" * 64`. Each subsequent record links to the prior hash, forming a tamper-evident chain.

The `attestation_example.json` file contains two consecutive BTC quotes demonstrating this chain.

## How This Plugs Into Existing Protocols

This interop pack addresses the **price ambiguity gap** in the Post Fiat signal lifecycle:

1. **Signal Schema** (`pf-signal-schema`) defines what a signal IS
2. **Resolution Protocol** (`pf-resolution-protocol`) resolves signal outcomes against prices
3. **Price Feed Interop** (this repo) ensures the prices used in step 2 are deterministic
4. **Consumer Audit** (`pf-consumer-audit`) can now verify that two auditors using the same policy arrive at identical resolved prices

Without canonical price resolution, two auditors resolving the same signal can disagree on the outcome — not because the signal was ambiguous, but because they looked up different prices. This pack eliminates that class of inconsistency.

## Design Decisions

1. **Policy as data, not code**: `resolution_policy.json` is machine-readable so implementations in any language can parse it
2. **Fixture-driven verification**: Any implementation can replay the fixtures to prove compliance
3. **Zero external dependencies**: Pure Python stdlib for maximum portability
4. **Hash chain optional**: Attestation is opt-in per quote, not mandatory — supports both lightweight and high-assurance deployments
5. **Confidence penalty is multiplicative**: Interpolated prices carry degraded confidence (base * penalty), not absolute confidence assignment
6. **Deviation is min-max range**: `(max - min) / min * 100` — simple, deterministic, no ambiguity about reference price

## Tests

133 tests across 20 classes:

- Timestamp parsing (6)
- Hash computation (6)
- Attestation building (3)
- Chain verification (5)
- Chain building (3)
- Attestation example (5)
- Staleness detection (6)
- Source priority (2)
- Deviation computation (6)
- Resolution paths — primary, fallback, interpolation, tolerance, median, weighted avg, dispute chain (26)
- Policy overrides (2)
- Fixture loading and replay (10)
- Schema validation (9)
- Cross-fixture consistency (8)
- Edge cases (6)
- File output (2)
- Constants (6)
- attestation_hash field (4)
- source_tier field (6)
- feed_version field (4)
- enrich_quote utility (2)
- 24h attribution path (3)
- Attestation example new fields (3)

```bash
python3 -m pytest tests/ -v
```

## License

MIT
