#!/usr/bin/env python3
"""
Post Fiat Canonical Price Feed Resolver

Deterministic price resolution engine that eliminates private interpretation
of (symbol, timestamp) pairs. Implements the resolution_policy.json contract:
source hierarchy, staleness gates, tolerance checks, interpolation, dispute
fallback, and hash-chained attestation.

Usage:
    python3 resolve_quotes.py                          # Replay all fixtures
    python3 resolve_quotes.py --case PFQ-001           # Replay single fixture
    python3 resolve_quotes.py --json                   # JSON output
    python3 resolve_quotes.py -o report.json           # Write to file
    python3 resolve_quotes.py --attest                 # Include attestation chain
    python3 resolve_quotes.py --validate               # Validate fixtures against schema
    python3 resolve_quotes.py --fixtures path/to/dir   # Custom fixtures directory
    python3 resolve_quotes.py --chain chain.json       # Build full attestation chain

Zero external dependencies. Pure Python stdlib.
"""

import json
import hashlib
import os
import sys
import argparse
import statistics
from datetime import datetime, timezone
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")
POLICY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resolution_policy.json")
SCHEMA_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "quote_schema.json")

DEFAULT_POLICY = {
    "policy_version": "1.0.0",
    "source_hierarchy": {
        "sources": [],
        "selection_rule": "first_valid",
        "top_n": 1
    },
    "staleness": {
        "max_staleness_seconds": 7200,
        "stale_action": "fallback_source"
    },
    "tolerance": {
        "max_deviation_percent": 2.0,
        "deviation_action": "dispute"
    },
    "interpolation": {
        "enabled": True,
        "method": "linear",
        "max_gap_seconds": 14400,
        "confidence_penalty": 0.5
    },
    "dispute": {
        "fallback_chain": ["next_source", "interpolate", "mark_unresolved"],
        "unresolved_action": "skip_signal"
    }
}

RESOLUTION_STATUSES = {"RESOLVED", "RESOLVED_DISPUTE", "UNRESOLVED", "CONTESTED"}
RESOLUTION_METHODS = {
    "first_valid", "fallback_source", "interpolated", "rejected",
    "dispute_median", "median_of_top_n", "weighted_average",
    "mark_unresolved", "use_last_known"
}

FEED_VERSION = "1.0.0"

SOURCE_TIER_MAP = {
    "exchange_direct": "tier_1_exchange",
    "aggregator": "tier_2_aggregator",
    "api_provider": "tier_3_api",
    "manual": "tier_4_manual",
    "interpolated": "tier_5_interpolated"
}

# 24h attribution path: the canonical window for signal outcome resolution
ATTRIBUTION_WINDOW_HOURS = 24
ATTRIBUTION_WINDOW_SECONDS = ATTRIBUTION_WINDOW_HOURS * 3600

# ---------------------------------------------------------------------------
# Utility: ISO-8601 timestamp parsing
# ---------------------------------------------------------------------------

def parse_timestamp(ts_str):
    """Parse ISO-8601 timestamp string to datetime (UTC)."""
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)


def timestamp_diff_seconds(ts1_str, ts2_str):
    """Absolute difference in seconds between two ISO-8601 timestamps."""
    t1 = parse_timestamp(ts1_str)
    t2 = parse_timestamp(ts2_str)
    return abs((t1 - t2).total_seconds())


# ---------------------------------------------------------------------------
# Attestation: SHA-256 hash chain
# ---------------------------------------------------------------------------

def derive_source_tier(source_type):
    """Map source_type to source_tier label."""
    return SOURCE_TIER_MAP.get(source_type, "tier_3_api")


def enrich_quote(quote):
    """Add attestation_hash, source_tier, feed_version to a quote if missing."""
    if "attestation_hash" not in quote:
        quote["attestation_hash"] = compute_quote_hash(
            quote["quote_id"], quote["symbol"], quote["timestamp"],
            quote["source"]["source_id"], quote["price"],
            quote.get("confidence", 1.0)
        )
    if "source_tier" not in quote:
        quote["source_tier"] = derive_source_tier(quote["source"]["source_type"])
    if "feed_version" not in quote:
        quote["feed_version"] = FEED_VERSION
    return quote


def compute_quote_hash(quote_id, symbol, timestamp, source_id, price, confidence):
    """Compute SHA-256 hash of canonical quote fields.

    Canonical string: quote_id|symbol|timestamp|source_id|price|confidence
    Price formatted to 2 decimal places, confidence to 6 decimal places.
    """
    canonical = "|".join([
        str(quote_id),
        str(symbol),
        str(timestamp),
        str(source_id),
        f"{float(price):.2f}",
        f"{float(confidence):.6f}"
    ])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_attestation(quote_id, symbol, timestamp, source_id, price,
                      confidence, previous_hash, sequence_number, chain_id="default"):
    """Build an attestation record linking this quote to the previous."""
    current_hash = compute_quote_hash(
        quote_id, symbol, timestamp, source_id, price, confidence
    )
    return {
        "hash": current_hash,
        "previous_hash": previous_hash,
        "sequence_number": sequence_number,
        "chain_id": chain_id
    }


def verify_attestation_chain(chain):
    """Verify a list of attestation records forms a valid chain.

    Returns (is_valid, error_message_or_none).
    """
    if not chain:
        return True, None

    # First record must have genesis previous_hash (64 zeros)
    if chain[0].get("previous_hash") != "0" * 64:
        return False, f"First record previous_hash is not genesis (64 zeros)"

    for i in range(1, len(chain)):
        expected_prev = chain[i - 1]["hash"]
        actual_prev = chain[i].get("previous_hash")
        if actual_prev != expected_prev:
            return False, (
                f"Chain break at sequence {chain[i].get('sequence_number')}: "
                f"expected previous_hash={expected_prev[:16]}..., "
                f"got {str(actual_prev)[:16]}..."
            )

        # Sequence numbers must be monotonically increasing
        if chain[i]["sequence_number"] <= chain[i - 1]["sequence_number"]:
            return False, (
                f"Non-monotonic sequence at index {i}: "
                f"{chain[i]['sequence_number']} <= {chain[i-1]['sequence_number']}"
            )

    return True, None


# ---------------------------------------------------------------------------
# Core Resolution Engine
# ---------------------------------------------------------------------------

class QuoteResolver:
    """Deterministic price resolution engine.

    Applies the canonical resolution policy to a set of available quotes
    and a requested (symbol, timestamp) pair. Produces a single resolved
    price with full audit trail.
    """

    def __init__(self, policy=None):
        """Initialize with a resolution policy dict.

        If None, loads DEFAULT_POLICY.
        """
        self.policy = policy or dict(DEFAULT_POLICY)

    def _get_staleness(self, quote, requested_timestamp):
        """Compute staleness in seconds between quote and requested timestamp."""
        return timestamp_diff_seconds(quote["timestamp"], requested_timestamp)

    def _is_fresh(self, quote, requested_timestamp, max_staleness=None):
        """Check if a quote is within the staleness window."""
        if max_staleness is None:
            max_staleness = self.policy["staleness"]["max_staleness_seconds"]

        # Per-source override
        source = quote.get("source", {})
        source_max = source.get("max_staleness_seconds")
        if source_max is not None:
            max_staleness = source_max

        staleness = self._get_staleness(quote, requested_timestamp)
        return staleness <= max_staleness

    def _sort_by_priority(self, quotes):
        """Sort quotes by source priority (lower = higher precedence)."""
        return sorted(quotes, key=lambda q: q.get("source", {}).get("priority", 999))

    def _filter_fresh(self, quotes, requested_timestamp, max_staleness=None):
        """Return only quotes within staleness window, sorted by priority."""
        fresh = [q for q in quotes if self._is_fresh(q, requested_timestamp, max_staleness)]
        return self._sort_by_priority(fresh)

    def _compute_deviation(self, prices):
        """Compute max percentage deviation across a set of prices."""
        if len(prices) < 2:
            return 0.0
        min_p = min(prices)
        max_p = max(prices)
        if min_p == 0:
            return float("inf")
        return ((max_p - min_p) / min_p) * 100.0

    def _try_interpolate(self, quotes, requested_timestamp):
        """Attempt linear or nearest interpolation between neighboring quotes.

        Returns (price, confidence, source_id, reason) or None.
        """
        interp_cfg = self.policy.get("interpolation", {})
        if not interp_cfg.get("enabled", True):
            return None

        method = interp_cfg.get("method", "linear")
        max_gap = interp_cfg.get("max_gap_seconds", 14400)
        penalty = interp_cfg.get("confidence_penalty", 0.5)
        req_ts = parse_timestamp(requested_timestamp)

        # Group by source
        by_source = {}
        for q in quotes:
            sid = q["source"]["source_id"]
            if sid not in by_source:
                by_source[sid] = []
            by_source[sid].append(q)

        # Try each source group
        for sid, src_quotes in sorted(by_source.items(),
                                       key=lambda x: min(q["source"].get("priority", 999) for q in x[1])):
            # Sort by timestamp
            src_quotes.sort(key=lambda q: parse_timestamp(q["timestamp"]))

            # Find bracketing pair
            before = None
            after = None
            for q in src_quotes:
                qt = parse_timestamp(q["timestamp"])
                if qt <= req_ts:
                    before = q
                elif qt > req_ts and after is None:
                    after = q

            if before is not None and after is not None:
                t_before = parse_timestamp(before["timestamp"])
                t_after = parse_timestamp(after["timestamp"])
                gap = (t_after - t_before).total_seconds()

                if gap > max_gap:
                    continue  # Gap too large for this source

                if method == "linear":
                    # Time-weighted linear interpolation
                    elapsed = (req_ts - t_before).total_seconds()
                    ratio = elapsed / gap if gap > 0 else 0.5
                    price = before["price"] + ratio * (after["price"] - before["price"])
                elif method == "nearest":
                    # Nearest neighbor
                    d_before = (req_ts - t_before).total_seconds()
                    d_after = (t_after - req_ts).total_seconds()
                    price = before["price"] if d_before <= d_after else after["price"]
                else:
                    continue

                base_confidence = min(before.get("confidence", 1.0),
                                      after.get("confidence", 1.0))
                confidence = base_confidence * penalty

                reason = (
                    f"{'Linear' if method == 'linear' else 'Nearest'} interpolation "
                    f"between {sid} observations at "
                    f"{before['timestamp'].split('T')[1].replace('Z','')} ({before['price']}) and "
                    f"{after['timestamp'].split('T')[1].replace('Z','')} ({after['price']}). "
                    f"Gap={int(gap)}s within {max_gap}s limit. "
                    f"Confidence={base_confidence}*{penalty}={confidence}."
                )

                return {
                    "price": round(price, 2),
                    "confidence": round(confidence, 6),
                    "source_id": sid,
                    "staleness_seconds": 0,
                    "reason": reason
                }

        return None

    def resolve(self, requested_symbol, requested_timestamp, available_quotes,
                policy_overrides=None):
        """Resolve a (symbol, timestamp) pair to a single canonical price.

        Args:
            requested_symbol: Asset symbol (e.g. "BTC")
            requested_timestamp: ISO-8601 UTC timestamp
            available_quotes: List of quote dicts
            policy_overrides: Optional dict to override policy sections

        Returns:
            Resolution result dict with keys:
            - resolved_price, resolved_source_id, resolution_method
            - staleness_seconds, confidence, interpolated
            - status, dispute, reason
            - deviation_percent (if applicable)
            - attestation (if requested)
        """
        # Apply overrides
        policy = dict(self.policy)
        if policy_overrides:
            for section, overrides in policy_overrides.items():
                if section in policy and isinstance(policy[section], dict):
                    policy[section] = {**policy[section], **overrides}
                else:
                    policy[section] = overrides
        self.policy = policy

        # Filter to requested symbol
        symbol_quotes = [q for q in available_quotes
                         if q.get("symbol") == requested_symbol]
        if not symbol_quotes:
            return self._unresolved(f"No quotes available for {requested_symbol}")

        staleness_cfg = policy["staleness"]
        tolerance_cfg = policy["tolerance"]
        hierarchy_cfg = policy.get("source_hierarchy", {})
        selection_rule = hierarchy_cfg.get("selection_rule", "first_valid")
        top_n = hierarchy_cfg.get("top_n", 1)

        # Step 1: Filter fresh quotes
        fresh = self._filter_fresh(symbol_quotes, requested_timestamp)

        # --- Selection rule: median_of_top_n ---
        if selection_rule == "median_of_top_n" and len(fresh) >= top_n:
            top_quotes = fresh[:top_n]
            prices = [q["price"] for q in top_quotes]
            median_price = round(statistics.median(prices), 2)

            # Find the quote closest to median for staleness/confidence
            closest = min(top_quotes, key=lambda q: abs(q["price"] - median_price))
            source_ids = [q["source"]["source_id"] for q in top_quotes]

            return {
                "resolved_price": median_price,
                "resolved_source_id": f"median_of_{len(top_quotes)}",
                "resolution_method": "median_of_top_n",
                "staleness_seconds": self._get_staleness(closest, requested_timestamp),
                "confidence": closest.get("confidence", 1.0),
                "interpolated": False,
                "status": "RESOLVED",
                "dispute": False,
                "reason": (
                    f"Median of top {len(top_quotes)} sources: "
                    f"{[round(p, 2) for p in sorted(prices)]} = {median_price}. "
                    f"Outlier {'eliminated' if len(set(prices)) > 1 else 'none'} by median."
                )
            }

        # --- Selection rule: weighted_average ---
        if selection_rule == "weighted_average" and len(fresh) >= top_n:
            top_quotes = fresh[:top_n]
            total_conf = sum(q.get("confidence", 1.0) for q in top_quotes)
            if total_conf == 0:
                total_conf = 1.0
            wavg = sum(q["price"] * q.get("confidence", 1.0) for q in top_quotes) / total_conf
            wavg = round(wavg, 2)

            return {
                "resolved_price": wavg,
                "resolved_source_id": f"weighted_avg_of_{len(top_quotes)}",
                "resolution_method": "weighted_average",
                "staleness_seconds": min(self._get_staleness(q, requested_timestamp) for q in top_quotes),
                "confidence": round(total_conf / len(top_quotes), 6),
                "interpolated": False,
                "status": "RESOLVED",
                "dispute": False,
                "reason": f"Weighted average of {len(top_quotes)} sources = {wavg}"
            }

        # --- Selection rule: first_valid (default) ---

        # Check tolerance if multiple fresh sources
        if len(fresh) >= 2:
            prices = [q["price"] for q in fresh]
            deviation = self._compute_deviation(prices)
            max_dev = tolerance_cfg.get("max_deviation_percent", 2.0)

            if deviation > max_dev:
                # Tolerance exceeded — handle based on deviation_action
                dev_action = tolerance_cfg.get("deviation_action", "dispute")

                if dev_action == "use_primary":
                    primary = fresh[0]
                    return {
                        "resolved_price": primary["price"],
                        "resolved_source_id": primary["source"]["source_id"],
                        "resolution_method": "first_valid",
                        "staleness_seconds": self._get_staleness(primary, requested_timestamp),
                        "confidence": primary.get("confidence", 1.0),
                        "interpolated": False,
                        "status": "RESOLVED",
                        "dispute": True,
                        "deviation_percent": round(deviation, 3),
                        "reason": (
                            f"Sources deviate {round(deviation, 3)}% (> {max_dev}% tolerance). "
                            f"deviation_action=use_primary. Using {primary['source']['source_id']}."
                        )
                    }

                if dev_action == "use_median":
                    median_price = round(statistics.median(prices), 2)
                    closest = min(fresh, key=lambda q: abs(q["price"] - median_price))
                    return {
                        "resolved_price": median_price,
                        "resolved_source_id": f"median_of_{len(fresh)}",
                        "resolution_method": "dispute_median",
                        "staleness_seconds": self._get_staleness(closest, requested_timestamp),
                        "confidence": closest.get("confidence", 1.0),
                        "interpolated": False,
                        "status": "RESOLVED_DISPUTE",
                        "dispute": True,
                        "deviation_percent": round(deviation, 3),
                        "reason": (
                            f"Sources deviate {round(deviation, 3)}% (> {max_dev}% tolerance). "
                            f"deviation_action=use_median. "
                            f"Median of {len(fresh)} sources "
                            f"{[round(p, 2) for p in sorted(prices)]} = {median_price}."
                        )
                    }

                if dev_action == "dispute":
                    # Enter dispute fallback chain
                    return self._run_dispute_chain(
                        symbol_quotes, fresh, requested_timestamp, deviation, max_dev
                    )

        # Single fresh source or within tolerance — use primary
        if fresh:
            primary = fresh[0]
            deviation = 0.0
            if len(fresh) >= 2:
                prices = [q["price"] for q in fresh]
                deviation = self._compute_deviation(prices)

            # Detect if primary was actually a fallback (higher-priority source was stale)
            all_sorted = self._sort_by_priority(symbol_quotes)
            best_priority = all_sorted[0].get("source", {}).get("priority", 999) if all_sorted else 999
            selected_priority = primary.get("source", {}).get("priority", 999)
            is_fallback = selected_priority > best_priority

            method = "fallback_source" if is_fallback else "first_valid"

            if is_fallback:
                stale_source = all_sorted[0]["source"]["source_id"]
                stale_secs = int(self._get_staleness(all_sorted[0], requested_timestamp))
                reason = (
                    f"Primary source {stale_source} stale "
                    f"({stale_secs}s > {staleness_cfg['max_staleness_seconds']}s). "
                    f"Fallback to {primary['source']['source_id']} "
                    f"({int(self._get_staleness(primary, requested_timestamp))}s staleness, within window)."
                )
            else:
                reason = (
                    f"Primary source {primary['source']['source_id']} fresh within "
                    f"{staleness_cfg['max_staleness_seconds']}s staleness window"
                )

            result = {
                "resolved_price": primary["price"],
                "resolved_source_id": primary["source"]["source_id"],
                "resolution_method": method,
                "staleness_seconds": self._get_staleness(primary, requested_timestamp),
                "confidence": primary.get("confidence", 1.0),
                "interpolated": False,
                "status": "RESOLVED",
                "dispute": False,
                "reason": reason
            }
            if deviation > 0:
                result["deviation_percent"] = round(deviation, 3)
                result["reason"] = (
                    f"Two sources deviate {round(deviation, 3)}% "
                    f"(< {tolerance_cfg.get('max_deviation_percent', 2.0)}% tolerance). "
                    f"Primary source {primary['source']['source_id']} selected."
                )
            return result

        # No fresh sources — apply stale_action
        stale_action = staleness_cfg.get("stale_action", "fallback_source")

        if stale_action == "reject":
            sorted_quotes = self._sort_by_priority(symbol_quotes)
            stale_details = []
            for q in sorted_quotes:
                s = self._get_staleness(q, requested_timestamp)
                stale_details.append(
                    f"{q['source']['source_id']}: {int(s)}s > "
                    f"{staleness_cfg['max_staleness_seconds']}s"
                )
            return self._unresolved(
                f"All sources stale. stale_action=reject. {'. '.join(stale_details)}."
            )

        if stale_action == "fallback_source":
            # Try cascading through sources by priority
            sorted_quotes = self._sort_by_priority(symbol_quotes)
            for q in sorted_quotes:
                staleness = self._get_staleness(q, requested_timestamp)
                if staleness <= staleness_cfg["max_staleness_seconds"]:
                    return {
                        "resolved_price": q["price"],
                        "resolved_source_id": q["source"]["source_id"],
                        "resolution_method": "fallback_source",
                        "staleness_seconds": staleness,
                        "confidence": q.get("confidence", 1.0),
                        "interpolated": False,
                        "status": "RESOLVED",
                        "dispute": False,
                        "reason": (
                            f"Primary source stale. Fallback to "
                            f"{q['source']['source_id']} "
                            f"({int(staleness)}s staleness, within window)."
                        )
                    }
            # All stale — try interpolation
            interp = self._try_interpolate(symbol_quotes, requested_timestamp)
            if interp:
                return {
                    "resolved_price": interp["price"],
                    "resolved_source_id": interp["source_id"],
                    "resolution_method": "interpolated",
                    "staleness_seconds": interp["staleness_seconds"],
                    "confidence": interp["confidence"],
                    "interpolated": True,
                    "status": "RESOLVED",
                    "dispute": False,
                    "reason": interp["reason"]
                }
            return self._unresolved("All sources stale. Fallback chain exhausted.")

        if stale_action == "interpolate":
            interp = self._try_interpolate(symbol_quotes, requested_timestamp)
            if interp:
                return {
                    "resolved_price": interp["price"],
                    "resolved_source_id": interp["source_id"],
                    "resolution_method": "interpolated",
                    "staleness_seconds": interp["staleness_seconds"],
                    "confidence": interp["confidence"],
                    "interpolated": True,
                    "status": "RESOLVED",
                    "dispute": False,
                    "reason": interp["reason"]
                }
            # Interpolation failed — run dispute fallback
            return self._run_dispute_chain(
                symbol_quotes, [], requested_timestamp, 0.0, 0.0
            )

        return self._unresolved(f"Unknown stale_action: {stale_action}")

    def _run_dispute_chain(self, all_quotes, fresh_quotes, requested_timestamp,
                           deviation, max_deviation):
        """Execute the dispute fallback chain."""
        dispute_cfg = self.policy.get("dispute", {})
        chain = dispute_cfg.get("fallback_chain",
                                ["next_source", "interpolate", "mark_unresolved"])
        unresolved_action = dispute_cfg.get("unresolved_action", "skip_signal")

        prices_for_median = [q["price"] for q in fresh_quotes] if fresh_quotes else [q["price"] for q in all_quotes]

        for step in chain:
            if step == "next_source":
                # Try next fresh source after primary
                if len(fresh_quotes) >= 2:
                    secondary = fresh_quotes[1]
                    return {
                        "resolved_price": secondary["price"],
                        "resolved_source_id": secondary["source"]["source_id"],
                        "resolution_method": "fallback_source",
                        "staleness_seconds": self._get_staleness(secondary, requested_timestamp),
                        "confidence": secondary.get("confidence", 1.0),
                        "interpolated": False,
                        "status": "RESOLVED_DISPUTE",
                        "dispute": True,
                        "deviation_percent": round(deviation, 3),
                        "reason": (
                            f"Sources deviate {round(deviation, 3)}% "
                            f"(> {max_deviation}% tolerance). "
                            f"Fallback: next_source = {secondary['source']['source_id']}."
                        )
                    }

            elif step == "interpolate":
                interp = self._try_interpolate(all_quotes, requested_timestamp)
                if interp:
                    return {
                        "resolved_price": interp["price"],
                        "resolved_source_id": interp["source_id"],
                        "resolution_method": "interpolated",
                        "staleness_seconds": interp["staleness_seconds"],
                        "confidence": interp["confidence"],
                        "interpolated": True,
                        "status": "RESOLVED_DISPUTE" if deviation > 0 else "RESOLVED",
                        "dispute": deviation > 0,
                        "reason": interp["reason"]
                    }

            elif step == "use_median":
                if len(prices_for_median) >= 2:
                    median_price = round(statistics.median(prices_for_median), 2)
                    source_quotes = fresh_quotes or all_quotes
                    closest = min(source_quotes,
                                  key=lambda q: abs(q["price"] - median_price))
                    return {
                        "resolved_price": median_price,
                        "resolved_source_id": f"median_of_{len(prices_for_median)}",
                        "resolution_method": "dispute_median",
                        "staleness_seconds": self._get_staleness(closest, requested_timestamp),
                        "confidence": closest.get("confidence", 1.0),
                        "interpolated": False,
                        "status": "RESOLVED_DISPUTE",
                        "dispute": True,
                        "deviation_percent": round(deviation, 3) if deviation > 0 else None,
                        "reason": (
                            f"Sources deviate {round(deviation, 3)}% "
                            f"(> {max_deviation}% tolerance). "
                            f"Fallback: use_median of {len(prices_for_median)} sources "
                            f"{[round(p, 2) for p in sorted(prices_for_median)]} = "
                            f"{median_price}."
                        )
                    }

            elif step == "mark_unresolved":
                stale_details = []
                for q in all_quotes:
                    s = self._get_staleness(q, requested_timestamp)
                    stale_details.append(f"{q['source']['source_id']}: {int(s)}s")

                interp_cfg = self.policy.get("interpolation", {})
                max_gap = interp_cfg.get("max_gap_seconds", 14400)

                # Compute actual gap if applicable
                gap_info = ""
                timestamps = sorted([parse_timestamp(q["timestamp"]) for q in all_quotes])
                if len(timestamps) >= 2:
                    actual_gap = int((timestamps[-1] - timestamps[0]).total_seconds())
                    gap_info = f"Interpolation gap {actual_gap}s exceeds max {max_gap}s. "

                return self._unresolved(
                    f"All sources stale. {gap_info}"
                    f"Fallback chain exhausted to mark_unresolved."
                )

        return self._unresolved("Dispute fallback chain empty or unrecognized steps.")

    def _unresolved(self, reason):
        """Return a standard UNRESOLVED result."""
        return {
            "resolved_price": None,
            "resolved_source_id": None,
            "resolution_method": "mark_unresolved" if "mark_unresolved" in reason.lower() or "exhausted" in reason.lower() else "rejected",
            "staleness_seconds": None,
            "confidence": 0.0,
            "interpolated": False,
            "status": "UNRESOLVED",
            "dispute": False,
            "reason": reason
        }


# ---------------------------------------------------------------------------
# Fixture Replay
# ---------------------------------------------------------------------------

def load_fixtures(fixtures_dir=None):
    """Load all fixture JSON files from the fixtures directory."""
    d = fixtures_dir or FIXTURES_DIR
    fixtures = []
    if not os.path.isdir(d):
        return fixtures
    for fname in sorted(os.listdir(d)):
        if fname.endswith(".json"):
            path = os.path.join(d, fname)
            with open(path, "r") as f:
                fixtures.append(json.load(f))
    return fixtures


def replay_fixture(fixture, resolver=None):
    """Replay a single fixture and compare actual vs expected.

    Returns dict with case_id, expected, actual, match, mismatches.
    """
    if resolver is None:
        resolver = QuoteResolver()

    inp = fixture["input"]
    expected = fixture["expected"]
    overrides = fixture.get("policy_overrides", {})

    actual = resolver.resolve(
        requested_symbol=inp["requested_symbol"],
        requested_timestamp=inp["requested_timestamp"],
        available_quotes=inp["available_quotes"],
        policy_overrides=overrides
    )

    # Compare key fields
    mismatches = []
    compare_fields = ["resolved_price", "resolved_source_id", "resolution_method",
                      "status", "dispute", "interpolated", "confidence"]

    for field in compare_fields:
        exp_val = expected.get(field)
        act_val = actual.get(field)
        if exp_val is not None and act_val is not None:
            # Float comparison with tolerance
            if isinstance(exp_val, float) and isinstance(act_val, float):
                if abs(exp_val - act_val) > 0.01:
                    mismatches.append({
                        "field": field,
                        "expected": exp_val,
                        "actual": act_val
                    })
            elif exp_val != act_val:
                mismatches.append({
                    "field": field,
                    "expected": exp_val,
                    "actual": act_val
                })

    return {
        "case_id": fixture["case_id"],
        "title": fixture.get("title", ""),
        "expected_action": expected.get("status"),
        "actual_action": actual.get("status"),
        "match": len(mismatches) == 0,
        "mismatches": mismatches,
        "actual": actual,
        "expected": expected
    }


def run_replay(fixtures_dir=None, case_filter=None):
    """Run full replay suite and produce report.

    Args:
        fixtures_dir: Path to fixtures directory
        case_filter: Optional case_id to run single fixture

    Returns:
        Report dict with summary and per-case results.
    """
    fixtures = load_fixtures(fixtures_dir)
    if case_filter:
        fixtures = [f for f in fixtures if f["case_id"] == case_filter]

    resolver = QuoteResolver()
    results = []
    for fixture in fixtures:
        result = replay_fixture(fixture, resolver)
        results.append(result)

    matches = sum(1 for r in results if r["match"])
    total = len(results)

    return {
        "summary": {
            "total": total,
            "matches": matches,
            "mismatches": total - matches,
            "pass_rate": f"{matches}/{total}",
            "all_pass": matches == total
        },
        "cases": results
    }


# ---------------------------------------------------------------------------
# Attestation Chain Builder
# ---------------------------------------------------------------------------

def build_attestation_chain(quotes, chain_id="default"):
    """Build a hash-chained attestation across a sequence of quotes.

    Args:
        quotes: List of quote dicts (will be sorted by timestamp)
        chain_id: Identifier for this attestation chain

    Returns:
        List of quotes with attestation field populated.
    """
    sorted_quotes = sorted(quotes, key=lambda q: parse_timestamp(q["timestamp"]))
    previous_hash = "0" * 64  # Genesis
    chain = []

    for i, q in enumerate(sorted_quotes):
        attestation = build_attestation(
            quote_id=q["quote_id"],
            symbol=q["symbol"],
            timestamp=q["timestamp"],
            source_id=q["source"]["source_id"],
            price=q["price"],
            confidence=q.get("confidence", 1.0),
            previous_hash=previous_hash,
            sequence_number=i,
            chain_id=chain_id
        )
        attested_quote = dict(q)
        attested_quote["attestation"] = attestation
        chain.append(attested_quote)
        previous_hash = attestation["hash"]

    return chain


def generate_attestation_example():
    """Generate the canonical 2-record attestation chain example.

    Returns a dict with two consecutive BTC quotes linked by SHA-256 hashes.
    """
    q1 = {
        "quote_id": "attest-001",
        "symbol": "BTC",
        "timestamp": "2026-04-01T12:00:00Z",
        "source": {"source_id": "binance_spot", "source_type": "exchange_direct", "priority": 0},
        "price": 84250.50,
        "confidence": 1.0,
        "staleness_seconds": 0,
        "interpolated": False,
        "interpolation_method": "none",
        "source_tier": "tier_1_exchange",
        "feed_version": FEED_VERSION
    }
    q2 = {
        "quote_id": "attest-002",
        "symbol": "BTC",
        "timestamp": "2026-04-01T12:15:00Z",
        "source": {"source_id": "binance_spot", "source_type": "exchange_direct", "priority": 0},
        "price": 84310.00,
        "confidence": 1.0,
        "staleness_seconds": 0,
        "interpolated": False,
        "interpolation_method": "none",
        "source_tier": "tier_1_exchange",
        "feed_version": FEED_VERSION
    }
    # Enrich with attestation_hash
    enrich_quote(q1)
    enrich_quote(q2)

    chain = build_attestation_chain([q1, q2], chain_id="btc_binance")

    # Verify
    attestations = [q["attestation"] for q in chain]
    is_valid, error = verify_attestation_chain(attestations)

    return {
        "description": "Two consecutive BTC quotes from binance_spot linked by SHA-256 hash chain",
        "chain_id": "btc_binance",
        "records": chain,
        "chain_valid": is_valid,
        "verification_error": error,
        "hash_algorithm": "SHA-256",
        "canonical_format": "quote_id|symbol|timestamp|source_id|price(2dp)|confidence(6dp)"
    }


# ---------------------------------------------------------------------------
# Schema Validation (lightweight, no deps)
# ---------------------------------------------------------------------------

def validate_quote(quote):
    """Validate a quote dict against quote_schema.json structure.

    Returns (is_valid, list_of_errors).
    Lightweight validation — no jsonschema dependency.
    """
    errors = []
    required = ["quote_id", "symbol", "timestamp", "source", "price", "confidence",
                "attestation_hash", "source_tier", "feed_version"]
    for field in required:
        if field not in quote:
            errors.append(f"Missing required field: {field}")

    if "symbol" in quote:
        import re
        if not re.match(r"^[A-Z][A-Z0-9_]{1,19}$", str(quote["symbol"])):
            errors.append(f"Invalid symbol format: {quote['symbol']}")

    if "price" in quote:
        if not isinstance(quote["price"], (int, float)) or quote["price"] <= 0:
            errors.append(f"Price must be positive number, got: {quote['price']}")

    if "confidence" in quote:
        c = quote["confidence"]
        if not isinstance(c, (int, float)) or c < 0 or c > 1:
            errors.append(f"Confidence must be 0-1, got: {c}")

    if "source" in quote:
        src = quote["source"]
        if not isinstance(src, dict):
            errors.append("Source must be an object")
        else:
            for sf in ["source_id", "source_type"]:
                if sf not in src:
                    errors.append(f"Source missing required field: {sf}")
            valid_types = {"exchange_direct", "aggregator", "api_provider",
                           "manual", "interpolated"}
            if src.get("source_type") not in valid_types:
                errors.append(f"Invalid source_type: {src.get('source_type')}")

    if "attestation_hash" in quote:
        import re
        if not re.match(r"^[a-f0-9]{64}$", str(quote["attestation_hash"])):
            errors.append(f"Invalid attestation_hash format: {quote['attestation_hash']}")

    if "source_tier" in quote:
        valid_tiers = {"tier_1_exchange", "tier_2_aggregator", "tier_3_api",
                       "tier_4_manual", "tier_5_interpolated"}
        if quote["source_tier"] not in valid_tiers:
            errors.append(f"Invalid source_tier: {quote['source_tier']}")

    if "feed_version" in quote:
        import re
        if not re.match(r"^[0-9]+\.[0-9]+\.[0-9]+$", str(quote["feed_version"])):
            errors.append(f"Invalid feed_version format: {quote['feed_version']}")

    if "attestation" in quote and quote["attestation"] is not None:
        att = quote["attestation"]
        for af in ["hash", "previous_hash", "sequence_number"]:
            if af not in att:
                errors.append(f"Attestation missing required field: {af}")
        if "hash" in att:
            import re
            if not re.match(r"^[a-f0-9]{64}$", str(att["hash"])):
                errors.append(f"Invalid attestation hash format")
        if "previous_hash" in att:
            import re
            if not re.match(r"^(0{64}|[a-f0-9]{64})$", str(att["previous_hash"])):
                errors.append(f"Invalid attestation previous_hash format")

    return len(errors) == 0, errors


def validate_fixtures(fixtures_dir=None):
    """Validate all fixture files for structural correctness.

    Returns (all_valid, list_of_per_fixture_results).
    """
    fixtures = load_fixtures(fixtures_dir)
    results = []
    all_valid = True

    for fix in fixtures:
        errors = []
        if "case_id" not in fix:
            errors.append("Missing case_id")
        if "input" not in fix:
            errors.append("Missing input")
        if "expected" not in fix:
            errors.append("Missing expected")

        inp = fix.get("input", {})
        if "requested_symbol" not in inp:
            errors.append("Missing input.requested_symbol")
        if "requested_timestamp" not in inp:
            errors.append("Missing input.requested_timestamp")

        quotes = inp.get("available_quotes", [])
        for i, q in enumerate(quotes):
            valid, q_errors = validate_quote(q)
            if not q_errors:
                continue
            for e in q_errors:
                errors.append(f"Quote[{i}]: {e}")

        is_valid = len(errors) == 0
        if not is_valid:
            all_valid = False

        results.append({
            "case_id": fix.get("case_id", "UNKNOWN"),
            "valid": is_valid,
            "errors": errors
        })

    return all_valid, results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Post Fiat Canonical Price Feed Resolver"
    )
    parser.add_argument("--case", type=str, default=None,
                        help="Run single fixture by case_id")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON")
    parser.add_argument("-o", "--output", type=str, default=None,
                        help="Write output to file")
    parser.add_argument("--fixtures", type=str, default=None,
                        help="Custom fixtures directory")
    parser.add_argument("--validate", action="store_true",
                        help="Validate fixtures against schema")
    parser.add_argument("--attest", action="store_true",
                        help="Include attestation chain in output")
    parser.add_argument("--chain", type=str, default=None,
                        help="Build and save attestation chain example")

    args = parser.parse_args()

    # --- Validation mode ---
    if args.validate:
        all_valid, results = validate_fixtures(args.fixtures)
        if args.json:
            output = {"all_valid": all_valid, "fixtures": results}
            text = json.dumps(output, indent=2)
        else:
            lines = [f"Fixture Validation: {'PASS' if all_valid else 'FAIL'}"]
            for r in results:
                status = "VALID" if r["valid"] else "INVALID"
                lines.append(f"  {r['case_id']}: {status}")
                for e in r.get("errors", []):
                    lines.append(f"    - {e}")
            text = "\n".join(lines)

        if args.output:
            with open(args.output, "w") as f:
                f.write(text + "\n")
            print(f"Validation written to {args.output}")
        else:
            print(text)
        sys.exit(0 if all_valid else 1)

    # --- Attestation chain mode ---
    if args.chain:
        example = generate_attestation_example()
        text = json.dumps(example, indent=2)
        with open(args.chain, "w") as f:
            f.write(text + "\n")
        print(f"Attestation chain written to {args.chain}")
        sys.exit(0)

    # --- Replay mode (default) ---
    report = run_replay(args.fixtures, args.case)

    if args.attest:
        example = generate_attestation_example()
        report["attestation_example"] = example

    if args.json:
        text = json.dumps(report, indent=2)
    else:
        lines = []
        summary = report["summary"]
        lines.append(f"=== Price Feed Interop Replay ===")
        lines.append(f"Cases: {summary['pass_rate']} MATCH")
        lines.append("")

        for case in report["cases"]:
            status = "MATCH" if case["match"] else "MISMATCH"
            lines.append(f"  [{status}] {case['case_id']}: {case['title']}")
            lines.append(f"    Expected: {case['expected_action']} | "
                         f"Actual: {case['actual_action']}")
            if case["mismatches"]:
                for m in case["mismatches"]:
                    lines.append(f"    MISMATCH {m['field']}: "
                                 f"expected={m['expected']} actual={m['actual']}")
            lines.append(f"    Reason: {case['actual'].get('reason', 'N/A')}")
            lines.append("")

        if "attestation_example" in report:
            ae = report["attestation_example"]
            lines.append("=== Attestation Chain Example ===")
            lines.append(f"  Chain ID: {ae['chain_id']}")
            lines.append(f"  Records: {len(ae['records'])}")
            lines.append(f"  Valid: {ae['chain_valid']}")
            for r in ae["records"]:
                att = r["attestation"]
                lines.append(f"  seq={att['sequence_number']} "
                             f"hash={att['hash'][:16]}... "
                             f"prev={att['previous_hash'][:16]}...")

        if not summary["all_pass"]:
            lines.append("\nFAILED — mismatches detected")
        else:
            lines.append("\nALL PASS")

        text = "\n".join(lines)

    if args.output:
        with open(args.output, "w") as f:
            f.write(text + "\n")
        print(f"Report written to {args.output}")
    else:
        print(text)

    sys.exit(0 if report["summary"]["all_pass"] else 1)


if __name__ == "__main__":
    main()
