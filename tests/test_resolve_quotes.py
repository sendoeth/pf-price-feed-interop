"""
Comprehensive test suite for Post Fiat Canonical Price Feed Resolver.

Tests cover: timestamp parsing, staleness detection, source selection,
interpolation, tolerance/dispute, attestation chain, fixture replay,
schema validation, edge cases, CLI modes, and cross-fixture consistency.
"""

import json
import hashlib
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from resolve_quotes import (
    parse_timestamp,
    timestamp_diff_seconds,
    compute_quote_hash,
    build_attestation,
    verify_attestation_chain,
    build_attestation_chain,
    generate_attestation_example,
    derive_source_tier,
    enrich_quote,
    QuoteResolver,
    load_fixtures,
    replay_fixture,
    run_replay,
    validate_quote,
    validate_fixtures,
    DEFAULT_POLICY,
    RESOLUTION_STATUSES,
    RESOLUTION_METHODS,
    FEED_VERSION,
    SOURCE_TIER_MAP,
    ATTRIBUTION_WINDOW_HOURS,
    ATTRIBUTION_WINDOW_SECONDS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_quote(quote_id, symbol, timestamp, source_id, source_type, priority,
               price, confidence=1.0, staleness=0):
    q = {
        "quote_id": quote_id,
        "symbol": symbol,
        "timestamp": timestamp,
        "source": {
            "source_id": source_id,
            "source_type": source_type,
            "priority": priority
        },
        "price": price,
        "confidence": confidence,
        "staleness_seconds": staleness,
        "interpolated": False,
        "interpolation_method": "none",
        "source_tier": derive_source_tier(source_type),
        "feed_version": FEED_VERSION,
    }
    q["attestation_hash"] = compute_quote_hash(
        quote_id, symbol, timestamp, source_id, price, confidence
    )
    return q


# ========================================================================
# 1. Timestamp Parsing
# ========================================================================

class TestTimestampParsing(unittest.TestCase):
    """Test ISO-8601 timestamp parsing and diff computation."""

    def test_parse_utc_z_suffix(self):
        dt = parse_timestamp("2026-04-01T12:00:00Z")
        self.assertEqual(dt.year, 2026)
        self.assertEqual(dt.month, 4)
        self.assertEqual(dt.hour, 12)
        self.assertEqual(dt.tzinfo, timezone.utc)

    def test_parse_utc_offset(self):
        dt = parse_timestamp("2026-04-01T12:00:00+00:00")
        self.assertEqual(dt.hour, 12)

    def test_diff_seconds_exact(self):
        diff = timestamp_diff_seconds(
            "2026-04-01T12:00:00Z",
            "2026-04-01T12:00:30Z"
        )
        self.assertEqual(diff, 30.0)

    def test_diff_seconds_reverse_order(self):
        diff = timestamp_diff_seconds(
            "2026-04-01T12:00:30Z",
            "2026-04-01T12:00:00Z"
        )
        self.assertEqual(diff, 30.0)

    def test_diff_seconds_same_timestamp(self):
        diff = timestamp_diff_seconds(
            "2026-04-01T12:00:00Z",
            "2026-04-01T12:00:00Z"
        )
        self.assertEqual(diff, 0.0)

    def test_diff_large_gap(self):
        diff = timestamp_diff_seconds(
            "2026-04-01T00:00:00Z",
            "2026-04-02T00:00:00Z"
        )
        self.assertEqual(diff, 86400.0)


# ========================================================================
# 2. Attestation / Hash Chain
# ========================================================================

class TestComputeQuoteHash(unittest.TestCase):
    """Test SHA-256 hash computation for quote canonical fields."""

    def test_deterministic_hash(self):
        h1 = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84000.0, 1.0)
        h2 = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84000.0, 1.0)
        self.assertEqual(h1, h2)

    def test_hash_length(self):
        h = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84000.0, 1.0)
        self.assertEqual(len(h), 64)

    def test_hash_hex_chars(self):
        h = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84000.0, 1.0)
        import re
        self.assertRegex(h, r"^[a-f0-9]{64}$")

    def test_different_price_different_hash(self):
        h1 = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84000.0, 1.0)
        h2 = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84001.0, 1.0)
        self.assertNotEqual(h1, h2)

    def test_different_confidence_different_hash(self):
        h1 = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84000.0, 1.0)
        h2 = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84000.0, 0.9)
        self.assertNotEqual(h1, h2)

    def test_canonical_format_precision(self):
        # Price to 2dp, confidence to 6dp
        canonical = "|".join(["q1", "BTC", "ts", "src", "84000.00", "1.000000"])
        expected = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        h = compute_quote_hash("q1", "BTC", "ts", "src", 84000.0, 1.0)
        self.assertEqual(h, expected)


class TestBuildAttestation(unittest.TestCase):
    """Test attestation record construction."""

    def test_genesis_record(self):
        att = build_attestation("q1", "BTC", "ts", "src", 84000.0, 1.0,
                                "0" * 64, 0, "chain1")
        self.assertEqual(att["previous_hash"], "0" * 64)
        self.assertEqual(att["sequence_number"], 0)
        self.assertEqual(att["chain_id"], "chain1")
        self.assertEqual(len(att["hash"]), 64)

    def test_chained_record(self):
        att1 = build_attestation("q1", "BTC", "ts1", "src", 84000.0, 1.0,
                                 "0" * 64, 0)
        att2 = build_attestation("q2", "BTC", "ts2", "src", 84100.0, 1.0,
                                 att1["hash"], 1)
        self.assertEqual(att2["previous_hash"], att1["hash"])
        self.assertEqual(att2["sequence_number"], 1)


class TestVerifyAttestationChain(unittest.TestCase):
    """Test attestation chain verification."""

    def test_empty_chain_valid(self):
        valid, err = verify_attestation_chain([])
        self.assertTrue(valid)
        self.assertIsNone(err)

    def test_single_genesis_valid(self):
        chain = [{"hash": "a" * 64, "previous_hash": "0" * 64, "sequence_number": 0}]
        valid, err = verify_attestation_chain(chain)
        self.assertTrue(valid)

    def test_valid_two_record_chain(self):
        chain = [
            {"hash": "a" * 64, "previous_hash": "0" * 64, "sequence_number": 0},
            {"hash": "b" * 64, "previous_hash": "a" * 64, "sequence_number": 1}
        ]
        valid, err = verify_attestation_chain(chain)
        self.assertTrue(valid)

    def test_broken_chain(self):
        chain = [
            {"hash": "a" * 64, "previous_hash": "0" * 64, "sequence_number": 0},
            {"hash": "b" * 64, "previous_hash": "c" * 64, "sequence_number": 1}
        ]
        valid, err = verify_attestation_chain(chain)
        self.assertFalse(valid)
        self.assertIn("Chain break", err)

    def test_non_genesis_first_record(self):
        chain = [{"hash": "a" * 64, "previous_hash": "b" * 64, "sequence_number": 0}]
        valid, err = verify_attestation_chain(chain)
        self.assertFalse(valid)
        self.assertIn("genesis", err)

    def test_non_monotonic_sequence(self):
        chain = [
            {"hash": "a" * 64, "previous_hash": "0" * 64, "sequence_number": 0},
            {"hash": "b" * 64, "previous_hash": "a" * 64, "sequence_number": 0}
        ]
        valid, err = verify_attestation_chain(chain)
        self.assertFalse(valid)
        self.assertIn("Non-monotonic", err)


class TestBuildAttestationChain(unittest.TestCase):
    """Test full chain building from quote list."""

    def test_two_quotes_chain(self):
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "src", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T12:15:00Z", "src", "exchange_direct", 0, 84100.0),
        ]
        chain = build_attestation_chain(quotes, "test_chain")
        self.assertEqual(len(chain), 2)
        self.assertIn("attestation", chain[0])
        self.assertIn("attestation", chain[1])
        self.assertEqual(chain[0]["attestation"]["previous_hash"], "0" * 64)
        self.assertEqual(chain[1]["attestation"]["previous_hash"], chain[0]["attestation"]["hash"])

    def test_chain_sorted_by_timestamp(self):
        # Pass in reverse order — should still chain correctly
        quotes = [
            make_quote("q2", "BTC", "2026-04-01T12:15:00Z", "src", "exchange_direct", 0, 84100.0),
            make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "src", "exchange_direct", 0, 84000.0),
        ]
        chain = build_attestation_chain(quotes)
        self.assertEqual(chain[0]["quote_id"], "q1")
        self.assertEqual(chain[1]["quote_id"], "q2")

    def test_chain_verifies(self):
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "src", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T12:15:00Z", "src", "exchange_direct", 0, 84100.0),
            make_quote("q3", "BTC", "2026-04-01T12:30:00Z", "src", "exchange_direct", 0, 84200.0),
        ]
        chain = build_attestation_chain(quotes)
        attestations = [q["attestation"] for q in chain]
        valid, err = verify_attestation_chain(attestations)
        self.assertTrue(valid)
        self.assertIsNone(err)


class TestGenerateAttestationExample(unittest.TestCase):
    """Test the canonical attestation example generator."""

    def test_example_has_two_records(self):
        ex = generate_attestation_example()
        self.assertEqual(len(ex["records"]), 2)

    def test_example_chain_valid(self):
        ex = generate_attestation_example()
        self.assertTrue(ex["chain_valid"])
        self.assertIsNone(ex["verification_error"])

    def test_example_has_chain_id(self):
        ex = generate_attestation_example()
        self.assertEqual(ex["chain_id"], "btc_binance")

    def test_example_genesis_hash(self):
        ex = generate_attestation_example()
        self.assertEqual(ex["records"][0]["attestation"]["previous_hash"], "0" * 64)

    def test_example_hash_links(self):
        ex = generate_attestation_example()
        r0_hash = ex["records"][0]["attestation"]["hash"]
        r1_prev = ex["records"][1]["attestation"]["previous_hash"]
        self.assertEqual(r0_hash, r1_prev)


# ========================================================================
# 3. QuoteResolver — Staleness
# ========================================================================

class TestStaleness(unittest.TestCase):
    """Test staleness detection and filtering."""

    def setUp(self):
        self.resolver = QuoteResolver()

    def test_fresh_quote(self):
        q = make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "src", "exchange_direct", 0, 84000.0)
        self.assertTrue(self.resolver._is_fresh(q, "2026-04-01T12:00:00Z"))

    def test_stale_quote(self):
        q = make_quote("q1", "BTC", "2026-04-01T08:00:00Z", "src", "exchange_direct", 0, 84000.0)
        self.assertFalse(self.resolver._is_fresh(q, "2026-04-01T12:00:00Z"))

    def test_exact_boundary_fresh(self):
        # Exactly at staleness limit = still fresh
        q = make_quote("q1", "BTC", "2026-04-01T10:00:00Z", "src", "exchange_direct", 0, 84000.0)
        self.assertTrue(self.resolver._is_fresh(q, "2026-04-01T12:00:00Z", max_staleness=7200))

    def test_one_second_over_stale(self):
        q = make_quote("q1", "BTC", "2026-04-01T09:59:59Z", "src", "exchange_direct", 0, 84000.0)
        self.assertFalse(self.resolver._is_fresh(q, "2026-04-01T12:00:00Z", max_staleness=7200))

    def test_per_source_staleness_override(self):
        q = make_quote("q1", "BTC", "2026-04-01T11:50:00Z", "src", "exchange_direct", 0, 84000.0)
        q["source"]["max_staleness_seconds"] = 300  # 5 min override
        self.assertFalse(self.resolver._is_fresh(q, "2026-04-01T12:00:00Z"))

    def test_filter_fresh_returns_only_fresh(self):
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "src1", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T08:00:00Z", "src2", "aggregator", 1, 83000.0),
        ]
        fresh = self.resolver._filter_fresh(quotes, "2026-04-01T12:00:00Z")
        self.assertEqual(len(fresh), 1)
        self.assertEqual(fresh[0]["quote_id"], "q1")


# ========================================================================
# 4. QuoteResolver — Source Priority
# ========================================================================

class TestSourcePriority(unittest.TestCase):
    """Test source priority sorting."""

    def setUp(self):
        self.resolver = QuoteResolver()

    def test_sort_by_priority(self):
        quotes = [
            make_quote("q2", "BTC", "ts", "src2", "aggregator", 2, 84000.0),
            make_quote("q1", "BTC", "ts", "src1", "exchange_direct", 0, 84000.0),
            make_quote("q3", "BTC", "ts", "src3", "api_provider", 1, 84000.0),
        ]
        sorted_q = self.resolver._sort_by_priority(quotes)
        self.assertEqual(sorted_q[0]["source"]["source_id"], "src1")
        self.assertEqual(sorted_q[1]["source"]["source_id"], "src3")
        self.assertEqual(sorted_q[2]["source"]["source_id"], "src2")

    def test_missing_priority_defaults_high(self):
        q_no_priority = make_quote("q1", "BTC", "ts", "src", "exchange_direct", 0, 84000.0)
        del q_no_priority["source"]["priority"]
        q_with = make_quote("q2", "BTC", "ts", "src2", "aggregator", 1, 84000.0)
        sorted_q = self.resolver._sort_by_priority([q_no_priority, q_with])
        # Missing priority defaults to 999, so src2 (priority 1) should be first
        self.assertEqual(sorted_q[0]["source"]["source_id"], "src2")


# ========================================================================
# 5. QuoteResolver — Deviation / Tolerance
# ========================================================================

class TestDeviation(unittest.TestCase):
    """Test price deviation computation."""

    def setUp(self):
        self.resolver = QuoteResolver()

    def test_zero_deviation_same_price(self):
        dev = self.resolver._compute_deviation([100.0, 100.0])
        self.assertEqual(dev, 0.0)

    def test_1_percent_deviation(self):
        dev = self.resolver._compute_deviation([100.0, 101.0])
        self.assertAlmostEqual(dev, 1.0, places=2)

    def test_single_price_zero_deviation(self):
        dev = self.resolver._compute_deviation([100.0])
        self.assertEqual(dev, 0.0)

    def test_empty_prices_zero_deviation(self):
        dev = self.resolver._compute_deviation([])
        self.assertEqual(dev, 0.0)

    def test_large_deviation(self):
        dev = self.resolver._compute_deviation([100.0, 200.0])
        self.assertAlmostEqual(dev, 100.0, places=2)

    def test_multiple_prices_max_range(self):
        dev = self.resolver._compute_deviation([100.0, 101.0, 105.0])
        self.assertAlmostEqual(dev, 5.0, places=2)


# ========================================================================
# 6. QuoteResolver — Resolution Paths
# ========================================================================

class TestResolutionPrimarySource(unittest.TestCase):
    """Test first_valid primary source resolution."""

    def test_single_fresh_source(self):
        resolver = QuoteResolver()
        result = resolver.resolve(
            "BTC", "2026-04-01T12:00:00Z",
            [make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 84000.0)]
        )
        self.assertEqual(result["status"], "RESOLVED")
        self.assertEqual(result["resolved_price"], 84000.0)
        self.assertEqual(result["resolved_source_id"], "binance")
        self.assertEqual(result["resolution_method"], "first_valid")

    def test_no_quotes_unresolved(self):
        resolver = QuoteResolver()
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", [])
        self.assertEqual(result["status"], "UNRESOLVED")
        self.assertIsNone(result["resolved_price"])

    def test_wrong_symbol_unresolved(self):
        resolver = QuoteResolver()
        result = resolver.resolve(
            "BTC", "2026-04-01T12:00:00Z",
            [make_quote("q1", "ETH", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 1800.0)]
        )
        self.assertEqual(result["status"], "UNRESOLVED")


class TestResolutionFallback(unittest.TestCase):
    """Test fallback source resolution when primary is stale."""

    def test_fallback_to_secondary(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T08:00:00Z", "binance", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:00Z", "coingecko", "aggregator", 1, 84100.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "RESOLVED")
        self.assertEqual(result["resolved_source_id"], "coingecko")
        self.assertEqual(result["resolution_method"], "fallback_source")

    def test_all_stale_with_reject(self):
        resolver = QuoteResolver({"staleness": {"max_staleness_seconds": 100, "stale_action": "reject"},
                                   **{k: v for k, v in DEFAULT_POLICY.items() if k != "staleness"}})
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T08:00:00Z", "binance", "exchange_direct", 0, 84000.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "UNRESOLVED")
        self.assertIn("reject", result["reason"].lower())


class TestResolutionInterpolation(unittest.TestCase):
    """Test interpolation resolution path."""

    def test_linear_interpolation_midpoint(self):
        policy = dict(DEFAULT_POLICY)
        policy["staleness"] = {"max_staleness_seconds": 1800, "stale_action": "interpolate"}
        policy["interpolation"] = {"enabled": True, "method": "linear",
                                    "max_gap_seconds": 14400, "confidence_penalty": 0.5}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "LINK", "2026-04-01T11:00:00Z", "cg", "aggregator", 0, 10.0, 1.0),
            make_quote("q2", "LINK", "2026-04-01T13:00:00Z", "cg", "aggregator", 0, 20.0, 1.0),
        ]
        result = resolver.resolve("LINK", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "RESOLVED")
        self.assertTrue(result["interpolated"])
        self.assertAlmostEqual(result["resolved_price"], 15.0, places=2)
        self.assertAlmostEqual(result["confidence"], 0.5, places=2)

    def test_linear_interpolation_quarter_point(self):
        policy = dict(DEFAULT_POLICY)
        policy["staleness"] = {"max_staleness_seconds": 1800, "stale_action": "interpolate"}
        policy["interpolation"] = {"enabled": True, "method": "linear",
                                    "max_gap_seconds": 14400, "confidence_penalty": 0.5}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "LINK", "2026-04-01T10:00:00Z", "cg", "aggregator", 0, 10.0, 1.0),
            make_quote("q2", "LINK", "2026-04-01T14:00:00Z", "cg", "aggregator", 0, 20.0, 1.0),
        ]
        # 12:00 = 2h into a 4h gap = 50% interpolation
        result = resolver.resolve("LINK", "2026-04-01T12:00:00Z", quotes)
        self.assertAlmostEqual(result["resolved_price"], 15.0, places=2)

    def test_nearest_interpolation(self):
        policy = dict(DEFAULT_POLICY)
        policy["staleness"] = {"max_staleness_seconds": 1800, "stale_action": "interpolate"}
        policy["interpolation"] = {"enabled": True, "method": "nearest",
                                    "max_gap_seconds": 14400, "confidence_penalty": 0.5}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "LINK", "2026-04-01T11:00:00Z", "cg", "aggregator", 0, 10.0, 1.0),
            make_quote("q2", "LINK", "2026-04-01T13:00:00Z", "cg", "aggregator", 0, 20.0, 1.0),
        ]
        # 12:00 is equidistant — nearest picks before (tie-break)
        result = resolver.resolve("LINK", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["resolved_price"], 10.0)

    def test_interpolation_gap_too_large(self):
        policy = dict(DEFAULT_POLICY)
        policy["staleness"] = {"max_staleness_seconds": 100, "stale_action": "interpolate"}
        policy["interpolation"] = {"enabled": True, "method": "linear",
                                    "max_gap_seconds": 1800, "confidence_penalty": 0.5}
        policy["dispute"] = {"fallback_chain": ["interpolate", "mark_unresolved"],
                              "unresolved_action": "skip_signal"}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "LINK", "2026-04-01T08:00:00Z", "cg", "aggregator", 0, 10.0, 1.0),
            make_quote("q2", "LINK", "2026-04-01T16:00:00Z", "cg", "aggregator", 0, 20.0, 1.0),
        ]
        result = resolver.resolve("LINK", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "UNRESOLVED")

    def test_interpolation_disabled(self):
        policy = dict(DEFAULT_POLICY)
        policy["staleness"] = {"max_staleness_seconds": 100, "stale_action": "interpolate"}
        policy["interpolation"] = {"enabled": False, "method": "linear",
                                    "max_gap_seconds": 14400, "confidence_penalty": 0.5}
        policy["dispute"] = {"fallback_chain": ["interpolate", "mark_unresolved"],
                              "unresolved_action": "skip_signal"}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "LINK", "2026-04-01T11:00:00Z", "cg", "aggregator", 0, 10.0, 1.0),
            make_quote("q2", "LINK", "2026-04-01T13:00:00Z", "cg", "aggregator", 0, 20.0, 1.0),
        ]
        result = resolver.resolve("LINK", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "UNRESOLVED")


class TestResolutionTolerance(unittest.TestCase):
    """Test tolerance-based dispute detection."""

    def test_within_tolerance(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 84100.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "RESOLVED")
        self.assertFalse(result["dispute"])
        self.assertEqual(result["resolved_source_id"], "binance")

    def test_exceeds_tolerance_dispute_median(self):
        policy = dict(DEFAULT_POLICY)
        policy["tolerance"] = {"max_deviation_percent": 1.0, "deviation_action": "dispute"}
        policy["dispute"] = {"fallback_chain": ["use_median", "mark_unresolved"],
                              "unresolved_action": "skip_signal"}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 86000.0),
            make_quote("q3", "BTC", "2026-04-01T11:58:00Z", "kraken", "exchange_direct", 2, 85000.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertTrue(result["dispute"])
        self.assertEqual(result["resolved_price"], 85000.0)  # median

    def test_exceeds_tolerance_use_primary(self):
        policy = dict(DEFAULT_POLICY)
        policy["tolerance"] = {"max_deviation_percent": 1.0, "deviation_action": "use_primary"}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 90000.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertTrue(result["dispute"])
        self.assertEqual(result["resolved_price"], 84000.0)  # primary wins

    def test_exceeds_tolerance_use_median_action(self):
        policy = dict(DEFAULT_POLICY)
        policy["tolerance"] = {"max_deviation_percent": 1.0, "deviation_action": "use_median"}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 100.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 200.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertTrue(result["dispute"])
        self.assertEqual(result["resolved_price"], 150.0)  # median of 2


class TestResolutionMedianTopN(unittest.TestCase):
    """Test median_of_top_n selection rule."""

    def test_median_of_3(self):
        policy = dict(DEFAULT_POLICY)
        policy["source_hierarchy"] = {"selection_rule": "median_of_top_n", "top_n": 3, "sources": []}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 100.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 200.0),
            make_quote("q3", "BTC", "2026-04-01T11:58:00Z", "kraken", "exchange_direct", 2, 150.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["resolved_price"], 150.0)
        self.assertEqual(result["resolution_method"], "median_of_top_n")

    def test_insufficient_sources_falls_back(self):
        policy = dict(DEFAULT_POLICY)
        policy["source_hierarchy"] = {"selection_rule": "median_of_top_n", "top_n": 3, "sources": []}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 100.0),
        ]
        # Only 1 source, needs 3 — falls back to first_valid
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["resolution_method"], "first_valid")


class TestResolutionWeightedAverage(unittest.TestCase):
    """Test weighted_average selection rule."""

    def test_weighted_average(self):
        policy = dict(DEFAULT_POLICY)
        policy["source_hierarchy"] = {"selection_rule": "weighted_average", "top_n": 2, "sources": []}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 100.0, confidence=1.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 200.0, confidence=1.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["resolution_method"], "weighted_average")
        self.assertAlmostEqual(result["resolved_price"], 150.0, places=2)

    def test_weighted_average_different_confidence(self):
        policy = dict(DEFAULT_POLICY)
        policy["source_hierarchy"] = {"selection_rule": "weighted_average", "top_n": 2, "sources": []}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 100.0, confidence=0.9),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 200.0, confidence=0.1),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        # (100*0.9 + 200*0.1) / (0.9 + 0.1) = (90 + 20) / 1.0 = 110
        self.assertAlmostEqual(result["resolved_price"], 110.0, places=2)


class TestResolutionDisputeChain(unittest.TestCase):
    """Test dispute fallback chain execution."""

    def test_dispute_chain_use_median(self):
        policy = dict(DEFAULT_POLICY)
        policy["tolerance"] = {"max_deviation_percent": 1.0, "deviation_action": "dispute"}
        policy["dispute"] = {"fallback_chain": ["use_median", "mark_unresolved"],
                              "unresolved_action": "skip_signal"}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 100.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 120.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "RESOLVED_DISPUTE")
        self.assertEqual(result["resolved_price"], 110.0)

    def test_dispute_chain_exhausts_to_unresolved(self):
        policy = dict(DEFAULT_POLICY)
        policy["tolerance"] = {"max_deviation_percent": 1.0, "deviation_action": "dispute"}
        policy["dispute"] = {"fallback_chain": ["mark_unresolved"],
                              "unresolved_action": "skip_signal"}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 100.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 200.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "UNRESOLVED")


# ========================================================================
# 7. Policy Overrides
# ========================================================================

class TestPolicyOverrides(unittest.TestCase):
    """Test that fixture-level policy overrides work correctly."""

    def test_staleness_override(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:50:00Z", "binance", "exchange_direct", 0, 84000.0),
        ]
        # Default 7200s — 600s staleness should pass
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "RESOLVED")

        # Override to 300s — 600s staleness should fail
        resolver2 = QuoteResolver()
        result2 = resolver2.resolve("BTC", "2026-04-01T12:00:00Z", quotes,
                                     policy_overrides={"staleness": {"max_staleness_seconds": 300, "stale_action": "reject"}})
        self.assertEqual(result2["status"], "UNRESOLVED")

    def test_tolerance_override(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 100.0),
            make_quote("q2", "BTC", "2026-04-01T11:59:30Z", "coinbase", "exchange_direct", 1, 101.5),
        ]
        # Default 2% — 1.5% should pass
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertFalse(result["dispute"])

        # Override to 1% — 1.5% should trigger dispute
        resolver2 = QuoteResolver()
        result2 = resolver2.resolve("BTC", "2026-04-01T12:00:00Z", quotes,
                                     policy_overrides={
                                         "tolerance": {"max_deviation_percent": 1.0, "deviation_action": "dispute"},
                                         "dispute": {"fallback_chain": ["use_median", "mark_unresolved"]}
                                     })
        self.assertTrue(result2["dispute"])


# ========================================================================
# 8. Fixture Replay
# ========================================================================

class TestFixtureLoading(unittest.TestCase):
    """Test fixture loading from directory."""

    def test_load_existing_fixtures(self):
        fixtures = load_fixtures()
        self.assertGreaterEqual(len(fixtures), 8)

    def test_each_fixture_has_required_fields(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            self.assertIn("case_id", fix)
            self.assertIn("input", fix)
            self.assertIn("expected", fix)
            self.assertIn("requested_symbol", fix["input"])
            self.assertIn("requested_timestamp", fix["input"])
            self.assertIn("available_quotes", fix["input"])

    def test_load_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            fixtures = load_fixtures(tmpdir)
            self.assertEqual(len(fixtures), 0)

    def test_load_nonexistent_directory(self):
        fixtures = load_fixtures("/nonexistent/path")
        self.assertEqual(len(fixtures), 0)


class TestReplayFixture(unittest.TestCase):
    """Test single fixture replay."""

    def test_replay_pfq_001(self):
        fixtures = load_fixtures()
        pfq_001 = [f for f in fixtures if f["case_id"] == "PFQ-001"][0]
        result = replay_fixture(pfq_001)
        self.assertTrue(result["match"])
        self.assertEqual(result["case_id"], "PFQ-001")
        self.assertEqual(len(result["mismatches"]), 0)

    def test_replay_all_fixtures_match(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            result = replay_fixture(fix)
            self.assertTrue(result["match"],
                            f"Fixture {fix['case_id']} failed: {result['mismatches']}")

    def test_replay_returns_actual_and_expected(self):
        fixtures = load_fixtures()
        result = replay_fixture(fixtures[0])
        self.assertIn("actual", result)
        self.assertIn("expected", result)
        self.assertIn("status", result["actual"])


class TestRunReplay(unittest.TestCase):
    """Test full replay suite."""

    def test_all_pass(self):
        report = run_replay()
        self.assertTrue(report["summary"]["all_pass"])
        self.assertEqual(report["summary"]["mismatches"], 0)

    def test_case_filter(self):
        report = run_replay(case_filter="PFQ-003")
        self.assertEqual(report["summary"]["total"], 1)
        self.assertEqual(report["cases"][0]["case_id"], "PFQ-003")

    def test_case_filter_not_found(self):
        report = run_replay(case_filter="NONEXISTENT")
        self.assertEqual(report["summary"]["total"], 0)


# ========================================================================
# 9. Schema Validation
# ========================================================================

class TestValidateQuote(unittest.TestCase):
    """Test lightweight quote validation."""

    def test_valid_quote(self):
        q = make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "binance", "exchange_direct", 0, 84000.0)
        valid, errors = validate_quote(q)
        self.assertTrue(valid)
        self.assertEqual(len(errors), 0)

    def test_missing_required_field(self):
        q = {"quote_id": "q1", "symbol": "BTC"}
        valid, errors = validate_quote(q)
        self.assertFalse(valid)
        self.assertTrue(any("Missing required" in e for e in errors))

    def test_invalid_symbol_format(self):
        q = make_quote("q1", "btc", "ts", "src", "exchange_direct", 0, 84000.0)
        valid, errors = validate_quote(q)
        self.assertFalse(valid)
        self.assertTrue(any("symbol" in e.lower() for e in errors))

    def test_negative_price(self):
        q = make_quote("q1", "BTC", "ts", "src", "exchange_direct", 0, -100.0)
        valid, errors = validate_quote(q)
        self.assertFalse(valid)
        self.assertTrue(any("price" in e.lower() or "Price" in e for e in errors))

    def test_zero_price(self):
        q = make_quote("q1", "BTC", "ts", "src", "exchange_direct", 0, 0.0)
        valid, errors = validate_quote(q)
        self.assertFalse(valid)

    def test_confidence_out_of_range(self):
        q = make_quote("q1", "BTC", "ts", "src", "exchange_direct", 0, 84000.0, confidence=1.5)
        valid, errors = validate_quote(q)
        self.assertFalse(valid)
        self.assertTrue(any("confidence" in e.lower() or "Confidence" in e for e in errors))

    def test_invalid_source_type(self):
        q = make_quote("q1", "BTC", "ts", "src", "unknown_type", 0, 84000.0)
        valid, errors = validate_quote(q)
        self.assertFalse(valid)
        self.assertTrue(any("source_type" in e for e in errors))


class TestValidateFixtures(unittest.TestCase):
    """Test fixture validation."""

    def test_all_fixtures_valid(self):
        all_valid, results = validate_fixtures()
        self.assertTrue(all_valid, f"Invalid fixtures: {[r for r in results if not r['valid']]}")

    def test_validation_empty_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            all_valid, results = validate_fixtures(tmpdir)
            self.assertTrue(all_valid)
            self.assertEqual(len(results), 0)


# ========================================================================
# 10. Cross-Fixture Consistency
# ========================================================================

class TestCrossFixtureConsistency(unittest.TestCase):
    """Test consistency across fixture cases."""

    def test_unique_case_ids(self):
        fixtures = load_fixtures()
        ids = [f["case_id"] for f in fixtures]
        self.assertEqual(len(ids), len(set(ids)))

    def test_unique_quote_ids_within_fixture(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            qids = [q["quote_id"] for q in fix["input"]["available_quotes"]]
            self.assertEqual(len(qids), len(set(qids)),
                             f"Duplicate quote_id in {fix['case_id']}")

    def test_all_expected_statuses_valid(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            status = fix["expected"]["status"]
            self.assertIn(status, RESOLUTION_STATUSES,
                          f"Invalid status in {fix['case_id']}: {status}")

    def test_all_resolution_methods_valid(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            method = fix["expected"]["resolution_method"]
            self.assertIn(method, RESOLUTION_METHODS,
                          f"Invalid method in {fix['case_id']}: {method}")

    def test_unresolved_has_null_price(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            if fix["expected"]["status"] == "UNRESOLVED":
                self.assertIsNone(fix["expected"]["resolved_price"],
                                  f"UNRESOLVED should have null price in {fix['case_id']}")

    def test_resolved_has_non_null_price(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            if fix["expected"]["status"] in ("RESOLVED", "RESOLVED_DISPUTE"):
                self.assertIsNotNone(fix["expected"]["resolved_price"],
                                     f"RESOLVED should have non-null price in {fix['case_id']}")

    def test_confidence_range(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            conf = fix["expected"]["confidence"]
            self.assertGreaterEqual(conf, 0.0, f"Confidence < 0 in {fix['case_id']}")
            self.assertLessEqual(conf, 1.0, f"Confidence > 1 in {fix['case_id']}")

    def test_symbols_are_valid_format(self):
        fixtures = load_fixtures()
        import re
        for fix in fixtures:
            sym = fix["input"]["requested_symbol"]
            self.assertRegex(sym, r"^[A-Z][A-Z0-9_]{1,19}$",
                             f"Invalid symbol in {fix['case_id']}: {sym}")


# ========================================================================
# 11. Edge Cases
# ========================================================================

class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def test_zero_staleness_window(self):
        policy = dict(DEFAULT_POLICY)
        policy["staleness"] = {"max_staleness_seconds": 0, "stale_action": "reject"}
        resolver = QuoteResolver(policy)
        # Only an exact-timestamp match would pass
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "binance", "exchange_direct", 0, 84000.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["status"], "RESOLVED")

    def test_single_quote_no_tolerance_check(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 84000.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertFalse(result["dispute"])

    def test_very_high_price(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 999999999.99),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["resolved_price"], 999999999.99)

    def test_very_small_price(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "PEPE", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 0.00001),
        ]
        result = resolver.resolve("PEPE", "2026-04-01T12:00:00Z", quotes)
        self.assertAlmostEqual(result["resolved_price"], 0.00001, places=5)

    def test_mixed_symbols_filtered(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 84000.0),
            make_quote("q2", "ETH", "2026-04-01T11:59:00Z", "binance", "exchange_direct", 0, 1800.0),
        ]
        result = resolver.resolve("BTC", "2026-04-01T12:00:00Z", quotes)
        self.assertEqual(result["resolved_price"], 84000.0)

    def test_interpolation_confidence_penalty_zero(self):
        policy = dict(DEFAULT_POLICY)
        policy["staleness"] = {"max_staleness_seconds": 1800, "stale_action": "interpolate"}
        policy["interpolation"] = {"enabled": True, "method": "linear",
                                    "max_gap_seconds": 14400, "confidence_penalty": 0.0}
        resolver = QuoteResolver(policy)
        quotes = [
            make_quote("q1", "LINK", "2026-04-01T11:00:00Z", "cg", "aggregator", 0, 10.0, 1.0),
            make_quote("q2", "LINK", "2026-04-01T13:00:00Z", "cg", "aggregator", 0, 20.0, 1.0),
        ]
        result = resolver.resolve("LINK", "2026-04-01T12:00:00Z", quotes)
        self.assertAlmostEqual(result["confidence"], 0.0, places=6)


# ========================================================================
# 12. CLI / File Output
# ========================================================================

class TestFileOutput(unittest.TestCase):
    """Test CLI file output modes."""

    def test_json_output_to_file(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            tmppath = f.name
        try:
            report = run_replay()
            with open(tmppath, "w") as f:
                json.dump(report, f, indent=2)
            with open(tmppath, "r") as f:
                loaded = json.load(f)
            self.assertTrue(loaded["summary"]["all_pass"])
        finally:
            os.unlink(tmppath)

    def test_attestation_chain_to_file(self):
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            tmppath = f.name
        try:
            example = generate_attestation_example()
            with open(tmppath, "w") as f:
                json.dump(example, f, indent=2)
            with open(tmppath, "r") as f:
                loaded = json.load(f)
            self.assertTrue(loaded["chain_valid"])
            self.assertEqual(len(loaded["records"]), 2)
        finally:
            os.unlink(tmppath)


# ========================================================================
# 13. Constants and Defaults
# ========================================================================

class TestConstants(unittest.TestCase):
    """Test module constants and defaults."""

    def test_default_policy_has_all_sections(self):
        required = ["policy_version", "source_hierarchy", "staleness",
                     "tolerance", "interpolation", "dispute"]
        for section in required:
            self.assertIn(section, DEFAULT_POLICY)

    def test_default_staleness(self):
        self.assertEqual(DEFAULT_POLICY["staleness"]["max_staleness_seconds"], 7200)

    def test_default_tolerance(self):
        self.assertEqual(DEFAULT_POLICY["tolerance"]["max_deviation_percent"], 2.0)

    def test_default_interpolation_method(self):
        self.assertEqual(DEFAULT_POLICY["interpolation"]["method"], "linear")

    def test_resolution_statuses_set(self):
        self.assertIn("RESOLVED", RESOLUTION_STATUSES)
        self.assertIn("UNRESOLVED", RESOLUTION_STATUSES)
        self.assertIn("RESOLVED_DISPUTE", RESOLUTION_STATUSES)

    def test_resolution_methods_set(self):
        self.assertIn("first_valid", RESOLUTION_METHODS)
        self.assertIn("interpolated", RESOLUTION_METHODS)
        self.assertIn("dispute_median", RESOLUTION_METHODS)


# ========================================================================
# 14. Attestation Integration with Resolution
# ========================================================================

class TestAttestationIntegration(unittest.TestCase):
    """Test attestation chain works with resolved quotes."""

    def test_resolve_and_attest(self):
        resolver = QuoteResolver()
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "binance", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T12:15:00Z", "binance", "exchange_direct", 0, 84100.0),
        ]
        chain = build_attestation_chain(quotes, "test")
        attestations = [q["attestation"] for q in chain]
        valid, err = verify_attestation_chain(attestations)
        self.assertTrue(valid)

    def test_tampered_price_breaks_hash(self):
        quotes = [
            make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "binance", "exchange_direct", 0, 84000.0),
            make_quote("q2", "BTC", "2026-04-01T12:15:00Z", "binance", "exchange_direct", 0, 84100.0),
        ]
        chain = build_attestation_chain(quotes)
        # Tamper with price
        original_hash = chain[0]["attestation"]["hash"]
        chain[0]["price"] = 99999.0
        recomputed = compute_quote_hash(
            chain[0]["quote_id"], chain[0]["symbol"], chain[0]["timestamp"],
            chain[0]["source"]["source_id"], chain[0]["price"], chain[0]["confidence"]
        )
        self.assertNotEqual(original_hash, recomputed)

    def test_long_chain_verification(self):
        quotes = []
        for i in range(10):
            quotes.append(make_quote(
                f"q{i}", "BTC", f"2026-04-01T{12+i}:00:00Z",
                "binance", "exchange_direct", 0, 84000.0 + i * 100
            ))
        chain = build_attestation_chain(quotes)
        attestations = [q["attestation"] for q in chain]
        valid, err = verify_attestation_chain(attestations)
        self.assertTrue(valid)
        self.assertEqual(len(chain), 10)


# ========================================================================
# 15. Replay Report Structure
# ========================================================================

class TestReplayReportStructure(unittest.TestCase):
    """Test the structure of replay reports."""

    def test_report_has_summary(self):
        report = run_replay()
        self.assertIn("summary", report)
        self.assertIn("total", report["summary"])
        self.assertIn("matches", report["summary"])
        self.assertIn("mismatches", report["summary"])
        self.assertIn("all_pass", report["summary"])

    def test_report_has_cases(self):
        report = run_replay()
        self.assertIn("cases", report)
        self.assertGreaterEqual(len(report["cases"]), 8)

    def test_each_case_has_required_fields(self):
        report = run_replay()
        for case in report["cases"]:
            self.assertIn("case_id", case)
            self.assertIn("match", case)
            self.assertIn("actual", case)
            self.assertIn("expected", case)
            self.assertIn("mismatches", case)


# ========================================================================
# 16. New Required Fields: attestation_hash, source_tier, feed_version
# ========================================================================

class TestAttestationHashField(unittest.TestCase):
    """Test attestation_hash as a top-level quote field."""

    def test_make_quote_includes_attestation_hash(self):
        q = make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "binance", "exchange_direct", 0, 84000.0)
        self.assertIn("attestation_hash", q)
        self.assertEqual(len(q["attestation_hash"]), 64)

    def test_attestation_hash_matches_compute(self):
        q = make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "binance", "exchange_direct", 0, 84000.0)
        expected = compute_quote_hash("q1", "BTC", "2026-04-01T12:00:00Z", "binance", 84000.0, 1.0)
        self.assertEqual(q["attestation_hash"], expected)

    def test_attestation_hash_in_fixtures(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            for q in fix["input"]["available_quotes"]:
                self.assertIn("attestation_hash", q,
                              f"Missing attestation_hash in {fix['case_id']}")
                self.assertEqual(len(q["attestation_hash"]), 64,
                                 f"Bad hash length in {fix['case_id']}")

    def test_attestation_hash_validates(self):
        q = make_quote("q1", "BTC", "2026-04-01T12:00:00Z", "binance", "exchange_direct", 0, 84000.0)
        valid, errors = validate_quote(q)
        self.assertTrue(valid, f"Validation errors: {errors}")


class TestSourceTierField(unittest.TestCase):
    """Test source_tier as a top-level quote field."""

    def test_derive_all_tiers(self):
        self.assertEqual(derive_source_tier("exchange_direct"), "tier_1_exchange")
        self.assertEqual(derive_source_tier("aggregator"), "tier_2_aggregator")
        self.assertEqual(derive_source_tier("api_provider"), "tier_3_api")
        self.assertEqual(derive_source_tier("manual"), "tier_4_manual")
        self.assertEqual(derive_source_tier("interpolated"), "tier_5_interpolated")

    def test_unknown_source_defaults_tier_3(self):
        self.assertEqual(derive_source_tier("unknown"), "tier_3_api")

    def test_make_quote_includes_source_tier(self):
        q = make_quote("q1", "BTC", "ts", "binance", "exchange_direct", 0, 84000.0)
        self.assertEqual(q["source_tier"], "tier_1_exchange")

    def test_source_tier_in_fixtures(self):
        fixtures = load_fixtures()
        valid_tiers = set(SOURCE_TIER_MAP.values())
        for fix in fixtures:
            for q in fix["input"]["available_quotes"]:
                self.assertIn("source_tier", q,
                              f"Missing source_tier in {fix['case_id']}")
                self.assertIn(q["source_tier"], valid_tiers,
                              f"Bad source_tier in {fix['case_id']}: {q['source_tier']}")

    def test_source_tier_validates(self):
        q = make_quote("q1", "BTC", "ts", "binance", "exchange_direct", 0, 84000.0)
        valid, errors = validate_quote(q)
        self.assertTrue(valid, f"Validation errors: {errors}")

    def test_invalid_source_tier_fails(self):
        q = make_quote("q1", "BTC", "ts", "binance", "exchange_direct", 0, 84000.0)
        q["source_tier"] = "invalid_tier"
        valid, errors = validate_quote(q)
        self.assertFalse(valid)
        self.assertTrue(any("source_tier" in e for e in errors))


class TestFeedVersionField(unittest.TestCase):
    """Test feed_version as a top-level quote field."""

    def test_make_quote_includes_feed_version(self):
        q = make_quote("q1", "BTC", "ts", "binance", "exchange_direct", 0, 84000.0)
        self.assertEqual(q["feed_version"], "1.0.0")

    def test_feed_version_in_fixtures(self):
        fixtures = load_fixtures()
        for fix in fixtures:
            for q in fix["input"]["available_quotes"]:
                self.assertIn("feed_version", q,
                              f"Missing feed_version in {fix['case_id']}")
                self.assertRegex(q["feed_version"], r"^\d+\.\d+\.\d+$",
                                 f"Bad feed_version in {fix['case_id']}")

    def test_feed_version_validates(self):
        q = make_quote("q1", "BTC", "ts", "binance", "exchange_direct", 0, 84000.0)
        valid, errors = validate_quote(q)
        self.assertTrue(valid, f"Validation errors: {errors}")

    def test_invalid_feed_version_fails(self):
        q = make_quote("q1", "BTC", "ts", "binance", "exchange_direct", 0, 84000.0)
        q["feed_version"] = "not-semver"
        valid, errors = validate_quote(q)
        self.assertFalse(valid)
        self.assertTrue(any("feed_version" in e for e in errors))


class TestEnrichQuote(unittest.TestCase):
    """Test the enrich_quote utility."""

    def test_enrich_adds_missing_fields(self):
        q = {
            "quote_id": "q1", "symbol": "BTC", "timestamp": "ts",
            "source": {"source_id": "binance", "source_type": "exchange_direct", "priority": 0},
            "price": 84000.0, "confidence": 1.0
        }
        enrich_quote(q)
        self.assertIn("attestation_hash", q)
        self.assertIn("source_tier", q)
        self.assertIn("feed_version", q)

    def test_enrich_does_not_overwrite_existing(self):
        q = make_quote("q1", "BTC", "ts", "binance", "exchange_direct", 0, 84000.0)
        q["feed_version"] = "2.0.0"
        enrich_quote(q)
        self.assertEqual(q["feed_version"], "2.0.0")


# ========================================================================
# 17. 24h Attribution Path
# ========================================================================

class TestAttributionPath(unittest.TestCase):
    """Test 24h attribution path constants and integration."""

    def test_attribution_window_hours(self):
        self.assertEqual(ATTRIBUTION_WINDOW_HOURS, 24)

    def test_attribution_window_seconds(self):
        self.assertEqual(ATTRIBUTION_WINDOW_SECONDS, 86400)

    def test_resolution_policy_has_attribution_path(self):
        import json
        policy_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "resolution_policy.json"
        )
        with open(policy_path) as f:
            policy = json.load(f)
        self.assertIn("attribution_path", policy["properties"])
        att = policy["properties"]["attribution_path"]
        self.assertEqual(att["properties"]["window_hours"]["const"], 24)


class TestAttestationExampleNewFields(unittest.TestCase):
    """Test that the attestation example includes new required fields."""

    def test_example_records_have_source_tier(self):
        ex = generate_attestation_example()
        for r in ex["records"]:
            self.assertIn("source_tier", r)
            self.assertEqual(r["source_tier"], "tier_1_exchange")

    def test_example_records_have_feed_version(self):
        ex = generate_attestation_example()
        for r in ex["records"]:
            self.assertIn("feed_version", r)
            self.assertEqual(r["feed_version"], FEED_VERSION)

    def test_example_records_have_attestation_hash(self):
        ex = generate_attestation_example()
        for r in ex["records"]:
            self.assertIn("attestation_hash", r)
            self.assertEqual(len(r["attestation_hash"]), 64)


if __name__ == "__main__":
    unittest.main()
