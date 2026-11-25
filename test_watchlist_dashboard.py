import unittest
from roro_watchlist_dashboard import build_watchlist_dashboard_payload

class WatchlistDashboardProjectionTests(unittest.TestCase):
    def test_payload_is_compact_and_does_not_mutate_source(self):
        source = {"BTCUSDT": {"symbol": "BTC/USDT:USDT", "side": "BUY", "decision_state": "WAITING_FOR_RETEST", "score": 72, "price": 60000, "market_regime": "TREND_CONTINUATION", "decision_context": {"setup_stage": "WAITING_FOR_RETEST", "required_next_event": "CLOSED_ZONE_RETEST", "zone_context": {"zone_quality_score": 80, "supply_demand": {"valid": True}}, "liquidity_context": {"event": "SWEEP", "stage": "CONFIRMED"}, "structure_context": {"pattern": "HH_HL"}, "cross_context": {"state": "CONFIRMED_BUY_CROSS", "durable": True}}, "evidence": ["large", "diagnostic", "payload"], "side_analyses": {"BUY": {"deep": "data"}}}}
        original = repr(source)
        result = build_watchlist_dashboard_payload(source)
        self.assertEqual(repr(source), original)
        self.assertEqual(result["BTCUSDT"]["next_event"], "CLOSED_ZONE_RETEST")
        self.assertTrue(result["BTCUSDT"]["durable"])
        self.assertNotIn("evidence", result["BTCUSDT"])
        self.assertNotIn("side_analyses", result["BTCUSDT"])

    def test_limit_is_bounded_and_entry_ready_is_prioritized(self):
        source = {f"SYM{i}": {"symbol": f"SYM{i}", "decision_state": "WAITING_FOR_CONFIRMATION", "score": 99} for i in range(40)}
        source["READY"] = {"symbol": "READY", "decision_state": "ENTRY_READY", "score": 1}
        result = build_watchlist_dashboard_payload(source, limit=10)
        self.assertEqual(len(result), 10)
        self.assertIn("READY", result)

    def test_non_mapping_input_returns_empty_payload(self):
        self.assertEqual(build_watchlist_dashboard_payload(None), {})

if __name__ == "__main__":
    unittest.main()
