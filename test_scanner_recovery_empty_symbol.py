from pathlib import Path


def test_flat_scanner_recovery_verifies_account_wide_without_empty_symbol_guard():
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    start = source.index("def _verify_engine_recovery(symbol=None):")
    end = source.index("\ndef _scanner_recovery_hard_blocked", start)
    recovery = source[start:end]
    assert 'if raw_symbol:' in recovery
    assert 'resolve_position_symbol(raw_symbol)' in recovery
    assert 'if not raw_symbol:' in recovery
    assert 'all_positions = safe_api_call(ex.fetch_positions)' in recovery
    assert 'FLAT_VERIFIED_ACCOUNT_WIDE' in recovery
    assert 'engine_recovery_symbol_missing_with_open_position' in recovery


def test_institutional_flow_is_not_registered_as_global_discovery_recovery_worker():
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    assert 'restartable=is_global_discovery' in source
    assert 'recovery_verifier=_verify_scanner_recovery if is_global_discovery else None' in source
    assert 'restart_fn=_resume_scanner_after_flat_verification if is_global_discovery else None' in source
    assert 'and not (STATE.get("open") or TRADE_STATE.get("in_position"))' in source
