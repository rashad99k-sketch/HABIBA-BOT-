"""Static integration checks for the fail-closed execute_entry boundary."""
import ast
from pathlib import Path


def _functions(tree, name):
    return [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == name]


def test_final_execute_entry_wrapper_checks_strategy_before_order_execution():
    tree = ast.parse(Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8"))
    wrappers = _functions(tree, "execute_entry")
    assert len(wrappers) >= 2  # original execution function plus final compatibility wrapper
    wrapper = max(wrappers, key=lambda node: node.lineno)
    calls = [node for node in ast.walk(wrapper) if isinstance(node, ast.Call)]
    called_names = {node.func.id for node in calls if isinstance(node.func, ast.Name)}
    assert "execution_handoff_allowed" in called_names
    assert "_original_execute_entry_for_journal" in called_names
    guard_line = next(node.lineno for node in calls
                      if isinstance(node.func, ast.Name) and node.func.id == "execution_handoff_allowed")
    execution_line = next(node.lineno for node in calls
                          if isinstance(node.func, ast.Name) and node.func.id == "_original_execute_entry_for_journal")
    assert guard_line < execution_line
    assert any(isinstance(node, ast.Return) and isinstance(node.value, ast.Constant)
               and node.value.value is False for node in ast.walk(wrapper))


def test_watchlist_refresh_uses_direct_ema9_vwap_cross_engine():
    tree = ast.parse(Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8"))
    refresh = _functions(tree, "_refresh_ema_vwap_setups")[0]
    calls = [node.func.attr for node in ast.walk(refresh) if isinstance(node, ast.Call)
             and isinstance(node.func, ast.Attribute)]
    assert "evaluate_direct_cross" in calls
    assert "evaluate" not in calls


def test_direct_cross_entry_has_atr_stop_fallback_without_causal_zone():
    source = Path(__file__).with_name("ggpbot.py").read_text(encoding="utf-8")
    assert "ATR_FALLBACK_NO_CAUSAL_ZONE" in source
    assert "EMA9_VWAP_CROSS_VWAP_GATE" in source
