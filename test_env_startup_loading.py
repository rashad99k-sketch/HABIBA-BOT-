from pathlib import Path
ROOT = Path(__file__).resolve().parent
BOT = (ROOT / "ggpbot.py").read_text(encoding="utf-8")
TEMPLATE = (ROOT / ".env.example").read_text(encoding="utf-8")
def test_env_template_is_startup_fallback_only():
    assert 'ENV_TEMPLATE_PATH = os.path.join(ENV_DIR, ".env.example")' in BOT
    assert 'elif os.path.isfile(ENV_TEMPLATE_PATH):' in BOT
    assert 'load_dotenv(dotenv_path=ENV_TEMPLATE_PATH, override=False)' in BOT
def test_real_env_precedes_template():
    assert 'if os.path.isfile(ENV_FILE_PATH):' in BOT
    assert 'load_dotenv(dotenv_path=ENV_FILE_PATH, override=_dotenv_override)' in BOT
def test_template_marks_legacy_cap_compatibility_and_exact_formula():
    assert 'MAX_TRADE_RISK_PCT=10.0' in TEMPLATE
    assert 'Deprecated compatibility setting' in TEMPLATE
    assert 'ACCOUNT_TOTAL x 20% x 20 / price' in TEMPLATE
