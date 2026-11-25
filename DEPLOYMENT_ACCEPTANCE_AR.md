# قائمة قبول الإصدار — PAPER ثم BingX Demo

## قبل التشغيل المحلي

1. فك ZIP كاملًا في مجلد مستقل.
2. أنشئ `.env` من `.env.example` عند الحاجة.
3. اترك `PAPER_MODE=true` و`ENABLE_LIVE_TRADING=false`.
4. شغّل `RUN.bat` من جذر المشروع.
5. تحقق من ظهور رسائل `[DASHBOARD]` ومنفذي 8000 و8080.
6. تحقق من `/health` و`/data` ومن حالة Scanner 1 وScanner 2.

## ما يجب مراقبته في PAPER

- تزايد عدد الرموز التي وصلت إلى Unified Institutional Watchlist.
- ظهور BUY وSELL hypotheses لكل رمز.
- ظهور أسباب WAITING / REJECTED / NO_CHASE بدل انتظار Cross فقط.
- وجود OB/FVG/Liquidity/Structure مع مصدر وتوقيت بيانات.
- عدم وجود `ENTRY_READY` دون Event ID وStructural SL.
- عدم وجود أي أمر حقيقي على BingX.

## قبول BingX Demo

اختبر منفصلًا:

- تفعيل 10x والتحقق من قراءة الرافعة مرة أخرى.
- 40% / 30% كهامش مستهدف مع Risk cap 1% عند SL.
- دقة العقد وcontractSize والحد الأدنى للكمية.
- Native SL coverage.
- TP1 50%، ثم TP2 للكمية المتبقية.
- RANGE_SCALP فقط عند تحقق أدلة الضعف/السوق العرضي أو السيولة الرقيقة.
- الإغلاق الخارجي وضرب SL وتحديد السبب من سجل BingX.
- Restart بعد الدخول وبعد TP1 وأثناء أمر إغلاق غير محسوم.
- استعادة SL وTP stages وTrailing والكمية المتبقية.
- انقطاع API وعودة الاتصال دون أوامر مكررة.

## حظر LIVE

لا يُعلن الإصدار LIVE READY قبل نجاح Windows runtime وBingX Demo والاختبارات التشغيلية الطويلة. نجاح اختبارات الوحدة وحده لا يثبت الربحية أو سلامة التنفيذ الحي.


## قبول سجل الإدارة وTelegram

- افتح `/decision` وتأكد من استجابة JSON بحالة HTTP 200 حتى عندما لا توجد أحداث.
- نفّذ دورة PAPER: Entry ثم TP1؛ يجب أن يظهر 🥇 TP1 VERIFIED بعد محاكاة/تأكيد التنفيذ فقط.
- أكمل TP2 أو Strict Exit؛ يجب أن يظهر 🥈 أو ⛔ مع حالة PAPER_SIMULATED أو EXCHANGE_VERIFIED، لا أن يُسجل كتنفيذ LIVE أثناء PAPER.
- عند غياب Telegram credentials، يجب أن يستمر البوت دون تنبيه ولا خطأ تداول. عند وضع credentials الجديدة، افحص Telegram test message/notification مع عدم عرض الأسرار في `/health` أو Logs.
- لا تعتمد على نجاح unit tests كإثبات لإرسال Telegram فعلي أو تنفيذ BingX فعلي.


## 2026-11-09 — حالة بوابات التنفيذ

الحالة ما زالت **PAPER / Demo Preparation — NOT LIVE READY**. اجتازت الحزمة 96 اختبارًا محليًا وPreflight، وثُبت CCXT على `4.5.92`. أضيف محول قراءة وضع الحساب ومسار تنفيذ موحد وتحقق Native STOP_MARKET ومهاجرات SQLite.

لم تُنفذ أوامر BingX حقيقية. القبول المتبقي قبل LIVE هو BingX Demo/VST، Hedge وOne-Way، الرافعة وNative SL، partial fill، restart، انقطاع الشبكة، وتشغيل Windows فعلي.


## 2026-11-25 — DigitalOcean App Platform liveness and safe restart

The bot exposes two distinct health routes:

- `/health`: readiness/diagnostics. It reports a stalled MainThread or Unified Watchlist worker as HTTP 503. A normal refresh in progress is a trading-readiness blocker, not a process failure.
- `/liveness`: restart authorization. It returns HTTP 503 only after the internal exchange recovery exit gate has positively recorded `EXIT_RECONCILED_FLAT` or `EXIT_VERIFIED_PROTECTED_OPEN`. If exchange exposure or native protection is UNKNOWN, it returns HTTP 200 so an App Platform probe cannot bypass the bot's fail-closed exit gate.

Recommended App Platform component settings (merge these fields into the **current complete App Spec**; do not replace the existing spec with this fragment):

```yaml
health_check:
  initial_delay_seconds: 30
  period_seconds: 10
  timeout_seconds: 5
  success_threshold: 1
  failure_threshold: 5
  http_path: /health

liveness_health_check:
  initial_delay_seconds: 60
  period_seconds: 15
  timeout_seconds: 5
  success_threshold: 1
  failure_threshold: 3
  http_path: /liveness
```

Leave `port` unset to use the component's configured `http_port`. DigitalOcean documents health checks as readiness probes and liveness checks as the mechanism that restarts a component when the liveness probe fails. The current App Spec is not stored in this repository, so these settings must be applied to the existing DigitalOcean App Platform component configuration (or to the downloaded complete App Spec) before relying on liveness-based recovery.

Do not point the liveness probe at `/health`: that route is diagnostic/readiness and may fail while an exchange position is still being reconciled. The dedicated `/liveness` route is deliberately gated by the same verified recovery outcome required before process exit.

## 2026-11-25 — Long-run acceptance gates

- Keep PAPER mode for CI and all synthetic fault-injection tests.
- Exercise API lock contention, rate-limit responses, read timeouts, worker stalls, late worker completion, and restart recovery.
- Validate OHLCV with empty, short, duplicated, non-finite, unordered, and malformed rows; invalid data must be rejected, not silently repaired into entry evidence.
- Verify that a stalled Watchlist worker is single-flight, cannot be replaced while still alive, cannot restore entry readiness after timing out, and triggers process recovery only through the exchange exit gate.
- Verify that open positions with UNKNOWN native protection prevent process exit; a verified exchange-side protective stop may permit a controlled restart.
- Run the complete regression suite without increasing its timeout or suppressing hanging tests.
- Run a staged PAPER soak, then BingX Demo soak, before any LIVE approval. A six-month uptime target is an operational objective, not a guarantee from unit tests.


### Required DigitalOcean runtime environment binding

DigitalOcean App Platform bindable variables are not assumed to be injected automatically. Add this runtime environment variable to the existing `habiba-bot` component:

```yaml
envs:
  - key: DIGITALOCEAN_APP_ID
    scope: RUN_TIME
    value: ${APP_ID}
```

This is the supervisor identity consumed by the bot's process-restart guard. Without it, the bot deliberately refuses an automatic process exit because it cannot prove that an external supervisor will bring the service back. Preserve all existing environment variables and secrets when applying this change.
