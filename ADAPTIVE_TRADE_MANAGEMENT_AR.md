# Adaptive Scalp & Trend Trade Management — تقرير هندسي

الفرع: `main`  
الـCommit قبل التعديل: `6b993c17b07703f99a8f63c2f5f3d8da5078e939`  
لا توجد مفاتيح حقيقية ولا أوامر ولا صفقات ولا استدعاءات BingX حية أثناء هذا العمل.

---

## 1. إدارة الصفقات قبل التعديل

- قرار الدخول كان ولا يزال في `Entry Engine` المستقل (`EMA9 + Session VWAP + ADX + Closed Candle + Entry Authorization`) ولم يُلمس.
- إثبات المركز يتم عبر `fetch_position_tri_state()` ثم `ExchangeSyncService`/`_snapshot_from_position()`.
- الجهة الوحيدة لقرارات الإدارة الروتينية هي `TradeManagementBoard.evaluate()` في `ggpbot.py`.
- `ggp_profit_brain.py` كان بالفعل محرك سياسات نقي (لا استدعاءات منصة ولا أوامر) يصنّف:
  - `RANGE_SCALP` لإدارة هدف واحد،
  - `TREND_RIDE` لإدارة 50/50،
  - `TWO_STAGE_50_50` كحالة آمنة عند نقص الأدلة.
- التنفيذ يمر حصراً عبر `safe_close_partial()` / `safe_close_position()` → `_verified_market_reduce()` → `_verified_market_reduce_impl()` مع تحقق فرق الكمية الفعلي على المنصة، `reduceOnly` في وضع One-Way، قفل `_CLOSE_ORDER_LOCK`، ومنع أي إغلاق جديد عند `execution_unresolved` أو `close_reconciliation_required`.
- الوقف الأصلي على المنصة ثابت بعد الدخول (`move_native_stop_on_exchange()` no-op)، والحماية الإضافية داخلية/اصطناعية فقط.

---

## 2. التغييرات بعد التعديل

### 2.1 `ggp_profit_brain.py`
أُضيفت دوال سياسات نقية فقط (بدون منصة وبدون أوامر):

- `verify_roe_inputs()`: يتحقق من الرافعة الفعلية، الهامش، الكمية، سعر الدخول/المارك، الـunrealized PnL، ورسوم/تمويل اختيارية، ويقارن ROE المحسوب بالقيمة المُبلّغ عنها من المنصة. الرافعة غير الموثّقة تمنع استخدام أهداف ROE. أي رافعة أخرى غير 20x تُقبل إذا كانت موثّقة لأن الأهداف طبيعية بالنسبة للهامش، مع تشخيص صريح.
- `roe_target_price()`: تحويل تشخيصي فقط من هدف ROE إلى مستوى سعر باستخدام الهامش والكمية والرافعة الفعلية (وحجم العقد إن وُجد؛ وإلا يُستنتج من هامش المنصة).
- `analyze_post_entry_behavior()`: تصنيف من الشموم المغلقة فقط إلى `RETEST`, `PULLBACK`, `MICRO_PULLBACK`, `TREND_CONTINUATION`, `DISTRIBUTION_PHASE`, `INSTITUTIONAL_EXIT`.
- `map_profit_mode_to_regime()` و`transition_trade_regime()`: حالة صريحة `SCALP`, `TREND`, `EXIT_REQUIRED`, `CLOSED` معConfirmation من شمعتين مغلقتين، ومنع التبديل بسبب الربح وحده، وخروج فوري عند فرضية مؤسسية منهارة.
- `select_scalp_roe_target()`: يختار هدفاً واحداً فقط من `5%`, `10%`, `20%` ROE حسب قوة المنطقة، المسافة لمقاومة/دعم، ADX واتجاهه، الحجم، DI، شموع الإغلاق، والسلوك بعد الدخول.

### 2.2 `ggpbot.py`
- استيراد دوال السياسة الجديدة دون إنشاء محرك موازٍ.
- `PositionSnapshot` و`_snapshot_from_position()` و`sync_position_state()` تسجّل الآن ما إذا كانت الرافعة مُبلّغاً عنها فعلياً من المنصة؛ قيمة 20x الافتراضية صارت للعرض فقط ولا تسمح بأهداف ROE.
- `LiveTradeManager.start_trade()` يعيد تهيئة حالة التصنيف للمراكز الجديدة فقط. عند الاستعادة من السجل أُضيف `preserve_management_state=True` حتى لا يُمسح `trade_regime_state` و`profit_mode` وأهداف ROE المحفوظة بين TP1 وTP2.
- `sync_position_state()` و`ExchangeSyncService.fetch_live_snapshot()` و`_on_reconciled()` تنسخ الآن `leverage_verified` و`leverage_source` و`contract_size` من لقطة المنصة إلى `STATE`؛ الرافعة غير الموثّقة لا تسمح بأهداف ROE.
- `verify_roe_inputs()` تُعيد الآن حقل `side` صراحةً؛ بدونه كان `roe_target_price()` يُرجع صفراً رغم صحة الهامش والرافعة والكمية، فتختفي مستويات TP1/TP2 التشخيصية ولا يُحفظ `tp2_price` في السجل.
- `_apply_management()`:
  - يحسب `roe_verified`/`roe_diagnostic` قبل أي هدف،
  - يضيف تحليل السلوك بعد الدخول،
  - يحدّث `trade_regime_state` و`profit_mode` عبر آلية الـhysteresis الموجودة،
  - يضبط هدف SCALP واحداً، أو أهداف TREND 40/60،
  - يمرّر كل ذلك داخل `context` إلى `TradeManagementBoard.evaluate()`.
- `TradeManagementBoard.evaluate()`:
  - يبقي بوابات الاستعادة/الطوارئ/Stop قبل بوابة ROE،
  - SCALP: إغلاق كامل عند الهدف المختار، بدون TP1/TP2،
  - TREND: TP1 عند 40% ROE لإغلاق نصف الكمية الأصلية، ثم TP2 عند 60% ROE للباقي،
  - يمنع تكرار TP1/TP2 عبر الأعلام الحالية،
  - أي حالة ROE غير موثّقة تعطي `HOLD` تشخيصياً دون لمس الحماية.
- `_execute_decision()` يسجّل حدث `SCALP_TARGET_VERIFIED` عبر نفس قناة الأحداث الحالية، ويضبط `trade_regime_state = CLOSED` بعد إغلاق مُتحقق منه.

### 2.3 اختبارات
أُضيف `test_adaptive_scalp_trend_management.py` بـ22 اختباراً: تصنيف، أهداف 5/10/20، إغلاق SCALP كامل، TP1/TP2، منع التكرار، بوابة ROE، كل أنواع السلوك، الانتقال بين الحالات، ومنع قرار جديد عند عدم وضوح التنفيذ.

أُضيف أيضاً `test_adaptive_lifecycle_integration.py` بـ3 اختبارات تكامل مقلّدة بالكامل (بدون مفاتيح أو اتصال حي):
1. فتح صفقة عبر `open_position()` الحقيقي، تحقق ثلاثي الحالة من المنصة، تصنيف SCALP من أدلة، هدف ROE واحد، ثم إغلاق كامل مُتحقق منه بحدث `SCALP_TARGET_VERIFIED` ومنع إغلاق مكرر.
2. تصنيف TREND من شموم/مؤشرات متوافقة، TP1 عند 40% يغلق نصف الكمية ويتحقق من الكمية المتبقية على المنصة، إعادة تشغيل البوت (STATE فارغ + نفس قاعدة البيانات + نفس المنصة المقلّدة)، استعادة الحالة دون تكرار أمر، ثم TP2 عند 60% يغلق كل الباقي ويتحقق من FLAT.
3. وضع One-Way: أمر الإغلاق يحمل `reduceOnly=true` و`positionSide=BOTH` مع تحقق فرق الكمية فعلياً.

المنصة المقلّدة لا تغيّر الكمية إلا بعد إرسال أمر تقليص وقراءته، لذلك كل تأكيدات OPEN/FLAT والكمية المتبقية مبنية على ملاحظة من المنصة لا على افتراض محلي.

### 2.4 توثيق
هذا الملف: `ADAPTIVE_TRADE_MANAGEMENT_AR.md`.

---

## 3. الملفات التي تغيّرت

- `ggp_profit_brain.py`
- `ggpbot.py`
- `test_adaptive_scalp_trend_management.py` (جديد)
- `test_adaptive_lifecycle_integration.py` (جديد)
- `ADAPTIVE_TRADE_MANAGEMENT_AR.md` (جديد)

## 4. الملفات التي لم تغيّر

- `ggp_position_sizing.py`, `bingx_execution.py`, `bingx_protection.py`, `ggp_recovery.py`, `roro_trade_journal.py`.
- `roro_opportunity_pipeline.py`, `roro_confluence.py`, `roro_institutional.py`, `ggp_institutional_bridge.py`.
- `ggp_market_data.py`, `ggp_discovery_scanner.py`, `roro_strategy_engine.py`.
- Dashboard وTelegram وLogging وWatchdog وكل ملفات `.bat` والتكوين.

---

## 5. كيفية اختيار SCALP أو TREND

1. `classify_profit_mode()` يظل مصدر أدلة الاتجاه/النطاق ولا يُستبدل.
2. `analyze_post_entry_behavior()` يضيف أدلة السلوك من شموم مغلقة فقط.
3. الخريطة: أدلة ترند مؤكدة → `TREND`؛ غياب الأدلة أو ضعفها → `SCALP`؛ انهيار فرضية/هيكل → `EXIT_REQUIRED`.
4. التحويل `SCALP → TREND` يتطلب شمعتين مغلقتين متتاليتين مع سلوك داعم (continuation/retest/pullback) وليس مجرد تحول الربح إلى موجب.
5. التحويل `TREND → SCALP` يتطلب شمعتين مع ضعف مؤكد (distribution/pullback/micro pullback).
6. `INSTITUTIONAL_EXIT` ينتقل فوراً دون انتظار الـhysteresis.

---

## 6. حساب ROE

- الصيغة المعتمدة: `ROE = (unrealized_pnl - fees - funding) / initial_margin * 100`.
- تُستخدم الكمية الفعلية والهامش وسعر الدخول من لقطة المنصة، مع قراءة الرافعة الفعلية؛ لا تُفترض 20x.
- تُقارن النتيجة بـROE المُبلّغ من المنصة؛ تجاوز التفاوت المسموح يمنع أهداف ROE ويُصدر تشخيصاً.
- إذا كانت الرافعة 10x مثلاً، تبقى الأهداف صحيحة لأنها على الهامش، مع رسالة `roe_normalized_to_actual_leverage_10_...`.
- المستويات السعرية المعروضة تشخيصية فقط؛ قرار الإغلاق يقارن ROE مباشرة.

---

## 7. Retest / Pullback

- `RETEST`: لمس حديث لمنطقة الدخول خلال آخر 3 شموم مغلقة، انعكاس ضعيف، ارتداد للإغلاق داخل المنطقة، continuation ≥ 0.5، وبدون كسر هيكل. يُسجّل `retest_success`.
- `MICRO_PULLBACK`: تراجع ≤ 0.35 ATR، شمعة عكسية واحدة على الأكثر، continuation ≥ 0.55، وبدون أدلة ترند كافية.
- `PULLBACK`: تصحيح حتى 1.25 ATR داخل هيكل سليم أو إعادة اختبار غير مستعادة للمنطقة؛ لا يُعتبر خروجاً تلقائياً.
- `TREND_CONTINUATION`: شمعتان مغلقتان متوافقتان + ADX ≥ 22 + DI موافق + هيكل موافق + continuation ≥ 0.6.
- `DISTRIBUTION_PHASE`: distribution risk مرتفع أو شموع عكسية بحجم موسّع وضعف continuation؛ يخفض هدف SCALP ولا يفرض إغلاقاً فورياً.
- `INSTITUTIONAL_EXIT`: structural invalidation أو failure score ≥ 70 أو كسر هيكل مع توزيع قوي؛ يفرض `EXIT_REQUIRED`.

---

## 8. تنفيذ TP1 / TP2 ومنع التعارض

- لا توجد قناة إغلاق جديدة؛ كل القرارات تخرج من `TradeManagementBoard` وتُنفّذ عبر `_verified_market_reduce`.
- SCALP يستخدم `FULL_CLOSE` واحداً بـ`close_reason="SCALP_TARGET"`؛ لا استدعاء لـ`safe_close_partial`.
- TREND يستخدم `PARTIAL_CLOSE` إلى `initial_qty * 0.5` عند 40% ROE، ثم `FULL_CLOSE` للباقي عند 60% ROE.
- `tp1_hit` / `tp2_hit` / `be_done` / `trail_active` تمنع التكرار، وتُحدَّث فقط بعد نجاح التحقق من المنصة.
- `execution_unresolved`, `close_reconciliation_required`, `recovery_hold` توقف أي قرار جديد قبل حساب الهدف.
- الوقف الأصلي على المنصة لا يُعدّل؛ BE/trailing داخلية فقط.

---

## 9. نتائج الاختبارات الفعلية

| الفحص | قبل التعديل | بعد التعديل |
|---|---:|---:|
| `python -m pytest -q` | 304 passed / 7 failed | **329 passed / 7 failed** |
| اختبارات السياسات الجديدة | — | 22 passed |
| اختبارات التكامل الجديدة | — | 3 passed |
| اختبارات مستهدفة ( adaptive + integration + hardening + recovery + compatibility + market-data ) | — | 139 passed / 3 failed (نفس الـ3 القديمة) |
| `python preflight.py` | PREFLIGHT OK | PREFLIGHT OK |
| `python -m compileall -q .` | exit 0 | exit 0 |

الـ7 اختبارات الفاشلة هي نفسها الموجودة على `main` قبل التعديل ولم تُلمس:
4 اختبارات `test_position_sizing_policy.py` و3 اختبارات `test_recovery_lifecycle.py` الخاصة بنقل الوقف الأصلي. لم أرفع أي timeout ولم أتجاهل فشلاً.

---

## 10. Regression

- لم يتغير كود الدخول، الـADX Gate، تأكيد الشمعة المغلقة، التفويض، تحديد الحجم، الرافعة، SL الأصلي، المزامنة، Scanner، Dashboard، Telegram، Runtime أو Watchdog.
- `move_native_stop_on_exchange()` ما زال no-op عن قصد؛ الاختبارات الثلاثة القديمة التي تتوقع نقل SL الأصلي ما زالت تفشل كما في الخط الأساسي.
- اختبارات `test_profit_brain.py`, `test_adaptive_profit_policy.py`, `test_emergency_telegram_surgical.py`, `test_management_log.py`, `test_main_engine_compatibility.py` و`test_market_data_refresh.py` نجحت بعد التعديل.
- لم يُستخدم Live Trading ولا مفاتيح حساب حقيقية.

---

## 11. قيود ونقاط لم تتحقق

- لم تُجرَ مصالحة حية مع BingX؛ الرافعة/Fee/Funding تُقرأ من الحقول المتاحة في اللقطة/الحالة، وحقول `fees_usdt`/`funding_usdt` غير معبّأة حالياً من بقية البوت فتُعامل كصفر مع إبقاء الواجهة جاهزة لها.
- حجم العقد يُستخدم صراحةً إن وُجد في لقطة المنصة، وإلا يُستنتج من الهامش والرافعة والكمية؛ لم تُجرَ قراءة `market()` جديدة لتجنب تغيير مسار المنصة.
- اختبارات Partial Fill/Rejected/Timeout الموجودة بالفعل في `test_close_diagnostics.py` ما زالت المصدر الأساسي لها ولم تُكرّر؛ اختبار إعادة التشغيل بين TP1 وTP2 أُضيف في `test_adaptive_lifecycle_integration.py` بمحاكاة كاملة.
- الـ7 اختبارات القديمة الفاشلة تحتاج قراراً منفصلاً لأن إصلاحها يغيّر سلوك المخاطرة/SL.
