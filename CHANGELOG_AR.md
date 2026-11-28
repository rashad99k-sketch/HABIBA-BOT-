# سجل التغييرات — JGB/GGP

## إصلاح 2026-11-28 — Opposing Liquidity NameError وMomentum Collapse
- إضافة الاستيراد الناقص `select_opposing_liquidity_pool` إلى `ggpbot.py` من محرك `ggp_market_intelligence` الموجود؛ دون تكرار الدالة أو تغيير خوارزمية اختيار مستويات السيولة.
- منع تصنيف السوق الهادئ على أنه `MOMENTUM_COLLAPSE`؛ يتطلب decay زخمًا سابقًا ثم انكماشًا مؤكدًا.
- تمرير `momentum_collapse_confirmed` إلى Profit Brain، وتأجيل HARD_EXIT الناتج عن حالة MOMENTUM_COLLAPSE غير المؤكدة فقط.
- الحفاظ على الخروج الفوري عند الكسر الهيكلي أو PANIC_EXIT أو LIQUIDITY_EXHAUSTION.
- أضيفت اختبارات انحدار للاستيراد ومنطق الانهيار المؤكد وغير المؤكد.
## إضافة 2026-11-28 — HABIBA EMA9/VWAP Lifecycle Demo Preview
- إضافة لوحة RTL مستقلة على `/lifecycle` داخل Flask Dashboard الحالي، مع رابط من الصفحة الرئيسية.
- إضافة محرك Demo مستقل لكل Cross Event؛ لا يقرأ STATE/MEMORY الحي ولا يستدعي CCXT أو BingX أو `execute_entry()`.
- إضافة Overview وScanner وWatchlist والتحليل المؤسسي وتصنيف A/B/C وترخيص 10 بوابات وخط تنفيذ محاكى من 8 مراحل والمراكز وسجل القرارات وتقرير الإصلاح.
- تثبيت BASE ADX >= 18 كشرط مطلق، ومنع READY دون نجاح بوابات A، ومنع تأكيد المركز دون SL وتعبئة وحماية محاكاة، ومنع تكرار simulated order.
- محاكاة إغلاق SCALP بنسبة 100% عند TP1 وTREND بنسبة 50% عند TP1 والباقي عند TP2.
- الاختبارات المركزة: 68 passed, 1 skipped؛ واختبار soak لمحرك Demo لمدة 120 دورة ناجح. المجموعة الكاملة لم تبدأ بسبب غياب `ccxt`، واختبار HTTP متخطى لغياب Flask في بيئة الفحص.
- تحديث `RELEASE_MANIFEST.txt` ليتطابق مع ملفات الحزمة المسلّمة.


## ترقية 2026-11-10 (تحديث ج) — تحصين TP1 وNative SL (Release 9 FINAL HARDENED)
- **هوية TP1 حسب وضع الحساب:** في Hedge يجب وجود `positionSide` ومطابقته (LONG/SHORT)؛ القيمة المفقودة أو
  المتعارضة تُرفض. في One-Way يجب أن يكون غائبًا أو BOTH، وساق Hedge تُرفض. الوضع غير المعروف يُرفض.
- **ربط الأمر بالصفقة:** يتطلب معرّف أمر صرف + تطابق الرمز بعد التطبيع + جانب الإغلاق المعاكس + حالة
  تعبئة مع `filled>0` + كمية = 50% من الأصل، مع ربط بمعرّف مسجّل أو وسم TP/Partial + وقت الأمر ≥ وقت الدخول.
- **نجاة الدليل بعد Restart:** `_collect_tp1_close_order_ids()` يقرأ معرّفات الإغلاق من Journal
  (snapshot + row) لا من `STATE`؛ ودالة الإثبات تستقبل `close_order_ids` و`position_mode` و`trade_entry_ts` صراحة.
- **Native SL State Machine:** PRECHECK (قراءة المركز + الهوية + الكمية + صلاحية سعر الوقف) ثم
  REPLACEMENT (cancelReplace/إلغاء مؤكد) ثم POSTCHECK (قراءة جديدة). `RETAINED` لا تُعتمد إلا بعد قراءة
  جديدة تُثبت أن الوقف القديم نشط ويغطي الكمية، ولا تُستنتج من غموض إلغاء أو حالة محلية أو رد API قديم.
  `LOST` تعني عدم إمكانية إثبات أي وقف ⇒ Hold.
- الاختبارات المحلية: 229 passed (39 في `test_hardening_release9.py`).

## ترقية 2026-11-10 (تحديث ب) — تحصين TP1 وفشل cancelReplace
- لم يعد انخفاض كمية المركز وحده يُسجَّل كـTP1. لا يُرقّى TP1 إلا إذا كان السجل قد حفظ `tp1_hit=True`
  قبل التعطل أو أثبتت المنصة وجود أمر إغلاق جزئي صادر من البوت وتعبّأ فعلًا؛ وإلا يبقى Hold صريح
  (`unverified_position_reduction_requires_tp1_fill_evidence`). هذا يمنع تسمية إغلاق يدوي/خارجي كـTP1.
- `replace_native_stop` أصبح يعيد `MOVED`/`RETAINED`/`LOST`: عند فشل التحريك يُتحقق من بقاء الوقف
  القديم أو يُعاد إنشاؤه عند مستواه الأصلي، ولا يُنشأ وقف مكرر، ولا يدخل Hold إلا عند فقدان الحماية فعلًا.
- مطابقة دقيقة للهوية والكمية: `RETAINED` تُعتمد فقط بعد قراءة BingX المطابقة (رمز/جانب/`positionSide`/سعر/تغطية كمية)؛
  و`place_verified` يرفض الوقف المتباين ويُلغي القديم ثم يُنشئ وقفًا صحيح الحجم؛ ودليل TP1 يطابق
  جانب الإغلاق و`positionSide` والكمية المتوقعة.
- الاختبارات المحلية: 190 passed (38 منها في `test_recovery_lifecycle.py`).

## ترقية 2026-11-10 — إصلاح جذر RECOVERY_HOLD + تحريك Native SL فعليًا
- إصلاح السبب الجذري لـ`RECOVERY_HOLD`: كانت أعلام الدخول/الإغلاق العابرة (`ENTRY_PENDING`،
  `close_reconciliation_required`، `external_close_detected`) وأي انخفاض في الكمية تبقى شروط Hold
  دائمة، فيبقى المركز عالقًا رغم أن BingX يثبت الحالة. الآن:
  - يُعاد اشتقاق كمية التعبئة الأصلية من سجل أمر الدخول على BingX عند فقدان الـcheckpoint
    (`ggp_recovery.reconcile_entry_fill`)، ولا تُخترع من `requested_qty`.
  - يُعتمد انخفاض الكمية المثبت من BingX (إغلاق جزئي قبل الـcheckpoint) كحقيقة، ويُشتق TP1 من
    سياسة 50/50 فلا يُنفّذ TP1 مرتين بعد Restart.
  - الزيادة غير المبرّرة في الكمية (لا يمكن أن تحدث لمركز أحادي Reduce-Only) تبقى Hold صريحًا.
- إضافة `TradeExecutionService.replace_native_stop` لتحريك أمر الوقف الأصلي فعليًا على BingX عبر
  `POST /openApi/swap/v1/trade/cancelReplace` (STOP_ON_FAILURE) مع read-back، وبديل Cancel+Place
  يرفض الإرسال عند غياب تأكيد الإلغاء.
- ربط Breakeven وTrailing Stop وTP1 بتحريك Native SL على المنصة بدل تحديث محلي فقط
  (`move_native_stop_on_exchange`). عند فشل التحريك يبقى المركز في RECOVERY_HOLD بدل ادّعاء حماية غير موجودة.
- تثبيت `entry_execution_state=FILLED` بعد دخول ناجح حتى لا يُعاد اشتقاق Hold زائف بعد Restart.
- إصلاح مساري الاختبار المعطوبين (`test_main_engine_compatibility`، `test_dual_scanner_dashboard`)
  ليكونا متوافقين مع المسار المسطّح أو مجلد tests/، وإضافة ملف `.env.example` المفقود.
- إضافة `test_recovery_lifecycle.py`: 35 سيناريو تكاملي (Restart، Native SL، Hedge/One-Way،
  Partial TP1، Timeout، تعديل SL، حدود التخصيص 20/30 وحد المخاطرة).
- الاختبارات المحلية: 180 passed؛ compilation وpreflight ناجحان.
- لم يجرِ اختبار اتصال فعلي على BingX Demo/LIVE أو اختبار تشغيل 24/7.

## ترقية 2026-11-09 — 20x / 30%-40% / Dynamic Profit Protection
- رفع سياسة الرافعة للدخول الجديد إلى 20x مع set/read-back verification.
- 20% هدف تخصيص هامش للفرص المتوسطة، و30% هدف للفرص القوية المعتمدة فقط.
- إبقاء حد مخاطرة الوقف 1% افتراضيًا؛ حجم الصفقة قد يكون أقل من سقف الهامش إذا فرضت مسافة الوقف ذلك.
- توسيع التحقق من Native SL ليشمل `triggerOrderId`، مع اشتراط حالة أمر نشطة ومطابقة الرمز والاتجاه والجانب والسعر والكمية.
- إضافة EMA20/EMA50 context من الشموع المغلقة، Peak ROE/Drawdown-from-peak، وAdaptive ATR Trail مستلهم من RORO إلى مدير GGP المركزي.
- الإبقاء على TP1 50% وTP2 للمتبقي وعلى بوابة التحقق من التعبئة.
- الاختبارات المحلية: 133 passed؛ compilation ناجح.
- لم يجرِ اختبار اتصال فعلي على BingX Demo/LIVE أو اختبار تشغيل 24/7.


## سياسة الوقف الثابت — تحديث 2026-11-11

وقف BingX الأصلي ثابت بعد الدخول. تم تعطيل مسار تحريك/استبدال Native SL بعد الدخول، بما في ذلك Breakeven وTP1 وTrailing. مستويات BE والتريلينج تُدار داخليًا فقط، والإغلاق الفعلي يمر عبر مسار reduce-only والتحقق الصارم. فشل إنشاء/التحقق من الوقف الأصلي يظل حالة حماية حرجة ولا يتم تجاهلها.

## 2026-11-28 — توحيد دورة EMA9 + Session VWAP الحية

- جعل Cross Event على شمعة مغلقة هو مدخل دورة الفرصة المستقلة، مع هوية ثابتة لا تعتمد على Ranking.
- حفظ التقاطعات المؤكدة حتى عند رفض ADX أو انتظار المنطقة؛ لا تختفي بسبب اختيار فرضية الاتجاه المعاكس.
- جعل التحليل المؤسسي تقييمًا لموقع الدخول فقط؛ Sweep/MSS/Retest ليست بوابات عامة لمسار Cross.
- اعتماد ADX >= 18 كشرط مطلق، وتوحيد طريقة DMI/ADX بين محلل المؤسسات ومحرك التنفيذ.
- إضافة تصنيف SCALP/TREND مثبت عند الدخول: TREND يحتاج مجموعة تأكيدات أقوى (ADX >= 25، Regime/DI/EMA9 slope/Volume متوافقة)، وإلا SCALP.
- إضافة أهداف SCALP 0.25% / 0.50%، مع 1.00% فقط عند متابعة استثنائية على شموع مغلقة؛ إغلاق SCALP كاملًا.
- الحفاظ على TREND TP1/TP2 مع 50% / 50%، وعلى رافعة 20x وتخصيص 20% القياسي لـCross.
- إبقاء `execute_entry()` و`LiveTradeManager` وNative SL وRecovery وFill Verification كسلطات تنفيذ وإدارة قائمة.
- الاختبارات المركزة بعد التعديلات: 150 passed، اختبار واحد مستبعد لاعتماده على `ggpbot` الذي تعذر استيراده في بيئة الفحص لغياب `ccxt`.
- مجموعة الاختبارات الكاملة لم تجتز في بيئة الفحص؛ يلزم إعادتها بعد تثبيت المتطلبات. لا يوجد اختبار اتصال BingX أو إثبات Live/Render ضمن هذا الإصدار.
