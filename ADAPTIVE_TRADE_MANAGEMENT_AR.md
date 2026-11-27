# Adaptive Scalp / Trend / Standard Trade Management — تقرير هندسي

الفرع: `main`<br>
الأساس: `origin/main` عند `6b993c17b07703f99a8f63c2f5f3d8da5078e939`<br>
آخر commit قبل التوصية C: `d742f6cb69f2c036953567491c3bccb626071c6c`<br>
نقطة الرجوع المحلية: `backup/pre-rec-c-d742f6c`<br>
لا مفاتيح حقيقية، لا أوامر حية، لا استدعاءات BingX مصادّق عليها، ولا تداول فعلي أثناء هذا العمل.

---

## 1. القرار المعماري للتوصية C

تم اعتماد نظام Adaptive SCALP/TREND كجهة إدارة واحدة، مع دمج أدلة الإرث داخل نفس المسار:

- `TradeManagementBoard.evaluate()` هو الجهة الوحيدة لقرارات `HOLD` / `PARTIAL_CLOSE` / `FULL_CLOSE`.
- لا يوجد Profit Engine موازٍ، ولا Exit Brain مستقل، ولا TP Engine منفصل، ولا SCALP Execution Engine جديد.
- كل الإغلاقات تمر عبر `_verified_market_reduce()` → `_verified_market_reduce_impl()` مع قراءة ثلاثية الحالة، قفل `_CLOSE_ORDER_LOCK`، `reduceOnly` في One-Way، والتحقق من فرق الكمية الفعلي على المنصة.
- `profit_mode` تشخيصي/للتوافق فقط؛ `trade_regime_state` هو الحالة الأساسية.
- قبل أول تصنيف من شمعة مغلقة: `profit_mode=STANDARD_MODE` و`trade_regime_state=None`؛ الافتراضي هو خطة STANDARD الأصلية 50/50، وليس SCALP.
- نقص الأدلة لا يُهبط بـSTANDARD إلى SCALP؛ ترقية TREND تتطلب أدلة استمرار مؤكدة من شموم مغلقة.

---

## 2. الملفات المعدّلة

- `ggp_profit_brain.py`
- `ggp_recovery.py`
- `ggpbot.py`
- `test_adaptive_lifecycle_integration.py`
- `test_adaptive_scalp_trend_management.py`
- `test_position_sizing_policy.py`
- `test_recovery_lifecycle.py`
- `test_market_data_refresh.py`
- `ADAPTIVE_TRADE_MANAGEMENT_AR.md`

---

## 3. سياسة الأنظمة

| النظام | المصدر | الهدف | التنفيذ |
|---|---|---|---|
| `SCALP_STATE` | أدلة نطاق/ضعف مؤكدة | هدف واحد فقط من 5% أو 10% أو 20% ROE | `FULL_CLOSE` عند بلوغ الهدف المختار |
| `TREND_STATE` | استمرار/بنية/ADX/DI/حجم مؤكدة | TP1 عند 40% ROE، TP2 عند 60% ROE | `PARTIAL_CLOSE` إلى 50% الكمية الأصلية، ثم `FULL_CLOSE` للباقي |
| `STANDARD_STATE` | أدلة غير كافية أو حالة سابقة محفوظة | TP1/TP2 بالأسعار الأصلية | نفس تقسيم 50/50 الأصلي، بدون أهداف ROE |
| `EXIT_REQUIRED` | انهيار فرضية أو إغلاق مؤسسي | خروج إجباري | `FULL_CLOSE` عبر نفس Board والتحقق |

- لا TP3.
- TP2 لا يُسمح قبل إثبات TP1.
- الربح وحده لا يرقّي الصفقة إلى TREND.
- `BEHAVIOR_INSUFFICIENT` لم يعد يُستخدم كدليل SCALP.

---

## 4. التحقق من ROE

- الصيغة: `ROE = (unrealized_pnl - fees - funding) / initial_margin * 100`.
- تُستخدم الرافعة الفعلية الموثّقة من المنصة فقط؛ لا تُفترض 20x.
- تُتحقق الكمية الفعلية، سعر الدخول، سعر المارك، الهامش، الجانب، وPnL.
- يُقارن ROE المحسوب بـROE المعلن من المنصة عند توفره.
- أي تعارض، غياب رافعة موثّقة، أو أساس غير قابل للتحقق ينتج `HOLD + DIAGNOSTIC`.
- `synthetic_sl` / BE / trailing تبقى داخلية فقط؛ الوقف الأصلي على المنصة لا يُعدّل بعد الدخول.

---

## 5. أولوية القرار داخل Board

1. Exchange / Position Safety و`execution_unresolved` / Recovery Hold.
2. Emergency Exit.
3. Thesis Failure / Structural Invalidation.
4. Institutional Exit (`EXIT_REQUIRED`).
5. Hard Exit States.
6. TP / Profit Management.
7. Trailing / HOLD.

---

## 6. الاستعادة بعد إعادة التشغيل

- `ggp_recovery.py` يحفظ ويستعيد `trade_regime_state`، `profit_mode` التشخيصي، `original_tp1_price`، `original_tp2_price`، أهداف SCALP/TREND ROE، وحقول التحقق/الرسوم/التمويل.
- الاستعادة لا تُعيد ضبط `remaining_qty` من ذاكرة محلية إذا كانت المنصة تعطي كمية مختلفة؛ كمية المنصة هي المرجع بعد التحقق.
- UNKNOWN لا يُعامل كـFLAT أبداً؛ يبقى التعرض محلياً وتُمنع الدخول الجديدة.
- أوامر الإغلاق المعلّقة تُحل فقط بقراءة نهائية للأمر + فرق كمية فعلي من المنصة.

---

## 7. بوابة الدخول

- لم يُلمس منطق EMA9 + Session VWAP ranking؛ دوره ترتيبي فقط وليس بوابة دخول مستقلة.
- كل تحليل الدخول يستخدم شموم مغلقة فقط عبر `roro_validate_closed_frame()`.
- أُضيفت بوابة صارمة `ADX >= 18` على مسار التنفيذ النهائي؛ القيمة المفقودة أو NaN تُرفض.
- Entry Authorization، Position Sizing، Leverage Configuration، Exchange Execution، ومزامنة الوقف الأصلي لم تُغيّر.
- لا توجد أوامر TP أصلية على المنصة عند الدخول؛ المستويات تُحفظ في حالة الإدارة/السجل ويُنفّذها Board فقط. لا يُادّعى وجود server-side TP مُتحقق منه.

---

## 8. توحيد نهاية الملف

تم دمج monkey-patches الموجودة في نهاية `ggpbot.py` داخل تعريفاتها الأصلية:

- `fetch_position`
- `ExchangeSyncService.fetch_live_snapshot`
- `ExchangeSyncService.reconcile`
- `sync_position_state`
- `sync_all_states`
- `execute_entry`
- `LiveTradeManager.manage_live_trade`
- `main_loop_sniper` / `main_loop`

الواجهات والسلوك ABI لم تتغير، بما في ذلك emergency-first وtri-state strings وreconciliation lock.

---

## 9. نتائج الاختبارات الفعلية

| الفحص | الأساس قبل التوصية C | بعد التوصية C |
|---|---:|---:|
| `python -m pytest -q` | 339 passed / 7 failed | **372 passed** |
| `python preflight.py` | PREFLIGHT OK | PREFLIGHT OK |
| `python -m compileall -q .` | exit 0 | exit 0 |
| `git diff --check` | — | نظيف (تحذيرات EOL فقط) |

الفشل السبعة في الأساس كان لها سببان معروفان: 4 اختبارات sizing افترضت أن قيد التخصيص هو الملزم بينما قيد المخاطرة كان الملزم فعلاً، و3 اختبارات وقف أصلية توقعت نقل STOP_MARKET بعد الدخول وهو محظور بpolicy. تم تصحيح الاختبارات لتعكس السلوك الصحيح دون تعطيلها أو إضعاف assertions.

بعد المراجعة المستقلة أُغلقت ملاحظاتها دون تغييرات معمارية: توحيد قفل reconciliation على نفس الكائن، فرض بوابة ADX عند حد التنفيذ المباشر، إزالة استدعاء journal مكرر، إثبات بقاء تعارض الحالة على HOLD مع تشخيص مع بقاء الخروج الطارئ أعلى أولوية، وتأكيد قيد التخصيص صراحةً في اختبارات sizing.

---

## 10. القيود

- لا توجد مصالحة حية مع BingX؛ كل التحقق من المنصة تم بمحاكاة Unit/Integration/Mock.
- لا توجد بيانات OHLCV/tick حقيقية ولا backtest؛ لا توجد ادعاءات بتفوق Net PnL أو Drawdown أو Profit Factor أو Win Rate أو MAE/MFE.
- لم يُثبت وجود server-side TP عند الدخول؛ الإغلاق الربحي يظل داخل Board ومسار التحقق نفسه.
- Push / Merge / PR / تغيير `origin/main` / إعادة كتابة التاريخ / التداول الحي كلها تتطلب موافقة صريحة منفصلة.
