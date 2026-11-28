# تقرير تنفيذ معاينة دورة حياة HABIBA BOT — Demo

## القرار الهندسي
تم دمج معاينة دورة الحياة داخل تطبيق Flask الحالي على المسار `/lifecycle`، مع إنشاء محرك محاكاة مستقل. لم يتم إدخال ملفات React التي أرسلها أليكس؛ لأن الأرشيف المرجعي لا يحتوي على مشروع React أو إعدادات Vite/TypeScript، بينما نقطة التشغيل الفعلية في الأرشيف هي `ggpbot.py` وFlask. تم استخدام مكونات Watchlist وScanner المرسلة كمرجع وظيفي للتسميات والحقول والسلوك.

## فجوة النسخة المرجعية
1. `roro_opportunity_pipeline.py` يصف EMA9/VWAP proximity بأنه سياق عرض وترتيب فقط؛ وبالتالي لا يُنشئ دورة مستقلة لكل Cross Event.
2. `ggp_institutional_bridge.py` يحتوي بالفعل على حساب Session VWAP canonical و`cross_event_id` وبيانات التقاطع في مخرجات التحليل، لكن Watchlist الحية تُدار حول الرمز وتستخدم الترتيب والتدوير؛ هذا لا يساوي طابورًا دائمًا لكل حدث تقاطع.
3. متطلبات `Sweep → MSS → Retest` في `roro_institutional.py` تخص مسار Reversal. تصدير أسباب الانتظار هذه كمانع عام إلى كل الإعدادات سيحجب إعدادات Trend Continuation بلا مسوغ.

## المخرجات
- `habiba_demo.py`: محرك أحداث ومحاكاة مستقل، ببيانات اصطناعية فقط.
- `habiba_demo_web.py`: Blueprint خاص بواجهة Demo وواجهات التحكم والحالة.
- `static/habiba_lifecycle_demo.html`: لوحة RTL عربية كثيفة المعلومات تشمل Overview وScanner وWatchlist والتحليل المؤسسي والترخيص والتنفيذ والمراكز وسجل القرارات وتقرير الإصلاح.
- `.atoms/ATOMS.md` و`.atoms/PROGRESS.md`: سياق المشروع وسجل التقدم. لم يتضمن الأرشيف قوالب `.atoms` سابقة؛ لذلك أُنشئت هذه الملفات في جذر نسخة العمل بدل الادعاء بتحديث قوالب غير موجودة.
- `test_habiba_demo_lifecycle.py`: اختبارات سيناريوهات ADX، المناطق، B-specific waiting، SL، منع التكرار، مراحل التنفيذ، TP1/TP2، ومصالحة Demo.

## ضوابط المحاكاة
- جميع الأوامر ذات بادئة `SIM-` وجميع المراكز تحمل المصدر `DEMO_SIMULATION_ONLY`.
- لا يستورد `habiba_demo.py` مكتبة CCXT ولا يقرأ `STATE` أو `MEMORY` الحي.
- لا يوجد مسار من محرك Demo إلى `execute_entry()` أو `TradeExecutionService` أو حماية BingX.
- لا يظهر READY إلا إذا نجحت بوابات A العشر؛ BASE ADX >= 18 شرط مطلق.
- الشروط الستة `closed_mss_pending` و`post_liquidity_window_not_ready` و`post_sweep_window_not_ready` و`valid_sweep_or_failed_auction_not_confirmed` و`post_mss_retest_pending` و`no_fresh_nearby_order_block_or_fvg` معروضة ضمن B كمتطلبات مرتبطة بنوع الإعداد، وليست موانع عامة.

## التحقق
- الاختبارات المركزة القديمة + الجديدة: **69 passed, 1 skipped**.
- الاختبار المتخطى: اختبار Flask Blueprint؛ لم تكن مكتبة Flask مثبتة في بيئة الفحص الحالية رغم إعلانها في `requirements.txt`.
- تشغيل المجموعة كاملة توقف أثناء collection في 6 ملفات تعتمد على `ggpbot.py` بسبب غياب `ccxt` من بيئة الفحص. لم تصل المجموعة إلى مرحلة assertion tests؛ لذلك لا يصح وصف النتيجة بأنها نجاح كامل أو فشل وظيفي.
- `compileall` نجح لملفات Python، وHTML parser قرأ الواجهة، و`node --check` اجتاز JavaScript المضمن.
- اجتاز محرك Demo اختبار soak لمدة 120 خطوة (121 cycle index مع دورة التهيئة الأولى) مع التحقق من ثبات Event ID، وعدم READY دون الترخيص، وعدم فتح مركز دون SL والتعبئة والحماية.

## الملفات الحية المحمية
لم تُعدّل استراتيجية الدخول الفعلية أو `execute_entry()` أو إدارة المراكز الحية أو تنفيذ BingX أو Native SL أو Recovery أو Sizing / Leverage. هذا تنفيذ معاينة Demo وليس اعتمادًا لتغيير سلوك التداول الحقيقي.

## التشغيل
بعد تثبيت `pip install -r requirements.txt` وتشغيل `python ggpbot.py`، افتح `/lifecycle`. الصفحة تحتوي على زر تشغيل/إيقاف وخطوة يدوية وإعادة توليد وفرض دورة كاملة. المسار `/` يحتوي على رابط مباشر إلى المعاينة.
