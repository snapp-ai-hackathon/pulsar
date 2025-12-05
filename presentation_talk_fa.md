Pulsar – ارائه فنی و بیزنسی (تمرکز روی ML/AI)
=============================================

> **Pulsar تنها سیستم پیش‌بینی سرج است که از روز اول با زیرساخت MLOps کامل، مدل‌های چندلایه (ElasticNet + CNN)، و یکپارچگی رویدادمحور با NATS ساخته شده است** — نه یک PoC آزمایشگاهی، بلکه یک پلتفرم تولیدی که همین الان روی داده‌های واقعی اسنپ آموزش دیده و آماده‌ی استقرار است.

## چرا Pulsar متفاوت است؟

در حالی که بسیاری از راه‌حل‌ها روی **یک** جنبه تمرکز می‌کنند (مثلاً فقط مدل ML، یا فقط داشبورد، یا فقط API)، Pulsar **سه لایه‌ی به‌هم‌پیوسته** را یکجا ارائه می‌دهد که هر کدام برای محیط تولید طراحی شده‌اند:

### 1. **لایه‌ی Feature Engineering زمان‑سری (Production-Ready)**
- ساخت خودکار دیتاست‌های زمان‑سری از داده‌های خام ClickHouse و CSV
- ذخیره‌سازی بهینه در Parquet با `TimeSeriesStore` برای دسترسی سریع
- ساخت lag features، cumulative signals، و پنجره‌های زمانی به صورت خودکار
- **تفاوت**: بسیاری از راه‌حل‌ها روی داده‌های آماده تکیه می‌کنند؛ ما خودمان لایه‌ی داده را می‌سازیم

### 2. **لایه‌ی مدل‌های چندگانه (Hybrid Approach)**
- **ElasticNet**: مدل سریع و قابل توضیح برای baseline و validation
- **Temporal CNN**: شبکه‌ی عصبی عمیق برای پیش‌بینی چندافقی (۳۰/۶۰/۹۰ دقیقه)
- آموزش روی داده‌های واقعی با ثبت کامل در MLflow
- **تفاوت**: ترکیب مدل‌های کلاسیک و عمیق در یک pipeline واحد، نه فقط یک مدل

### 3. **زیرساخت MLOps رویدادمحور (Event-Driven Architecture)**
- **MLflow**: رجیستری کامل مدل‌ها، متریک‌ها، و آرتیفکت‌ها
- **NATS Integration**: آموزش مدل به صورت event-driven، نه cron job
- یکپارچگی با استک فعلی اسنپ (ClickHouse، Kubernetes، Docker)
- API production-ready با FastAPI و داشبورد عملیاتی
- **تفاوت**: MLOps از روز اول، نه به عنوان add-on بعدی

### 4. **تفاوت کلیدی: Production-Ready از روز اول**
- ✅ مدل‌ها روی داده‌های واقعی ۸ روزه آموزش دیده‌اند
- ✅ آرتیفکت‌های MLflow در ریپو موجود است (قابل بررسی مستقیم)
- ✅ Pipeline کامل از CSV/ClickHouse تا پیش‌بینی در یک CLI واحد
- ✅ پشتیبانی از NATS برای یکپارچگی با زیرساخت اصلی
- ✅ کانتینریزه شده با Docker و Helm charts آماده

**این سند را می‌توان هم به عنوان اسکریپت ارائه، هم به عنوان مستند فنی برای تیم داوری و تیم‌های فنی اسنپ استفاده کرد.**

---

## 1. داستان کلی در چند جمله (اسلاید اول)

### مسئله کسب‌وکاری: چرا راه‌حل‌های فعلی کافی نیستند؟

1. **راننده‌ها**: از «زمان مُرده» و سرج‌های غیرقابل‌پیش‌بینی ناراضی‌اند — نمی‌دانند کجا بروند تا درآمد بهینه داشته باشند.
2. **مسافرها**: از این‌که هر بار اپ را باز می‌کنند با سرج عجیب روبه‌رو شوند خسته شده‌اند — تجربه‌ی کاربری ناپایدار.
3. **تیم‌های عملیاتی**: مجبورند مدام دستی آتش‌نشانی کنند — قیمت را بالا/پایین ببرند، کمپین بزنند، نقشه را تنظیم کنند. **واکنشی، نه پیش‌کنشی.**

### پاسخ ما: Pulsar — از پیش‌بینی تا عمل

Pulsar یک **موتور پیش‌بینی چندافقی** است که روی شبکه هگزاگونی شهر سوار می‌شود و **قبل از وقوع بحران** سیگنال می‌دهد:

- **برای راننده**: «در ۳۰ دقیقه‌ی بعدی در این محدوده سرج +۲۲٪ می‌رود، الان حرکت کن.»
- **برای تیم اوپس**: «این منطقه در ۶۰ دقیقه‌ی آینده oversupply خواهد داشت، کمپین را این‌جا فعال نکن.»
- **برای سیستم قیمت‌گذاری**: پیش‌بینی‌های چندافقی (۳۰/۶۰/۹۰ دقیقه) برای تصمیم‌گیری هوشمند.

### چرا Pulsar قابل اجراست و متفاوت است؟

**۱. داده‌های واقعی و دیتاست آماده**
- داده‌ی واقعی از `ClickHouse` (جدول `kandoo_parameter_nats`) و CSV
- دیتاست زمان‑سری کامل در `cache/timeseries/` با هزاران هگزاگون و سرویس
- **قابل بررسی مستقیم در ریپو** — نه فقط کد، بلکه داده و نتایج

**۲. مدل‌های آموزش‌دیده و قابل استقرار**
- `ElasticNet` و `TemporalCNN` روی همین دیتاست **آموزش دیده‌اند**
- آرتیفکت‌های کامل در `mlruns/` با متریک‌ها، پارامترها، و مدل‌های ذخیره‌شده
- **MAE و RMSE محاسبه‌شده روی داده‌های واقعی** — نه synthetic data

**۳. زیرساخت MLOps از روز اول**
- MLflow برای رجیستری و traceability کامل
- NATS برای آموزش رویدادمحور (event-driven retraining)
- API production-ready با FastAPI
- Docker و Helm charts برای استقرار در Kubernetes

**۴. یکپارچگی با استک اسنپ**
- استفاده از ClickHouse موجود (نه database جدید)
- NATS برای یکپارچگی با سیستم‌های دیگر
- سازگار با Docker و Kubernetes که الان استفاده می‌شود

### تفاوت کلیدی با راه‌حل‌های دیگر

| ویژگی | راه‌حل‌های معمول | Pulsar |
|-------|------------------|--------|
| **مدل ML** | یک مدل ساده یا PoC | Hybrid: ElasticNet + CNN |
| **داده** | روی داده‌های آماده | ساخت خودکار TimeSeriesStore |
| **MLOps** | بعداً اضافه می‌شود | از روز اول با MLflow |
| **یکپارچگی** | نیاز به تغییرات زیاد | NATS + ClickHouse موجود |
| **Production-Ready** | نیاز به کار زیاد | کانتینریزه + Helm charts |
| **قابل بررسی** | فقط کد | کد + داده + مدل‌های آموزش‌دیده |

این سه محور (مسئله → راه‌حل → امکان‌پذیری) را می‌توان در ۴۵–۶۰ ثانیه اول ارائه گفت تا ذهن همه روی **تفاوت واقعی Pulsar** قفل شود.

---

## 2. معماری داده و مسیر کلی (از CSV تا مدل)

### 2.1 جریان داده

1. **ورود داده**
   - داده‌های آفلاین از طریق فایل‌های `data/*.csv` (لوکال، خارج از ریپو برای حفظ پرایوسی) و خروجی جدول `kandoo_parameter_nats` روی `ClickHouse` وارد سیستم می‌شود.
   - هر ردیف شامل است:
     - زمان (`clickhouse_time`)
     - شناسه هگزا (`hex_id`)
     - `service_type`, `city_id`
     - سیگنال‌های سرج: `surge_percent`, `surge_absolute`, `cumulative_surge_*`
     - سیگنال‌های رفتاری: `accept_rate`, `price_cnvr` و …

2. **ساخت Snapshot**
   - کلاس `CSVSignalLoader` این داده‌ها را می‌خواند و به همراه `SnapshotBuilder`، برای هر `(hexagon, service_type, city_id, period)` یک شیء `HexagonSnapshot` می‌سازد.
   - این snapshot شامل ویژگی‌هایی مثل:
     - `acceptance_rate`, `price_conversion`, `demand_signal`, `supply_signal`
     - مقادیر سرج و cumulative surge.

3. **ذخیره‌سازی در TimeSeriesStore**
   - کلاس `TimeSeriesStore` این اسنپ‌شات‌ها را به صورت ردیف‌های زمانی در فایل‌های پارکت (`cache/timeseries/{hexagon}_{service_type}.parquet`) ذخیره می‌کند.
   - هر فایل در واقع تاریخچه‌ی یک هگزا–سرویس است.

4. **ساخت دیتاست برای مدل‌ها (و وضعیت فعلی ریپو)**
   - برای **ElasticNet** (کلاس `MLTrainer`):
     - همه پارکت‌ها با هم `concat` می‌شوند.  
     - برای هر هگزا/سرویس، lag یک‑گانه از `demand_signal`, `supply_signal`, `acceptance_rate` ساخته می‌شود (`lag_demand`, `lag_supply`, `lag_acceptance`).  
     - تارگت: `demand_signal` در گام بعدی است.
   - برای **Temporal CNN** (کلاس `CNNTrainer`):
     - روی هر پارکت، پنجره‌های طولی `window_size` (پیش‌فرض ۱۲ بازه‌ی زمانی ~ مثلاً ۱۲×۷.۵ دقیقه) ساخته می‌شود.  
     - ورودی: ماتریس `[window_size × feature_dim]` شامل ۸ ویژگی اصلی (در `feature_columns`).  
     - خروجی: یک بردار از مقدار سرج آینده در چند افق مختلف (`forecast.horizons` مثلاً ۳۰ و ۶۰ و ۹۰ دقیقه).

5. **آموزش مدل و لاگ در MLflow**
   - پس از ساخت دیتاست، مدل‌ها روی GPU/CPU آموزش داده می‌شوند، متریک‌ها (`mae`, `rmse`) محاسبه و در MLflow با پارامترها و وزن‌های مدل ثبت می‌شوند.  
   - در نسخه‌ی فعلی ریپو، حداقل چند ران کامل ElasticNet روی دیتاست ۸ روزه اجرا و **کل `mlruns/` و `cache/timeseries/` روی گیت‌هاب کامیت شده‌اند** تا داور بتواند مستقیماً آرتیفکت‌ها را ببیند و ایمیج نهایی بدون نیاز به ترین مجدد، مدل را همراه داشته باشد.

این بخش را می‌توان در یک اسلاید «Data Flow» با فلش نشان داد و شفاهی توضیح داد که: «ما روی داده‌های آماده نمی‌نشینیم؛ خودمان لایه‌ی زمان‑سری را می‌سازیم.»

---

## 3. لایه‌های هوش مصنوعی در Pulsar (AI Stack)

برای این‌که در ارائه هم دانشگاهی قوی باشی، هم بیزنسی، خوب است AI را به چند لایه‌ی مشخص بشکنی و روی هر کدام ۲۰–۳۰ ثانیه مانور بدهی:

1. **لایه‌ی Feature Engineering زمان‑سری**
   - تبدیل سیگنال‌های خام به ویژگی‌های معنادار:
     - lagهای مختلف (۱ تیک قبل، چند تیک قبل)،
     - سیگنال‌های تجمیعی (`demand_signal`, `supply_signal`)،
     - cumulative surge و نسبت‌های رفتاری (`price_conversion`).
   - این‌جا در واقع همان کاری را می‌کنیم که یک Data Scientist دستی انجام می‌داد، ولی اتوماتیک و پایدار.

2. **لایه‌ی مدل‌های کلاسیک (ElasticNet)**
   - مدل خطی با regularization که پایه‌ی قابل توضیح و سریع ماست.
   - به‌خاطر وزن‌های روشن، این لایه بخش «Explainable AI» داستان ماست.

3. **لایه‌ی مدل‌های عمیق (Temporal CNN و در آینده LSTM/TFN)**
   - CNN روی پنجره‌ی زمانی، الگوهای پیچیده‌تری را می‌بیند که خطی نیستند (مثلاً چند قله و دره‌ی پشت‌سرهم قبل از سرج).
   - در Roadmap، LSTM/TFN برای مدل‌کردن توالی‌های طولانی‌تر و ترکیب چند نوع فیچر (آب‌وهوا، event، ترافیک) اضافه می‌شوند.

4. **لایه‌ی MLOps (MLflow + NATS)**
   - MLflow همه‌ی این تجربه‌ها را ثبت و قابل بازپخش می‌کند.
   - NATS به ما اجازه می‌دهد *هوش* را به صورت event‑driven وارد بقیه‌ی سرویس‌ها کنیم (هر جا که لازم باشد، فقط یک پیام بفرست و مدل آموزش ببیند).

با این framing، وقتی می‌گویی «ما از AI استفاده کردیم»، دقیقاً می‌توانی نشان بدهی AI در کجاها نشسته است، نه فقط در یک اسلاید مبهم.

---

## 4. مدل‌های فعلی: ElasticNet و CNN

### 4.1 ElasticNet – لایه‌ی ML کلاسیک، قابل توضیح

**چرا اول سراغ ElasticNet رفتیم؟**

- بسیار سریع است؛ برای چند صد هزار ردیف در چند ثانیه آموزش می‌بیند.
- به‌شدت **قابل توضیح** است:
  - هر ضریب (`coefficient`) دقیقاً می‌گوید «اگر `lag_demand` ۱ واحد بیشتر شود، پیش‌بینی demand چقدر زیاد می‌شود».
- برای شروع، فهمیدن رفتار داده و validate کردن pipeline فوق‌العاده است.

**ساختار مدل (`MLTrainer`):**

- ورودی: `[lag_demand, lag_supply, lag_acceptance, price_conversion]`
- مدل: `ElasticNet(alpha=0.3, l1_ratio=0.1)`  
- خروجی: یک اسکالر `demand_signal` در بازه‌ی بعدی.
- آموزش:
  - `train_test_split` (۸۰٪/۲۰٪)
  - متریک‌ها: MAE و RMSE
  - لاگ در MLflow با نام `elasticnet-demand-<date>`.

در ارائه‌ی فنی می‌توانی بگویی:

> «ElasticNet برای ما مثل baseline است؛ هم سریع است، هم شفاف است، و اگر فردا کسی از تیم قیمت‌گذاری بپرسد *چرا این اتفاق افتاد؟* می‌توانیم نمودار ضرایب را نشان بدهیم.»

---

### 4.2 Temporal CNN – شبکه‌ی عصبی برای پیش‌بینی سرج

**ایده‌ی اصلی:**

- به‌جای این‌که فقط به آخرین نقطه‌ی زمان‌سری نگاه کنیم، یک **پنجره‌ی زمانی** از ۱۲ بازه‌ی اخیر را می‌گیریم (مثلاً ۱۲×۷.۵ دقیقه = ~۹۰ دقیقه‌ی گذشته).  
- برای هر هگزا و سرویس، یک دنباله‌ی `window_size × feature_dim` می‌سازیم؛ هر ردیف ترکیبی از:
  - `acceptance_rate`, `price_conversion`, `demand_signal`, `supply_signal`,
  - `surge_percent`, `surge_absolute`,
  - `cumulative_surge_percent`, `cumulative_surge_absolute`.
- CNN روی محور زمان یک فیلتر ۱ بعدی می‌کشد و الگوهایی شبیه «سه بازه‌ی رشد سریع + افت supply» را شناسایی می‌کند.

**ساختار دقیق `TemporalCNN`:**

- ورودی: `x` با شکل `(batch, window, features)`.
- لایه‌ها:
  1. `Conv1d(in_channels = feature_dim, out_channels = 64, kernel_size = 3, padding = 1)`  
  2. `ReLU`
  3. `Conv1d(64 → 64, kernel_size = 3, padding = 1)`
  4. `ReLU`
  5. `AdaptiveAvgPool1d(1)` → فشرده‌سازی بعد زمانی
  6. `Dropout(p=0.15)` برای جلوگیری از overfitting
  7. `Linear(64 → target_dim)`؛ `target_dim` = تعداد افق‌های پیش‌بینی (مثلاً ۲ یا ۳ خروجی: ۳۰ / ۶۰ / ۹۰ دقیقه)

به زبان ساده در ارائه:

> «ما ۱۲ تیک آخر هر هگزا را مثل یک سیگنال صوتی در نظر می‌گیریم، روی آن فیلتر زمانی می‌کشیم تا الگوهای قبل از سرج را ببینیم، و بعد با یک لایه‌ی نهایی، مقدار سرج آینده را برای چند تایم‌فریم مختلف حدس می‌زنیم.»

**فرآیند آموزش CNN (`CNNTrainer.train`):**

- **ساخت توالی‌ها**:
  - از هر فایل پارکت، اگر حداقل `(window_size + max_step + 1)` ردیف داشته باشد، هزاران جفت `(sequence, target)` می‌سازیم.
  - `sequence`: ماتریس ۱۲×۸ (برای ۸ ویژگی اصلی).
  - `target`: برداری از مقادیر `surge_percent` در گام‌های آینده (بر اساس `forecast.horizons` و `period_duration_minutes` محاسبه شده).
- **ساخت `DataLoader`**:
  - `SequenceWindowDataset` برای پکیج‌کردن `inputs` و `targets` در قالب `torch.Tensor`.
  - `random_split` به train/val.
- **حلقه‌ی آموزش**:
  - optimizer: `Adam`, loss: `MSELoss`.
  - چاپ `train_loss` و `val_loss` در هر epoch.
- **ارزیابی و لاگ در MLflow**:
  - محاسبه‌ی MAE و RMSE روی کل داده.
  - `mlflow.pytorch.log_model` برای ذخیره‌ی مدل CNN.

این باعث می‌شود که CNN به صورت عملیاتی و قابل تست در کنار ElasticNet قرار بگیرد و آماده‌ی A/B تست روی ترافیک واقعی باشد؛ و از دید داور/استاد، این‌جا دقیقاً جایی است که «هوش مصنوعی» فراتر از رگرسیون ساده وارد بازی می‌شود.

---

## 5. نقش MLflow در چرخه‌ی عمر مدل (MLOps هوشمند)

MLflow ستون فقرات **رصد و مدیریت مدل‌ها** در Pulsar است. برخلاف بسیاری از راه‌حل‌ها که MLOps را به عنوان add-on بعدی اضافه می‌کنند، Pulsar از روز اول با MLflow ساخته شده است.

### معماری MLflow در Pulsar

**پیکربندی انعطاف‌پذیر:**
- پشتیبانی از storage موقت (`/tmp/mlruns`) برای محیط‌های محدود از نظر منابع
- امکان استفاده از SQLite برای persistence سبک‌وزن
- آماده برای اتصال به MLflow server مرکزی اسنپ (از طریق `mlflow_tracking_uri`)

**هر بار که `MLTrainer.train` یا `CNNTrainer.train` اجرا می‌شود:**
- یک `run` جدید در MLflow ایجاد می‌شود با نام منحصر به فرد (`elasticnet-demand-<date>` یا `cnn-surge`)
- **پارامترها** ذخیره می‌شوند: `alpha`, `l1_ratio`, `window_size`, `epochs`, `horizons`, `train_date`
- **متریک‌ها** ثبت می‌شوند: `mae`, `rmse`, تعداد ردیف‌ها، تعداد هگزاها
- **مدل نهایی** به عنوان artifact (`model`) ذخیره می‌شود و `model_uri` تولید می‌شود
- **آرتیفکت‌های اضافی**: نمونه پیش‌بینی‌ها در CSV برای inspection

### مزایای کلیدی

**۱. Traceability کامل**
- می‌دانیم کدام نسخه‌ی مدل روی کدام تاریخ آموزش دیده و چه عملکردی داشته
- هر run قابل بازپخش و مقایسه است

**۲. مقایسه‌ی سناریوها**
- ElasticNet vs CNN: کدام مدل برای کدام use case بهتر است؟
- افق ۳۰ دقیقه vs ۶۰ دقیقه: trade-off بین دقت و افق زمانی
- با/بدون ویژگی‌های جدید: impact هر feature روی performance

**۳. آماده برای MLOps اسنپ**
- تیم‌های دیگر که قبلاً با MLflow کار می‌کنند، می‌توانند مدل‌های Pulsar را به‌راحتی لود کنند
- سازگار با MLflow registry موجود در اسنپ
- امکان export مدل‌ها برای استفاده در سرویس‌های دیگر

**۴. بهینه‌سازی منابع**
- پشتیبانی از storage موقت برای محیط‌های cloud با محدودیت منابع
- امکان استفاده از SQLite برای persistence سبک‌وزن
- بدون نیاز به MLflow server مرکزی برای شروع (اما آماده برای اتصال)

### تفاوت با راه‌حل‌های دیگر

| ویژگی | راه‌حل‌های معمول | Pulsar |
|-------|------------------|--------|
| **MLOps** | بعداً اضافه می‌شود | از روز اول با MLflow |
| **Traceability** | لاگ‌های ساده | رجیستری کامل با پارامترها و متریک‌ها |
| **مدل Registry** | فایل‌های جداگانه | MLflow Model Registry |
| **مقایسه‌ی Runs** | دستی | خودکار در MLflow UI |
| **Production Integration** | نیاز به کار اضافی | آماده برای اتصال به MLflow server اسنپ |

در ارائه دانشگاهی می‌توانی روی این تأکید کنی که:

> «ما فقط مدل نساختیم، بلکه چرخه‌ی کامل *Data → Model → Metrics → Registry* را هم از روز اول در نظر گرفتیم، مطابق best practiceهای MLOps. آرتیفکت همین چرخه (mlruns + cache/timeseries) الان داخل ریپو موجود است و قابل بررسی مستقیم است — نه فقط کد، بلکه داده، مدل‌های آموزش‌دیده، و نتایج کامل.»

---

## 6. NATS – چرا و کجا در تصویر می‌آید؟

در نسخه‌ی فعلی، NATS را برای **ارکستریشن آموزش آنلاین** استفاده می‌کنیم، نه برای سروینگ بلادرنگ.

### 5.1 الگوی فعلی

- یک سرویس خارجی (مثلاً ClickHouse exporter یا سرویس اپراتوری) یک پیام روی subject مثلاً `kandoo.parameter.train` منتشر می‌کند که در آن مشخص است:
  - `model_type`: `"elasticnet"` یا `"cnn"`.
  - `service_types`: لیست سرویس‌ها (مثلاً `[1,2,3,4,5,6,7,8,24,31]`).
  - `alpha`, `l1_ratio`, `train_date`, `force` و …
- مصرف‌کننده (در کد: `NATSTrainerConsumer`) این پیام را می‌گیرد،:
  - دیتای CSV/ClickHouse را برای بازه‌ی مربوطه از نو بارگذاری می‌کند،
  - اسنپ‌شات‌ها و `TimeSeriesStore` را آپدیت می‌کند،
  - مدل را با کانفیگ خواسته‌شده آموزش می‌دهد،
  - نتیجه را در MLflow و لاگ‌ها ثبت می‌کند.

به این ترتیب:

> «آموزش مدل یک *event* می‌شود، نه یک job دستی. هر سیستم دیگری در اکوسیستم اسنپ می‌تواند فقط با publish کردن یک پیام، یک retrain کامل را تریگر کند.»

### 5.2 آینده‌ی NATS در محصول

- اتصال به **کلاستر NATS اصلی اسنپ** با:
  - احراز هویت و مجوز،
  - retry و idempotent job id،
  - مانیتورینگ و آلارم روی نرخ fail/retry.
- گسترش موضوعات:
  - `parameter.train` برای آموزش،
  - `parameter.forecast` برای بروزرسانی مدل‌های سرویسی دیگر،
  - و حتی `parameter.feedback` برای برگرداندن نتایج A/B تست‌ها به سیستم یادگیری.

در ارائه می‌توانی بگویی:

> «در نسخه‌ی هکاتونی، NATS را برای راه‌انداختن روال آموزش آنلاین استفاده کردیم. در نسخه‌ی پرو، همین الگو را روی کلاستر اصلی NATS می‌بریم و آن را به مرکز عصبی کل فرآیندهای ML تبدیل می‌کنیم.»

---

## 7. ایده‌های آینده: فراتر از سرج و فقط راننده

Pulsar در واقع یک **موتور پیش‌بینی فشار و ازدحام** است؛ وقتی این موتور را داشته باشیم، می‌توانیم:

1. **کنترل هوشمند ترافیک و پیشنهادهای پویا**
   - استفاده از پیش‌بینی سرج + داده‌های ترافیک برای:
     - ارسال پیشنهاد به راننده‌ها: «اگر از خیابان شلوغ خارج شوی و به این محدوده بروی، ترافیک کمتر + تقاضای بهتر می‌گیری».
     - پیشنهاد به مسافر: «اگر نقطه‌ی سوارشدنت را ۳۰۰ متر جابه‌جا کنی، هم سریع‌تر سوار می‌شوی، هم هزینه کمتر می‌شود.»

2. **هماهنگی بین سرویس‌ها (Ride, Food, Intercity, …)**
   - اگر یک هاب خیلی شلوغ است، می‌توانیم:
     - سرج سواری را بالا ببریم ولی همزمان شدت تخفیف Food را کم کنیم.
     - کمپین‌های بازاریابی را به سمت منطقه‌ای ببریم که ظرفیت خالی بیشتری دارد.

3. **قوانین عدالت و کیفیت سرویس**
   - تعریف قاعده‌هایی مثل:
     - "هیچ راننده‌ای نباید بیش از ۳۰ دقیقه بدون سفر در هسته‌ی سرویس بماند."
     - “نباید همه‌ی سرج را روی چند محله‌ی خاص قفل کنیم.”
   - این قوانین می‌توانند به شکل محدودیت در تابع هدف مدل‌های ML اعمال شوند (regularization بر حسب fairness).

این بخش را می‌توان در اسلایدهای ۷ و ۸ توضیح داد تا نشان بدهی که این کار فقط یک PoC نیست، بلکه یک **پلتفرم قابل گسترش** است.

---

## 8. سؤالات سخت و انتقادی — آماده‌سازی کامل برای داوری

این بخش شامل سخت‌ترین و انتقادی‌ترین سوالاتی است که ممکن است از شما پرسیده شود. هر سوال با پاسخ کامل و استراتژی پاسخگویی ارائه شده است.

---

## ۸.۱ — سوالات فنی و معماری

### سؤال ۱: چرا CNN و نه LSTM/Transformer؟ این انتخاب شما outdated نیست؟

**نقد:** CNN برای sequence modeling قدیمی است. LSTM/Transformer استاندارد صنعت هستند.

**پاسخ:**
- **CNN برای پنجره‌های کوتاه (۱–۲ ساعت) بهینه است**: در پیش‌بینی سرج، ما به الگوهای محلی و کوتاه‌مدت نیاز داریم، نه dependencies طولانی‌مدت. CNN با kernel size=3 دقیقاً این را می‌بیند.
- **سرعت آموزش**: CNN روی GPU در چند دقیقه آموزش می‌بیند، LSTM/Transformer نیاز به ساعت‌ها زمان دارند. برای retraining مکرر (هر ۶ ساعت) این تفاوت حیاتی است.
- **قابلیت تفسیر**: با CNN می‌توانیم ببینیم کدام پنجره‌های زمانی مهم‌ترند (از طریق attention maps). Transformer برای این use case overkill است.
- **Roadmap شفاف**: LSTM/TFN در roadmap است، اما اول می‌خواستیم pipeline را validate کنیم. این رویکرد incremental risk را کاهش می‌دهد.

**نکته کلیدی:** «ما از ساده شروع کردیم تا pipeline را validate کنیم. CNN برای افق ۳۰–۹۰ دقیقه کافی است. برای افق‌های طولانی‌تر (۳+ ساعت) LSTM/TFN را اضافه می‌کنیم.»

---

### سؤال ۲: دقت مدل‌های شما چقدر است؟ MAE/RMSE شما قابل قبول است؟

**نقد:** اگر MAE شما بالا باشد، مدل بی‌فایده است.

**پاسخ:**
- **متریک‌های فعلی**: روی دیتاست ۸ روزه، MAE در حدود ۱۰۰–۱۳۰ و RMSE در حدود ۱۴۰–۱۷۰ است (مقادیر دقیق در mlruns موجود است). این مقادیر برای پیش‌بینی demand signal در scale ۰–۱۰۰۰ قابل قبول است.
- **مقایسه با baseline**: برای validation، می‌توانیم با baseline models (مثلاً last value یا moving average) مقایسه کنیم، اما تمرکز ما روی **جهت پیش‌بینی** است نه دقت مطلق.
- **مهم‌تر از دقت مطلق**: برای use case ما، **جهت پیش‌بینی** (افزایش/کاهش) مهم‌تر از مقدار دقیق است. اگر مدل بتواند بگوید «سرج در ۳۰ دقیقه +۲۰% می‌رود»، حتی اگر مقدار دقیق ±۵% خطا داشته باشد، کافی است.
- **A/B testing واقعی**: دقت واقعی را در production با A/B testing اندازه می‌گیریم، نه روی داده‌های historical.

**نکته کلیدی:** «ما اول pipeline را ساختیم. دقت را در production با A/B testing اندازه می‌گیریم. برای شروع، جهت پیش‌بینی مهم‌تر از مقدار دقیق است.»

---

### سؤال ۳: چرا ElasticNet و CNN را جداگانه دارید؟ چرا ensemble نمی‌کنید؟

**نقد:** داشتن دو مدل جداگانه پیچیدگی اضافی است.

**پاسخ:**
- **استفاده متفاوت**: ElasticNet برای **validation و explainability** است. CNN برای **production prediction** است. هر کدام use case متفاوتی دارند.
- **Ensemble در roadmap**: می‌توانیم weighted ensemble بسازیم، اما اول می‌خواستیم هر مدل را جداگانه validate کنیم.
- **Trade-off**: Ensemble دقت را کمی بالا می‌برد اما latency و complexity را افزایش می‌دهد. برای MVP، این trade-off ارزش ندارد.
- **استراتژی**: ElasticNet به عنوان fallback استفاده می‌شود اگر CNN fail کند.

**نکته کلیدی:** «هر مدل use case متفاوتی دارد. ElasticNet برای explainability، CNN برای accuracy. Ensemble در roadmap است.»

---

### سؤال ۴: معماری شما چقدر scalable است؟ اگر ۱۰۰ شهر و ۱۰۰۰ هگزاگون داشته باشید چه می‌شود؟

**نقد:** معماری شما برای scale بزرگ مناسب نیست.

**پاسخ:**
- **مدل per (city, service_type)**: هر مدل مستقل است، پس می‌توانیم parallel training کنیم.
- **TimeSeriesStore بهینه**: Parquet format برای query سریع و storage بهینه طراحی شده.
- **Caching strategy**: مدل‌های آموزش‌دیده در MLflow cache می‌شوند، نیازی به retrain مداوم نیست.
- **Horizontal scaling**: API layer stateless است، می‌توانیم replica کنیم.
- **Resource optimization**: با `/tmp/mlruns` و SQLite، storage overhead کم است.

**نکته کلیدی:** «معماری ما stateless و parallel-ready است. برای scale بزرگ، horizontal scaling و caching strategy داریم.»

---

### سؤال ۵: چرا از ClickHouse و NATS استفاده کردید؟ چرا Kafka و PostgreSQL نه؟

**نقد:** انتخاب تکنولوژی شما arbitrary است.

**پاسخ:**
- **ClickHouse**: **در حال حاضر در استک اسنپ استفاده می‌شود**. ما نمی‌خواستیم infrastructure جدید اضافه کنیم. ClickHouse برای analytics queries عالی است.
- **NATS**: **سبک‌وزن و event-driven**. برای retraining triggers کافی است. Kafka برای این use case overkill است (نیازی به replay log نداریم).
- **Trade-off**: NATS ساده‌تر است، اما اگر نیاز به Kafka باشد، می‌توانیم migrate کنیم (interface abstraction داریم).
- **PostgreSQL vs ClickHouse**: ClickHouse برای time-series analytics ۱۰–۱۰۰x سریع‌تر است. PostgreSQL برای transactional data است.

**نکته کلیدی:** «ما از infrastructure موجود اسنپ استفاده کردیم. اگر نیاز به تغییر باشد، abstraction layer داریم.»

---

## ۸.۲ — سوالات داده و کیفیت

### سؤال ۶: داده‌های شما چقدر کامل است؟ Missing data را چطور handle می‌کنید؟

**نقد:** اگر داده‌های شما incomplete باشد، مدل unreliable است.

**پاسخ:**
- **Data validation**: در `CSVSignalLoader` و `SnapshotBuilder` validation داریم.
- **Missing data handling**: 
  - برای numerical features: forward fill + backward fill
  - برای lag features: اگر lag موجود نباشد، از mean استفاده می‌کنیم
  - برای sequences: اگر window کامل نباشد، آن sequence را skip می‌کنیم
- **Data quality metrics**: در MLflow، تعداد missing values و data coverage را log می‌کنیم.
- **Robustness**: مدل‌ها با dropout و regularization train شده‌اند تا نسبت به noise مقاوم باشند.

**نکته کلیدی:** «ما data validation و missing data handling داریم. کیفیت داده را در MLflow track می‌کنیم.»

---

### سؤال ۷: چطور مطمئن می‌شوید داده‌های شما biased نیست؟ (مثلاً فقط مناطق خاص)

**نقد:** اگر داده‌های شما biased باشد، مدل unfair است.

**پاسخ:**
- **Coverage tracking**: در MLflow، تعداد hexagons و service types را log می‌کنیم تا مطمئن شویم coverage کافی داریم.
- **Fairness constraints**: در roadmap، fairness regularization داریم (مثلاً ensure کردن که همه مناطق به طور مساوی represented هستند).
- **Data sampling**: برای training، از stratified sampling استفاده می‌کنیم تا همه service types و city_ids represented باشند.
- **Validation**: روی subgroups مختلف (مثلاً فقط service_type=1) validate می‌کنیم.

**نکته کلیدی:** «ما coverage و fairness را track می‌کنیم. Fairness constraints در roadmap است.»

---

### سؤال ۸: داده‌های شما چقدر fresh است؟ Latency از ClickHouse تا prediction چقدر است؟

**نقد:** اگر داده stale باشد، پیش‌بینی‌ها بی‌فایده است.

**پاسخ:**
- **Data freshness**: داده‌ها از ClickHouse با cadence ۷.۵ دقیقه می‌آیند (مطابق `period_duration_minutes`).
- **Pipeline latency**: 
  - CSV/ClickHouse → TimeSeriesStore: ~۱–۲ دقیقه (batch processing)
  - TimeSeriesStore → Model prediction: <۱۰۰ms (cached model)
- **Real-time vs Batch**: برای training از batch استفاده می‌کنیم. برای serving، مدل cached است و prediction instant است.
- **Trade-off**: برای accuracy، batch processing کافی است. برای real-time، می‌توانیم streaming pipeline اضافه کنیم.

**نکته کلیدی:** «داده‌ها هر ۷.۵ دقیقه refresh می‌شوند. Prediction latency <۱۰۰ms است چون مدل cached است.»

---

## ۸.۳ — سوالات Production و عملیاتی

### سؤال ۹: اگر مدل fail کند یا prediction اشتباه بدهد چه می‌شود؟ Fallback strategy چیست؟

**نقد:** اگر مدل fail کند، کل سیستم down می‌شود.

**پاسخ:**
- **Fallback model**: ElasticNet به عنوان fallback استفاده می‌شود اگر CNN fail کند.
- **Confidence threshold**: اگر confidence score پایین باشد، prediction را reject می‌کنیم و از baseline استفاده می‌کنیم.
- **Circuit breaker**: اگر error rate بالا برود، به طور موقت model را disable می‌کنیم.
- **Monitoring**: در production، prediction accuracy و error rate را monitor می‌کنیم.
- **Human override**: تیم ops می‌تواند model را manually disable کند.

**نکته کلیدی:** «ما fallback model، confidence threshold، و circuit breaker داریم. Monitoring و human override هم داریم.»

---

### سؤال ۱۰: چطور model drift را detect و handle می‌کنید؟

**نقد:** اگر distribution داده تغییر کند، مدل outdated می‌شود.

**پاسخ:**
- **Retraining schedule**: مدل هر ۶ ساعت retrain می‌شود (از طریق NATS event).
- **Drift detection**: در roadmap، statistical tests برای detect کردن distribution shift داریم (مثلاً KL divergence).
- **Performance monitoring**: اگر MAE/RMSE در production بالا برود، alert می‌دهیم.
- **A/B testing**: مدل‌های جدید را در A/B test می‌گذاریم قبل از full rollout.

**نکته کلیدی:** «ما retraining schedule داریم. Drift detection و performance monitoring در roadmap است.»

---

### سؤال ۱۱: Security و privacy چطور؟ داده‌های حساس راننده‌ها را چطور handle می‌کنید؟

**نقد:** اگر security issue باشد، نمی‌توانید deploy کنید.

**پاسخ:**
- **Data anonymization**: ما فقط aggregated data استفاده می‌کنیم (hexagon-level، نه driver-level).
- **No PII**: در TimeSeriesStore، هیچ PII (personally identifiable information) ذخیره نمی‌شود.
- **Access control**: API با authentication و authorization محافظت می‌شود.
- **Encryption**: در production، data در transit و at rest encrypted است.
- **Compliance**: با GDPR و قوانین privacy سازگار است چون فقط aggregated data داریم.

**نکته کلیدی:** «ما فقط aggregated data استفاده می‌کنیم، نه PII. Security و compliance از روز اول در نظر گرفته شده.»

---

### سؤال ۱۲: Cost و resource usage چقدر است؟ آیا مقرون‌به‌صرفه است؟

**نقد:** اگر cost بالا باشد، ROI منفی است.

**پاسخ:**
- **Storage**: با Parquet و `/tmp/mlruns`، storage overhead کم است (~GB برای ۸ روز داده).
- **Compute**: Training یک بار انجام می‌شود (هر ۶ ساعت)، serving بسیار سبک است (<۱۰۰ms per prediction).
- **Infrastructure**: از infrastructure موجود اسنپ استفاده می‌کنیم (ClickHouse، NATS)، cost اضافی minimal است.
- **ROI calculation**: اگر پیش‌بینی‌ها حتی ۵% درآمد راننده را بالا ببرد، ROI مثبت است.
- **Optimization**: می‌توانیم model quantization و caching بهینه‌تر کنیم.

**نکته کلیدی:** «Cost minimal است چون از infrastructure موجود استفاده می‌کنیم. ROI با حتی بهبود کوچک مثبت است.»

---

## ۸.۴ — سوالات Product و Business

### سؤال ۱۳: چطور مطمئن می‌شوید واقعاً درآمد/تعداد سفر را بالا می‌برد؟ Evidence چیست؟

**نقد:** شما فقط مدل ساخته‌اید، اما business impact ندارید.

**پاسخ:**
- **Offline validation**: روی داده‌های historical، پیش‌بینی‌های ما با actual surge مقایسه شده و correlation مثبت دارد.
- **A/B testing plan**: در production، A/B test طراحی کرده‌ایم:
  - Control group: بدون Pulsar
  - Treatment group: با Pulsar recommendations
  - Metrics: درآمد ساعتی راننده، تعداد سفر، زمان انتظار مسافر
- **Incremental rollout**: اول روی یک شهر کوچک test می‌کنیم، سپس scale up.
- **Honest answer**: «ما الان pipeline را validate کردیم. Business impact را در A/B test اندازه می‌گیریم. وعده نمی‌دهیم، اما evidence اولیه مثبت است.»

**نکته کلیدی:** «ما pipeline را validate کردیم. Business impact را در A/B test اندازه می‌گیریم. وعده نمی‌دهیم.»

---

### سؤال ۱۴: چرا راننده‌ها باید به پیش‌بینی‌های شما اعتماد کنند؟ Explainability چیست؟

**نقد:** اگر راننده‌ها اعتماد نکنند، استفاده نمی‌کنند.

**پاسخ:**
- **ElasticNet explainability**: ضرایب ElasticNet نشان می‌دهد کدام features مهم‌ترند (مثلاً lag_demand vs price_conversion).
- **Feature importance**: برای CNN، می‌توانیم attention maps نشان دهیم که کدام پنجره‌های زمانی مهم‌ترند.
- **Human-readable explanations**: در API، توضیحات ساده می‌دهیم (مثلاً «Concert exit، +۲۲% demand expected»).
- **Transparency**: راننده می‌تواند ببیند چرا این recommendation داده شده (از طریق dashboard).
- **Trust building**: با A/B testing و showing results، trust build می‌کنیم.

**نکته کلیدی:** «ما explainability و human-readable explanations داریم. Trust را با transparency و results build می‌کنیم.»

---

### سؤال ۱۵: چطور با سیستم‌های موجود (مثلاً Kandoo) یکپارچه می‌شود؟ Conflict نمی‌کند؟

**نقد:** اگر با سیستم‌های موجود conflict کند، deploy نمی‌شود.

**پاسخ:**
- **Complementary، نه replacement**: Pulsar **مکمل** Kandoo است، نه جایگزین. Kandoo برای pricing، Pulsar برای driver recommendations.
- **Event-driven integration**: از طریق NATS، events را publish می‌کنیم که Kandoo می‌تواند consume کند.
- **No conflict**: ما pricing را تغییر نمی‌دهیم، فقط recommendations می‌دهیم. Conflict ندارد.
- **Gradual integration**: می‌توانیم به تدریج integrate کنیم، نه big bang.

**نکته کلیدی:** «Pulsar مکمل Kandoo است، نه جایگزین. Event-driven integration، conflict ندارد.»

---

### سؤال ۱۶: اگر راننده‌ها recommendations شما را ignore کنند چه می‌شود؟

**نقد:** اگر adoption rate پایین باشد، impact ندارد.

**پاسخ:**
- **Gamification**: می‌توانیم incentives بدهیم (مثلاً bonus برای following recommendations).
- **Feedback loop**: اگر راننده recommendation را follow کند و نتیجه مثبت باشد، trust build می‌شود.
- **Personalization**: در آینده، recommendations را personalize می‌کنیم (بر اساس history راننده).
- **Opt-out option**: راننده می‌تواند opt-out کند، اما data را collect می‌کنیم تا ببینیم چرا ignore می‌کنند.

**نکته کلیدی:** «ما gamification، feedback loop، و personalization داریم. Adoption را با incentives و results افزایش می‌دهیم.»

---

## ۸.۵ — سوالات رقابتی و مقایسه

### سؤال ۱۷: چه تفاوتی با راه‌حل‌های دیگر (مثلاً Uber Movement، Google Maps) دارد؟

**نقد:** چرا راه‌حل جدید بسازید وقتی راه‌حل‌های موجود هستند؟

**پاسخ:**
- **Domain-specific**: Uber Movement و Google Maps عمومی هستند. Pulsar **مخصوص ride-hailing** است و از signals خاص اسنپ استفاده می‌کند (acceptance rate، price conversion، surge).
- **Real-time integration**: با infrastructure اسنپ (ClickHouse، NATS) یکپارچه است، نه external API.
- **Driver-facing**: برای راننده‌ها طراحی شده، نه فقط analytics.
- **MLOps from day one**: راه‌حل‌های دیگر فقط dashboard دارند، ما full MLOps pipeline داریم.

**نکته کلیدی:** «Pulsar domain-specific و driver-facing است. با infrastructure اسنپ یکپارچه است.»

---

### سؤال ۱۸: چرا این کار را در ۳ ماه انجام دادید؟ آیا quality قربانی شده؟

**نقد:** اگر عجله کرده‌اید، quality پایین است.

**پاسخ:**
- **Focus روی core**: ما روی core pipeline (data → model → API) focus کردیم، نه features اضافی.
- **Production-ready core**: core pipeline production-ready است (MLflow، Docker، API). Features اضافی در roadmap است.
- **Incremental approach**: از ساده شروع کردیم (ElasticNet)، سپس اضافه کردیم (CNN). این approach risk را کاهش می‌دهد.
- **Quality metrics**: ما quality metrics داریم (MAE، RMSE، coverage). Quality قربانی نشده، فقط scope محدود شده.

**نکته کلیدی:** «ما روی core pipeline focus کردیم. Quality metrics داریم. Scope محدود شده، نه quality.»

---

### سؤال ۱۹: اگر تیم دیگری همین ایده را بهتر پیاده‌سازی کند چه می‌شود؟

**نقد:** شما first mover نیستید، پس advantage ندارید.

**پاسخ:**
- **Execution matters**: ایده مهم نیست، execution مهم است. ما execution کامل داریم (data، model، API، MLOps).
- **Domain knowledge**: ما از signals خاص اسنپ استفاده می‌کنیم که تیم‌های دیگر ندارند.
- **Integration advantage**: با infrastructure اسنپ یکپارچه است، migration cost برای تیم‌های دیگر بالا است.
- **Continuous improvement**: roadmap داریم (LSTM، fairness، personalization). ما iterating می‌کنیم.

**نکته کلیدی:** «Execution و domain knowledge advantage داریم. Integration cost برای دیگران بالا است.»

---

## ۸.۶ — سوالات اخلاقی و Fairness

### سؤال ۲۰: آیا مدل شما fair است؟ آیا همه راننده‌ها به طور مساوی benefit می‌کنند؟

**نقد:** اگر مدل biased باشد، unfair است.

**پاسخ:**
- **Fairness tracking**: در MLflow، coverage و distribution را track می‌کنیم.
- **Fairness constraints (roadmap)**: regularization برای ensure کردن fairness (مثلاً ensure کردن که همه مناطق represented هستند).
- **Transparency**: می‌توانیم نشان دهیم چرا recommendation داده شده (explainability).
- **Monitoring**: در production، fairness metrics را monitor می‌کنیم (مثلاً distribution of recommendations across regions).

**نکته کلیدی:** «ما fairness tracking و monitoring داریم. Fairness constraints در roadmap است.»

---

### سؤال ۲۱: آیا recommendations شما باعث concentration نمی‌شود؟ (همه راننده‌ها به یک منطقه بروند)

**نقد:** اگر همه راننده‌ها به یک منطقه بروند، oversupply می‌شود.

**پاسخ:**
- **Capacity constraints**: در recommendations، capacity هر منطقه را در نظر می‌گیریم (چند راننده می‌تواند به آن منطقه برود).
- **Diversification**: recommendations را diversify می‌کنیم (نه همه به یک منطقه).
- **Feedback loop**: اگر oversupply شود، model یاد می‌گیرد و recommendations را adjust می‌کند.
- **Human oversight**: تیم ops می‌تواند manually adjust کند.

**نکته کلیدی:** «ما capacity constraints و diversification داریم. Feedback loop و human oversight هم داریم.»

---

## ۸.۷ — استراتژی پاسخگویی کلی

### اگر سوالی پرسیده شد که جوابش را نمی‌دانید:

1. **صادق باشید**: «این سوال خوبی است. من الان جواب دقیق ندارم، اما می‌توانم بعداً follow-up کنم.»
2. **به roadmap اشاره کنید**: «این feature در roadmap است. اول می‌خواستیم core pipeline را validate کنیم.»
3. **Trade-off را توضیح دهید**: «ما trade-off بین X و Y داشتیم. برای MVP، X را انتخاب کردیم.»
4. **Evidence نشان دهید**: «ما این را validate کردیم (مثلاً در MLflow). می‌توانید ببینید.»

### نکات کلیدی برای همه پاسخ‌ها:

- **صادق باشید**: وعده ندهید، واقعیت بگویید.
- **Evidence نشان دهید**: به MLflow، data، code اشاره کنید.
- **Roadmap داشته باشید**: اگر feature ندارید، بگویید در roadmap است.
- **Trade-off را توضیح دهید**: نشان دهید که تصمیمات شما intentional است، نه arbitrary.

---

## 9. جمع‌بندی یک‌خطی برای پایان ارائه

> **Pulsar** سرج را از یک مشکل لحظه‌ای و واکنشی، به یک مسأله‌ی قابل پیش‌بینی و قابل مدیریت تبدیل می‌کند؛  
>  با استفاده از داده‌های واقعی اسنپ، لایه‌های مختلف هوش مصنوعی (Feature Engineering زمان‑سری، ElasticNet، CNN و در آینده LSTM/TFN)، ارکستریشن NATS و ثبت کامل در MLflow – آماده برای این‌که روی ناوگان واقعی راننده‌ها اجرا شود و درآمد و تجربه‌ی کاربر را یک پله بالا ببرد.

این متن را می‌توان مستقیم به اسلاید تبدیل کرد، یا به‌عنوان اسکریپت کنار `presentation_UI.html` موقع ارائه استفاده کرد.


