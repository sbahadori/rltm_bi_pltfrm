<div dir="rtl">

# راهنمای Onboarding برای Pipeline و Job

این راهنما برای وقتی است که می‌خواهیم یک `pipeline` یا `job` جدید به پلتفرم اضافه کنیم و آن را از `Catalog` وارد `Control DB` کنیم.

## قانون اصلی

- فایل `configs/batch/pipeline_catalog.json` منبع اصلی تعریف pipeline و job است.
- جدول‌های `meta.pipeline` و `meta.job` و `meta.dataset` و `meta.job_dependency` خروجی runtime هستند و با onboarding از روی Catalog ساخته یا به‌روزرسانی می‌شوند.
- UI Job Builder فقط draft/proposal می‌سازد. job تا وقتی approve و onboard نشود، runtime source of truth نیست.
- Airflow فقط executor/orchestrator است. اجرای batch باید job spec را از `meta.job.config` بخواند.

## پیش‌نیازها

از root پروژه اجرا کنید:

```powershell
.\scripts\dev\rebuild_control_db.ps1
.\scripts\dev\seed_dashboard_admin.ps1 `
  -Username admin `
  -Password "change-me" `
  -Email admin@example.com `
  -Role admin
.\scripts\dev\verify_control_db.ps1
```

اگر دیتابیس local را کامل پاک کرده‌اید:

```powershell
.\scripts\dev\rebuild_control_db.ps1 -Reset -IUnderstandThisDeletesData
```

## مسیر پیشنهادی: اضافه کردن از UI

1. داشبورد را باز کنید:

```text
http://localhost:9090
```

2. با کاربر admin وارد شوید.
3. به بخش `Pipeline Catalog Editor` بروید.
4. JSON مربوط به job یا pipeline را وارد کنید.
5. اول `Validate` و بعد `Preview` بزنید.
6. برای draft فقط `Submit Proposal` بزنید.
7. برای اینکه بعد از approval درخواست onboarding هم ثبت شود، `Submit Proposal + Onboarding Request` بزنید.
8. کاربر `admin` یا `operator` باید proposal را approve کند.
9. اگر گزینه `Approve + Onboard` استفاده شود، DAG مربوط به onboarding اجرا می‌شود و Catalog به `meta.job.config` materialize می‌شود.

## مسیر مستقیم با اسکریپت

بعد از اینکه `configs/batch/pipeline_catalog.json` را تغییر دادید، همیشه اول dry-run بگیرید.

Dry-run یک job:

```powershell
.\scripts\dev\onboard_catalog.ps1 `
  -PipelineName weather_forecast_pipeline `
  -JobName weather_forecast_ingest `
  -DryRun
```

Onboard همان job:

```powershell
.\scripts\dev\onboard_catalog.ps1 `
  -PipelineName weather_forecast_pipeline `
  -JobName weather_forecast_ingest
```

Onboard کل pipeline:

```powershell
.\scripts\dev\onboard_catalog.ps1 `
  -PipelineName weather_forecast_pipeline
```

Onboard کل Catalog:

```powershell
.\scripts\dev\onboard_catalog.ps1
```

Onboard چند انتخاب با هم:

```powershell
.\scripts\dev\onboard_catalog.ps1 `
  -SelectionsJson '[{"pipeline_name":"gold_price_pipeline","job_name":"gold_price_ingest"},{"pipeline_name":"weather_forecast_pipeline","job_name":"weather_forecast_silver"}]'
```

اگر نمی‌خواهید dependencyهای upstream هم onboard شوند:

```powershell
.\scripts\dev\onboard_catalog.ps1 `
  -PipelineName weather_forecast_pipeline `
  -JobName weather_forecast_silver `
  -SkipDependencies
```

## ساختار Pipeline در Catalog

نمونه pipeline:

```json
{
  "name": "weather_forecast_pipeline",
  "enabled": true,
  "description": "Weather forecast Bronze/Silver/Gold batch pipeline",
  "domain": "weather",
  "source_type": "api",
  "dag": {
    "schedule": null,
    "catchup": false,
    "max_active_runs": 1,
    "start_date": "2026-01-01T00:00:00+00:00",
    "tags": ["weather", "batch", "api", "catalog_driven"],
    "default_args": {
      "owner": "admin",
      "retries": 0,
      "retry_delay_minutes": 1
    }
  },
  "jobs": []
}
```

## ساختار Job در Catalog

حداقل فیلدهای مهم job:

```json
{
  "name": "weather_forecast_ingest",
  "enabled": true,
  "job_type": "generic_api_to_bronze",
  "source_type": "api",
  "dependencies": [],
  "execution_timeout_minutes": 10,
  "retries": 0,
  "retry_delay_minutes": 1,
  "tags": ["weather", "api", "bronze"],
  "layer": "bronze",
  "spec": {}
}
```

نکته‌ها:

- `name` باید داخل همان pipeline یکتا باشد.
- `dependencies` نام jobهای upstream داخل همان pipeline است، نه `job_code`.
- `layer` خروجی job است: `bronze`، `silver`، `gold` یا `stream`.
- `job_type` معمولاً یکی از این‌هاست:
  - `generic_api_to_bronze`
  - `generic_jdbc_manifest_to_bronze`
  - `generic_bronze_to_silver`
  - `generic_silver_to_gold`
  - `generic_delta_to_gold`
- `spec` همان config اجرایی job است و در runtime داخل `meta.job.config` ذخیره می‌شود.

## نمونه Proposal برای یک Job جدید

برای endpoint زیر:

```text
POST /api/catalog/jobs/propose?request_onboarding=true
```

body:

```json
{
  "pipeline_name": "weather_forecast_pipeline",
  "create_pipeline_if_missing": false,
  "overwrite_existing_job": true,
  "job": {
    "name": "weather_forecast_ingest",
    "enabled": true,
    "job_type": "generic_api_to_bronze",
    "source_type": "api",
    "dependencies": [],
    "execution_timeout_minutes": 10,
    "retries": 0,
    "retry_delay_minutes": 1,
    "tags": ["weather", "api", "bronze"],
    "layer": "bronze",
    "spec": {
      "load_type": "event",
      "strategy": "append_event",
      "source": {
        "base_url": "https://api.example.com/v1/data",
        "method": "GET",
        "timeout_seconds": 20
      },
      "request": {
        "query_params": {},
        "headers": {},
        "body": null
      },
      "response": {
        "format": "json",
        "root_path": "$",
        "record_mode": "single_object"
      },
      "bronze_write": {
        "target_path": "s3a://lakehouse/bronze/weather_forecast_events",
        "mode": "append",
        "partition_by": ["ingest_year", "ingest_month", "ingest_day", "ingest_hour"],
        "include_raw_payload": true
      }
    }
  }
}
```

## نمونه Proposal برای Pipeline جدید

برای endpoint زیر:

```text
POST /api/catalog/pipelines/propose?request_onboarding=true
```

body:

```json
{
  "pipeline_name": "weather_forecast_pipeline",
  "create_pipeline_if_missing": true,
  "overwrite_existing_jobs": true,
  "pipeline": {
    "name": "weather_forecast_pipeline",
    "enabled": true,
    "description": "Weather forecast batch pipeline",
    "domain": "weather",
    "source_type": "api",
    "dag": {
      "schedule": null,
      "catchup": false,
      "max_active_runs": 1,
      "start_date": "2026-01-01T00:00:00+00:00",
      "tags": ["weather", "batch", "api", "catalog_driven"],
      "default_args": {
        "owner": "admin",
        "retries": 0,
        "retry_delay_minutes": 1
      }
    }
  },
  "jobs": [
    {
      "name": "weather_forecast_ingest",
      "enabled": true,
      "job_type": "generic_api_to_bronze",
      "source_type": "api",
      "dependencies": [],
      "layer": "bronze",
      "spec": {
        "source": {
          "base_url": "https://api.example.com/v1/data",
          "method": "GET"
        },
        "bronze_write": {
          "target_path": "s3a://lakehouse/bronze/weather_forecast_events",
          "mode": "append"
        }
      }
    }
  ]
}
```

اگر job از قبل در همان pipeline داخل Catalog وجود دارد، در payload pipeline می‌توان فقط نام job را داد:

```json
{
  "pipeline_name": "gold_price_pipeline",
  "jobs": ["gold_price_ingest"]
}
```

این حالت فقط برای reference به job موجود است. برای ساخت job جدید باید object کامل job را بفرستید.

## Approval و Publish

proposalها در جدول `meta.catalog_proposal` ذخیره می‌شوند. approve کردن proposal این کارها را انجام می‌دهد:

1. آخرین hash فایل Catalog را با proposal چک می‌کند.
2. از `pipeline_catalog.json` backup می‌گیرد.
3. Catalog جدید را publish می‌کند.
4. اگر onboarding درخواست شده باشد، DAG `control_plane_onboarding` را trigger می‌کند.

اگر خطای stale hash گرفتید، یعنی Catalog بعد از ساخت proposal تغییر کرده است. آخرین Catalog را reload کنید و proposal را دوباره بسازید.

## بعد از Onboarding چه چیزی باید ساخته شود؟

بعد از onboarding:

- `meta.pipeline` باید pipeline فعال را داشته باشد.
- `meta.job` باید job را با `job_code` فعال داشته باشد.
- `meta.job.config` باید spec کامل job را داشته باشد.
- `meta.dataset` برای jobهایی که target path دارند ساخته یا آپدیت می‌شود.
- `meta.job_dependency` برای dependencyها ساخته یا آپدیت می‌شود.

فرمت معمول `job_code`:

```text
{layer}.{pipeline_name}.{job_name}
```

نمونه:

```text
bronze.weather_forecast_pipeline.weather_forecast_ingest
silver.weather_forecast_pipeline.weather_forecast_silver
gold.weather_forecast_pipeline.weather_forecast_gold
```

## کنترل نهایی در دیتابیس

```powershell
docker exec postgres-warehouse psql -U warehouse -d warehouse -c "
SELECT pipeline_name, is_active, created_at, updated_at
FROM meta.pipeline
ORDER BY pipeline_name;
"
```

```powershell
docker exec postgres-warehouse psql -U warehouse -d warehouse -c "
SELECT job_code, job_name, pipeline_name, layer, is_active, active_flag
FROM meta.job
ORDER BY pipeline_name, job_code;
"
```

برای دیدن config runtime یک job:

```powershell
docker exec postgres-warehouse psql -U warehouse -d warehouse -c "
SELECT job_code, config
FROM meta.job
WHERE job_code = 'bronze.weather_forecast_pipeline.weather_forecast_ingest';
"
```

برای dependencyها:

```powershell
docker exec postgres-warehouse psql -U warehouse -d warehouse -c "
SELECT parent_job_code, child_job_code, dependency_type, is_active
FROM meta.job_dependency
ORDER BY child_job_code, parent_job_code;
"
```

## خطاهای رایج

### `Field required: pipeline_name`

برای endpoint مستقیم API نباید body را با root `pipelines` بفرستید. endpointهای direct مثل `/api/catalog/pipelines/propose` یک pipeline request می‌خواهند و باید `pipeline_name` داشته باشند.

در UI، payload کامل با root `pipelines` پشتیبانی می‌شود و UI آن را به چند proposal جدا تبدیل می‌کند.

### `jobs[0] should be a valid dictionary`

اگر job جدید می‌سازید، `jobs` باید object کامل job داشته باشد. string فقط وقتی درست است که به job موجود در همان pipeline اشاره کند.

### `Pipeline not found`

یا `pipeline_name` اشتباه است، یا `create_pipeline_if_missing=false` است، یا pipeline در Catalog disabled شده است.

### `Job not found or disabled`

یا `job_name` اشتباه است، یا job داخل Catalog `enabled=false` دارد.

### job اجرا می‌شود ولی config پیدا نمی‌شود

onboarding انجام نشده یا fail شده است. اول dry-run و بعد onboarding واقعی را اجرا کنید:

```powershell
.\scripts\dev\onboard_catalog.ps1 `
  -PipelineName your_pipeline `
  -JobName your_job `
  -DryRun

.\scripts\dev\onboard_catalog.ps1 `
  -PipelineName your_pipeline `
  -JobName your_job
```

## چک‌لیست کوتاه

- Pipeline و job در `configs/batch/pipeline_catalog.json` وجود دارد.
- `Validate` و `Preview` بدون خطا هستند.
- proposal approve شده است.
- onboarding با `DryRun` پاس شده است.
- onboarding واقعی اجرا شده است.
- `meta.job.config` برای job پر شده است.
- dependencyهای job در `meta.job_dependency` دیده می‌شوند.
- Airflow فقط job را trigger می‌کند و runtime config از Control DB خوانده می‌شود.

</div>
