# 🏥 hospital_emergency_databricks

End-to-end **medallion data platform for hospital emergency-room visits** on Databricks (Azure), deployed to DEV and PROD with Databricks Asset Bundles and GitHub Actions.

## Highlights

- **Streaming ingestion:** patient rows are served by a Databricks App (`csv-api-app_hospital/`) and sent to bronze with **ZeroBus**.
- **Batch ingestion:** doctors are read from Postgres (Neon) via **Lakehouse Federation** (full table read on every pipeline update).
- **Declarative pipeline:** Lakeflow bronze → silver (Auto CDC, expectations) → gold.
- **Governance:** Unity Catalog, column masks + row filter on silver, secrets from an Azure Key Vault-backed scope.
- **Automation:** tests + bundle validate/deploy through one GitHub Actions workflow; nothing is built by hand in PROD.
- **Analytics:** an AI/BI dashboard on top of the gold tables.

## Architecture

```mermaid
flowchart LR
    A[CSV API App] -->|ZeroBus| B1[(patients_bronze)]
    P[(Postgres / Neon)] -->|Lakehouse Federation| S2[doctors_silver]
    B1 --> S1[patient_streaming_silver<br/>Auto CDC SCD1, masks + row filter]
    S1 --> G[gold: visits_enriched + 5 aggregates]
    S2 --> G
    KV[Azure Key Vault<br/>secret scope] -.-> P
    KV -.-> A
```

### Lakeflow pipeline

`patients_bronze` → `patient_silver_clean` (view) → `patient_streaming_silver` (streaming table, Auto CDC upserts) and `doctors_silver` (materialized view, 4 expectations) → `visits_enriched` → five gold aggregates: `wait_vs_satisfaction`, `department_monthly_stats`, `doctor_performance`, `coordinator_impact`, `wait_time_by_hour_weekday`.

![Lakeflow pipeline graph](docs/pipeline_photo.png)

### Orchestration job

Job `hospital_ingest_and_refresh`: `external_table_connection` + `zerobus_patients` + `governance` run in parallel, then `run_pipeline`.

![Lakeflow job run](docs/job_photo.png)

## Data layers

| Layer | Schema | Content |
| --- | --- | --- |
| Bronze | `hospital_bronze` | `patients_bronze` (ZeroBus), volumes for CSV files and stream state |
| Silver | `hospital_silver` | `patient_streaming_silver` (masked, row-filtered), `doctors_silver` |
| Gold | `hospital_gold` | `visits_enriched` + 5 aggregates; no patient identifiers |

## Dashboard

[emergency_hospital dashboard.lvdash.json](<emergency_hospital dashboard.lvdash.json>) answers business questions on the gold tables: doctors leaderboard, visits per month, wait time per department, wait-time heatmap by hour and weekday, admission rate and satisfaction per department.

## Layout

| Path | Role |
| --- | --- |
| `databricks.yml` | Asset Bundle: targets `dev` / `prod` |
| `resources/` | job, pipeline, app, schemas + volumes |
| `hospital_ingestion_pipeline/transformations/` | Lakeflow files; pure logic in `src/` (unit tested) |
| `hospital_ingestion_pipeline/tests/` | pytest unit tests |
| `ZEROBUS_patients_ingest.py` | ZeroBus load into `{catalog}.hospital_bronze.patients_bronze` |
| `EXTERNAL_TABLE_CONNECTION.py` | Postgres connection + foreign catalog `hospital_db` |
| `GOVERNANCE.py` | creates mask / row-filter functions used by silver |
| `csv-api-app_hospital/` | Databricks App that streams CSV rows |
| `emergency_hospital dashboard.lvdash.json` | gold-layer dashboard |
| `.github/workflows/deploy.yml` | tests + bundle validate + deploy |

## CI/CD

One workflow, `.github/workflows/deploy.yml`, run **manually only**: Actions → *Run workflow* → pick `dev` or `prod`. Tests run first; only if they pass is the bundle validated and deployed to that target. 

Deploy does not start the job; run `hospital_ingest_and_refresh` afterwards (UI, or `databricks bundle run hospital_ingest_and_refresh -t <target>`).

### GitHub setup

Create Environments `dev` and `prod` (on `prod` enable **Required reviewers**). Secrets per environment, with suffix `_DEV` / `_PROD` as used in the workflow:

`DATABRICKS_HOST_*`, `DATABRICKS_TOKEN_*`, `ZEROBUS_WORKSPACE_ID_*`, `CATALOG_NAME_*`, `CSV_API_APP_NAME_*`, `ZEROBUS_REGION_*`

## Governance

- Column masks (`patient_id`, `first_initial`, `last_name`): shown only to group `hospital_admin`, others see `***`.
- Row filter on `department_referral`: visible to `hospital_admin` or group `dept_<department>` (e.g. `dept_orthopedics`).
- Gold tables contain no patient identifiers.

## One-time setup in each workspace 

1. Create an **Azure Key Vault-backed** secret scope named `scope_blech` with keys: `postgresHost-blech`, `postgresUser-blech`, `postgresPassword-blech`, `sp-databricks-adls-appid`, `sp-databricks-adls-appkey`.
2. Create the UC catalog (`dbr_dev` / `dbr_prod`) and the account groups `hospital_admin`, `dept_*`.
3. Create Service Principal (all apis scope) and token for GitHub Actions.




