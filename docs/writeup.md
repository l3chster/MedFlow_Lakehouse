# Write-up

## Design decisions
- **VARIANT payload in bronze:** ZeroBus rows land as `payload VARIANT`, so new source columns never break ingestion; silver extracts the known fields with `try_variant_get` and bad values become NULL.
- **Idempotency:** `visit_id = sha2(patient_id | admission_datetime)` plus `create_auto_cdc_flow` (SCD1, keyed by `visit_id`) means re-running the job or re-loading a file updates the same rows instead of duplicating them. Bronze is append-only; duplicates are removed in silver.
- **Data quality:** `expect_or_drop` for missing keys, `expect_all` for ranges (age, score, wait time) in silver; doctors validated for id format and experience.
- **Pure functions in `src/`:** all transformations are plain DataFrame functions, so they are unit tested locally with Spark in CI without a workspace.
- **Governance:** masks and row filter are attached to the silver table in the pipeline definition; gold has no PII.
- **Advanced features:** ZeroBus (streaming ingest) and Lakehouse Federation (Postgres doctors).

## Cost / performance trade-offs
- Serverless pipeline and job: no idle clusters, pay per run; startup latency instead of a warm cluster.
- Gold as materialized views: cheap incremental refresh, but refreshed only when the job runs (not real time).
- ZeroBus gives low-latency streaming; the demo app is a single stream sent in small batches.
- Federation reads the whole Postgres doctors table on every pipeline update. That is fine for a small dimension table; a large one would need a copy or incremental load.
- Row filter / masks add a small per-query cost on silver; gold stays unrestricted and cheap.

## AI-assisted development
- AI helped with: drafting Lakeflow/Asset Bundle YAML, test cases for bucket boundaries, CI workflow, README.
- Guardrails: every change goes through a pull request with unit tests and `bundle validate`; PROD deploy needs green tests and a reviewer approval; no secrets in code or prompts (Key Vault scope); generated code was reviewed and the pipeline run on DEV before PROD.
