# Databricks notebook source
# Builds the RAG insight docs from the gold tables into a Delta table, creates (idempotent) the Delta Sync
# vector index on it and triggers a sync. Runs after the pipeline has refreshed the gold tables.

import time

from pyspark.sql.functions import coalesce, col, concat, concat_ws, lit
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists
from databricks.sdk.service.vectorsearch import (
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    EndpointType,
    PipelineType,
    VectorIndexType,
)

CATALOG = dbutils.widgets.get("catalog_name").strip()
GOLD_SCHEMA = dbutils.widgets.get("gold_schema").strip()
ENDPOINT = dbutils.widgets.get("vector_search_endpoint").strip()
EMBEDDING_MODEL = dbutils.widgets.get("embedding_model").strip()

GOLD = f"{CATALOG}.{GOLD_SCHEMA}"
# Vector Search cannot sync from a materialized view, so the docs live in a plain Delta table built here.
SOURCE_TABLE = f"{GOLD}.rag_insight_docs_vs"
INDEX_NAME = f"{GOLD}.rag_insight_index"


def txt(name):
    # concat() returns NULL if any part is NULL, so substitute a placeholder
    return coalesce(col(name).cast("string"), lit("n/a"))


dept = spark.read.table(f"{GOLD}.department_monthly_stats").select(
    concat_ws("-", lit("dept"), col("department_referral"), col("admission_month").cast("string")).alias("doc_id"),
    lit("department_month").alias("doc_type"),
    col("department_referral").alias("department"),
    col("admission_month").cast("string").alias("period"),
    concat(
        lit("In "), txt("admission_month"), lit(", the "), txt("department_referral"),
        lit(" department had "), txt("visits"), lit(" visits, an average wait time of "),
        txt("avg_wait_time"), lit(" minutes, an average satisfaction score of "), txt("avg_satisfaction"),
        lit(" ("), txt("satisfaction_responses"), lit(" responses), an admission rate of "),
        txt("admission_rate_pct"), lit("% and coordinator usage of "), txt("coordinator_usage_pct"),
        lit("%. Average patient age was "), txt("avg_age"), lit("."),
    ).alias("text"),
)

doctors = spark.read.table(f"{GOLD}.doctor_performance").select(
    concat_ws("-", lit("doctor"), col("doctor_id").cast("string")).alias("doc_id"),
    lit("doctor").alias("doc_type"),
    col("department"),
    lit(None).cast("string").alias("period"),
    concat(
        lit("Dr. "), txt("doctor_first_name"), lit(" "), txt("doctor_last_name"), lit(" works in "),
        txt("department"), lit(" ("), txt("subspecialty"), lit("), studied at "), txt("university"),
        lit(" and has "), txt("years_of_experience"), lit(" years of experience. Across "),
        txt("visits"), lit(" visits: average wait time "), txt("avg_wait_time"),
        lit(" minutes, average satisfaction "), txt("avg_satisfaction"), lit(" ("),
        txt("satisfaction_responses"), lit(" responses), admission rate "), txt("admission_rate_pct"),
        lit("%, coordinator usage "), txt("coordinator_usage_pct"), lit("%."),
    ).alias("text"),
)

dept.unionByName(doctors).createOrReplaceTempView("rag_insight_docs_src")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {SOURCE_TABLE} (
        doc_id STRING NOT NULL, doc_type STRING, department STRING, period STRING, text STRING
    ) TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
# MERGE (not overwrite) so only changed rows show up in the change feed and get re-embedded.
spark.sql(f"""
    MERGE INTO {SOURCE_TABLE} t
    USING rag_insight_docs_src s
    ON t.doc_id = s.doc_id
    WHEN MATCHED AND NOT (t.text <=> s.text AND t.department <=> s.department
                          AND t.period <=> s.period AND t.doc_type <=> s.doc_type)
        THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE THEN DELETE
""")

w = WorkspaceClient()

try:
    w.vector_search_endpoints.get_endpoint(ENDPOINT)
except NotFound:
    w.vector_search_endpoints.create_endpoint_and_wait(name=ENDPOINT, endpoint_type=EndpointType.STANDARD)

try:
    w.vector_search_indexes.create_index(
        name=INDEX_NAME,
        endpoint_name=ENDPOINT,
        primary_key="doc_id",
        index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=SOURCE_TABLE,
            pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[
                EmbeddingSourceColumn(name="text", embedding_model_endpoint_name=EMBEDDING_MODEL)
            ],
        ),
    )
    print(f"Created index {INDEX_NAME}")
except ResourceAlreadyExists:
    w.vector_search_indexes.sync_index(INDEX_NAME)
    print(f"Index {INDEX_NAME} exists, sync triggered")
except Exception as e:
    if "already exists" in str(e).lower():
        w.vector_search_indexes.sync_index(INDEX_NAME)
        print(f"Index {INDEX_NAME} exists, sync triggered")
    else:
        raise

for _ in range(60):
    if w.vector_search_indexes.get_index(INDEX_NAME).status.ready:
        break
    time.sleep(30)
print("Index ready:", w.vector_search_indexes.get_index(INDEX_NAME).status.ready)

# Governance: the RAG app service principal may read the insight table and the index.
APP_NAME = dbutils.widgets.get("rag_app_name").strip()
try:
    sp = w.apps.get(APP_NAME).service_principal_client_id
    spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{sp}`")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{GOLD_SCHEMA} TO `{sp}`")
    spark.sql(f"GRANT SELECT ON TABLE {SOURCE_TABLE} TO `{sp}`")
    spark.sql(f"GRANT SELECT ON TABLE {INDEX_NAME} TO `{sp}`")
except NotFound:
    print(f"App {APP_NAME} not deployed yet; skipping grants")
