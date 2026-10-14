# Databricks notebook source
# Creates (idempotent) the Delta Sync vector index on gold.rag_insight_docs and triggers a sync.
# Runs after the pipeline has refreshed the source table.

import time

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

SOURCE_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.rag_insight_docs"
INDEX_NAME = f"{CATALOG}.{GOLD_SCHEMA}.rag_insight_index"

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
