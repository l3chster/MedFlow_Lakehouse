from pyspark import pipelines as dp
from transformations.src.department_monthly_stats import transform_department_monthly_stats

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
gold_schema = spark.conf.get("hospital.ingestion.gold_schema")
silver_schema = spark.conf.get("hospital.ingestion.silver_schema")

GOLD = f"{catalog_name}.{gold_schema}"
SILVER = f"{catalog_name}.{silver_schema}"


# one row per department, so the same department row filter as silver applies (function created by GOVERNANCE.py)
@dp.materialized_view(
    name=f"{GOLD}.department_monthly_stats",
    schema="""
        department_referral STRING,
        admission_month DATE,
        visits BIGINT,
        avg_wait_time DOUBLE,
        avg_satisfaction DOUBLE,
        satisfaction_responses BIGINT,
        admission_rate_pct DOUBLE,
        coordinator_usage_pct DOUBLE,
        avg_age DOUBLE
    """,
    row_filter=f"ROW FILTER {SILVER}.department_row_filter ON (department_referral)",
)
def department_monthly_stats():
    return transform_department_monthly_stats(spark.read.table(f"{GOLD}.visits_enriched"))
