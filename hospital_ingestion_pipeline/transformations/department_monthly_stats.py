from pyspark import pipelines as dp
from transformations.src.department_monthly_stats import transform_department_monthly_stats

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
gold_schema = spark.conf.get("hospital.ingestion.gold_schema")

GOLD = f"{catalog_name}.{gold_schema}"


@dp.materialized_view(name=f"{GOLD}.department_monthly_stats")
def department_monthly_stats():
    return transform_department_monthly_stats(spark.read.table(f"{GOLD}.visits_enriched"))
