from pyspark import pipelines as dp
from transformations.src.wait_vs_satisfaction import transform_wait_vs_satisfaction

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
gold_schema = spark.conf.get("hospital.ingestion.gold_schema")

GOLD = f"{catalog_name}.{gold_schema}"


@dp.materialized_view(name=f"{GOLD}.wait_vs_satisfaction")
def wait_vs_satisfaction():
    return transform_wait_vs_satisfaction(spark.read.table(f"{GOLD}.visits_enriched"))
