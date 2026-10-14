from pyspark import pipelines as dp
from pyspark.sql.functions import coalesce, col, concat, concat_ws, lit

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
gold_schema = spark.conf.get("hospital.ingestion.gold_schema")

GOLD = f"{catalog_name}.{gold_schema}"


def txt(name):
    # concat() returns NULL if any part is NULL, so substitute a placeholder
    return coalesce(col(name).cast("string"), lit("n/a"))


@dp.materialized_view(
    name=f"{GOLD}.rag_insight_docs",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
def rag_insight_docs():
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

    return dept.unionByName(doctors)
