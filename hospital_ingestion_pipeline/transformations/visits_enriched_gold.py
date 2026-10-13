from pyspark import pipelines as dp
from transformations.src.visits_enriched import transform_visits_enriched

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")
gold_schema = spark.conf.get("hospital.ingestion.gold_schema")
silver_schema = spark.conf.get("hospital.ingestion.silver_schema")

SILVER = f"{catalog_name}.{silver_schema}"
GOLD = f"{catalog_name}.{gold_schema}"


# gold has no patient PII, so only the department row filter is attached (function created by GOVERNANCE.py).
# The refresh must run as a member of `hospital_admin`, otherwise rows are filtered out of the stored result.
@dp.materialized_view(
    name=f"{GOLD}.visits_enriched",
    schema="""
        visit_id STRING,
        admission_datetime TIMESTAMP,
        age INT,
        gender STRING,
        race STRING,
        admission_flag BOOLEAN,
        satisfaction_score INT,
        wait_time INT,
        department_referral STRING,
        coordinator_involved INT,
        doctor_id STRING,
        doctor_first_name STRING,
        doctor_last_name STRING,
        department STRING,
        subspecialty STRING,
        university STRING,
        years_of_experience INT,
        admission_month DATE,
        admission_hour INT,
        weekday_num INT,
        weekday STRING,
        age_group STRING,
        wait_bucket_in_mins STRING,
        experience_bucket_in_years STRING
    """,
    row_filter=f"ROW FILTER {SILVER}.department_row_filter ON (department_referral)",
)
def visits_enriched():
    patients_df = spark.read.table(f"{SILVER}.patient_streaming_silver")
    doctors_df = spark.read.table(f"{SILVER}.doctors_silver")
    return transform_visits_enriched(patients_df, doctors_df)
