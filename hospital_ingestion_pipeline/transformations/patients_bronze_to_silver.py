from pyspark import pipelines as dp
from pyspark.sql.functions import col
from transformations.src.patient_silver import VALIDATIONS, transform_patient_silver

catalog_name = spark.conf.get("hospital.ingestion.catalog_name")


@dp.view
@dp.expect_or_drop("existing patient_id", "patient_id IS NOT NULL")
@dp.expect_all(VALIDATIONS)
def patient_silver_clean():
    return transform_patient_silver(
        spark.readStream.table(f"{catalog_name}.hospital_bronze.patients_bronze")
    )


MASK = f"MASK {catalog_name}.hospital_silver.mask_pii"

# functions are created by GOVERNANCE.py (first task of the job)
dp.create_streaming_table(
    name=f"{catalog_name}.hospital_silver.patient_streaming_silver",
    comment="transformed streaming data",
    schema=f"""
        patient_id STRING {MASK},
        admission_datetime TIMESTAMP,
        first_initial STRING {MASK},
        last_name STRING {MASK},
        gender STRING,
        age INT,
        race STRING,
        department_referral STRING,
        admission_flag BOOLEAN,
        satisfaction_score INT,
        wait_time INT,
        coordinator_manager_flag BOOLEAN,
        doctor_id STRING,
        source_file STRING,
        source_row INT,
        visit_id STRING,
        bronze_timestamp TIMESTAMP,
        silver_timestamp TIMESTAMP
    """,
    row_filter=f"ROW FILTER {catalog_name}.hospital_silver.department_row_filter ON (department_referral)",
    table_properties={
        "quality": "silver",
        #"pipelines.reset.allowed": "false",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)


dp.create_auto_cdc_flow(
    target=f"{catalog_name}.hospital_silver.patient_streaming_silver",
    source="patient_silver_clean",
    keys=["visit_id"],
    sequence_by=col("bronze_timestamp"),
    stored_as_scd_type="1",
)
