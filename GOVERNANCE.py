# Databricks notebook source
# Creates the Unity Catalog functions used for column masking (CLS) and row filtering (RLS).
# They are attached to hospital_silver.patient_streaming_silver in
# hospital_ingestion_pipeline/transformations/patients_bronze_to_silver.py.
# Runs as a job task BEFORE the pipeline, so the functions exist when the pipeline creates the table.
#
# CLS: patient_id / last_name / first_initial are visible only to group `hospital_admin`.
# RLS: a row is visible to `hospital_admin` or to members of the group `dept_<department_referral>`
#      (e.g. dept_orthopedics, dept_general_practice).
# The pipeline's run-as principal must be a member of `hospital_admin`, otherwise gold is built from masked data.

CATALOG = dbutils.widgets.get("catalog_name").strip()
SILVER_SCHEMA = dbutils.widgets.get("silver_schema").strip()
SCHEMA = f"{CATALOG}.{SILVER_SCHEMA}"

spark.sql(f"""
CREATE OR REPLACE FUNCTION {SCHEMA}.mask_pii(value STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('hospital_admin') THEN value ELSE '***' END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {SCHEMA}.department_row_filter(department STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('hospital_admin')
    OR is_account_group_member(concat('dept_', lower(replace(department, ' ', '_'))))
""")


