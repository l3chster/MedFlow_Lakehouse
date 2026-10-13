# Databricks notebook source
# Creates the Unity Catalog functions used for column masking (CLS) and row filtering (RLS).
# Runs as a job task BEFORE the pipeline, so the functions exist when the pipeline creates the table.
#
# CLS: patient_id / last_name / first_initial are visible only to group `hospital_admin`.
# RLS: a row is visible to `hospital_admin` or to members of the group `dept_<department_referral>` (e.g. dept_orthopedics, dept_general_practice).


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


