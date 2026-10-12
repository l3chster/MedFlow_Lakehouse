# Databricks notebook source
# scope_blech must be an Azure Key Vault-backed secret scope
host = dbutils.secrets.get("scope_blech", "postgresHost-blech")
user = dbutils.secrets.get("scope_blech", "postgresUser-blech")
pwd = dbutils.secrets.get("scope_blech", "postgresPassword-blech")

spark.sql(f"""
CREATE CONNECTION IF NOT EXISTS postgresql_connection
TYPE POSTGRESQL
OPTIONS (
  host '{host}',
  port '5432',
  user '{user}',
  password '{pwd}'
)
""")

spark.sql("""
CREATE FOREIGN CATALOG IF NOT EXISTS hospital_db
    USING CONNECTION postgresql_connection
    OPTIONS (database 'neondb')
""")
