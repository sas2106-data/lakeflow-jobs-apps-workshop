# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver
# MAGIC Cleans bronze into `silver_claims`: removes duplicate claims, standardises status, keeps only valid
# MAGIC rows, and adds `report_lag_days` and `outstanding_amount`.
# MAGIC
# MAGIC **Column mapping:** if your claims table uses different column names, change only the `SELECT` below.
# MAGIC Set job parameter `simulate_failure=true` to fail this task on purpose for the **Repair run** exercise.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "claims")
dbutils.widgets.text("target_schema", "")
dbutils.widgets.text("simulate_failure", "false")

catalog = dbutils.widgets.get("catalog")
target = dbutils.widgets.get("target_schema").strip() or dbutils.widgets.get("schema")

if dbutils.widgets.get("simulate_failure").lower() == "true":
    raise RuntimeError("Simulated failure: use Repair run with simulate_failure=false")

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE `{catalog}`.`{target}`.silver_claims AS
  SELECT claim_id, policy_id, line_of_business, channel, loss_date, report_date,
         lower(trim(claim_status))                       AS claim_status,
         CAST(incurred_amount AS DECIMAL(18,2))          AS incurred_amount,
         CAST(paid_amount AS DECIMAL(18,2))              AS paid_amount,
         CAST(incurred_amount - paid_amount AS DECIMAL(18,2)) AS outstanding_amount,
         datediff(report_date, loss_date)                AS report_lag_days
  FROM (
    SELECT *, row_number() OVER (PARTITION BY claim_id ORDER BY _loaded_at DESC) AS _rn
    FROM `{catalog}`.`{target}`.bronze_claims
    WHERE claim_id IS NOT NULL AND incurred_amount >= 0 AND loss_date <= current_date()
  )
  WHERE _rn = 1
""")
display(spark.sql(f"""SELECT claim_status, count(*) AS claims
                      FROM `{catalog}`.`{target}`.silver_claims GROUP BY claim_status ORDER BY claims DESC"""))
