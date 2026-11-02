# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold
# MAGIC Business-ready tables:
# MAGIC * `gold_claims_by_lob` — KPIs per line of business (the table the app shows)
# MAGIC * `gold_claims_monthly` — claim count and amounts by loss month and line of business

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "claims")
dbutils.widgets.text("target_schema", "")
dbutils.widgets.text("run_date", "")

catalog = dbutils.widgets.get("catalog")
target = dbutils.widgets.get("target_schema").strip() or dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date") or str(spark.sql("SELECT current_date()").first()[0])
t = f"`{catalog}`.`{target}`"

# Value published by the bronze task (debugValue is used when running this notebook by hand)
bronze_rows = dbutils.jobs.taskValues.get(taskKey="bronze", key="row_count", default=0, debugValue=0)

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE {t}.gold_claims_by_lob AS
  SELECT DATE'{run_date}'                                         AS run_date,
         line_of_business,
         count(*)                                                 AS claims,
         sum(CASE WHEN claim_status <> 'closed' THEN 1 ELSE 0 END) AS open_claims,
         sum(incurred_amount)                                     AS incurred_amount,
         sum(paid_amount)                                         AS paid_amount,
         sum(outstanding_amount)                                  AS outstanding_amount,
         round(avg(incurred_amount), 2)                           AS avg_severity,
         round(avg(report_lag_days), 1)                           AS avg_report_lag_days,
         {int(bronze_rows)}                                       AS bronze_rows
  FROM {t}.silver_claims
  GROUP BY line_of_business
""")

spark.sql(f"""
  CREATE OR REPLACE TABLE {t}.gold_claims_monthly AS
  SELECT date_trunc('month', loss_date) AS loss_month, line_of_business,
         count(*) AS claims, sum(incurred_amount) AS incurred_amount, sum(paid_amount) AS paid_amount
  FROM {t}.silver_claims
  GROUP BY ALL
""")

display(spark.table(f"{t}.gold_claims_by_lob").orderBy("line_of_business"))
