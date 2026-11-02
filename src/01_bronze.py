# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze
# MAGIC Lands the claims table as-is into `bronze_claims`, adding load metadata.
# MAGIC Publishes the row count as a **task value** for the gold task.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "claims")
dbutils.widgets.text("claims_table", "claims")
dbutils.widgets.text("target_schema", "")
dbutils.widgets.text("run_date", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
target = dbutils.widgets.get("target_schema").strip() or schema
source = f"`{catalog}`.`{schema}`.`{dbutils.widgets.get('claims_table')}`"
run_date = dbutils.widgets.get("run_date") or str(spark.sql("SELECT current_date()").first()[0])

# COMMAND ----------

spark.sql(f"""
  CREATE OR REPLACE TABLE `{catalog}`.`{target}`.bronze_claims AS
  SELECT *, current_timestamp() AS _loaded_at, DATE'{run_date}' AS _run_date, '{source}' AS _source
  FROM {source}
""")
rows = spark.table(f"`{catalog}`.`{target}`.bronze_claims").count()
dbutils.jobs.taskValues.set(key="row_count", value=rows)
print(f"bronze_claims rows={rows} run_date={run_date}")
