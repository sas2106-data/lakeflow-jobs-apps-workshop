# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Checks the source **claims** table in the `schema` passed to the job and creates the target (gold) schema.
# MAGIC If `create_sample_data=true` and the claims table doesn't exist, creates a synthetic one so the
# MAGIC workshop can run before real data is connected.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "claims")              # schema that holds the claims table (job parameter)
dbutils.widgets.text("claims_table", "claims")
dbutils.widgets.text("target_schema", "")              # blank = write bronze/silver/gold into `schema`
dbutils.widgets.dropdown("create_sample_data", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
target = dbutils.widgets.get("target_schema").strip() or schema
source = f"`{catalog}`.`{schema}`.`{dbutils.widgets.get('claims_table')}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{target}`")

# COMMAND ----------

if not spark.catalog.tableExists(source.replace("`", "")):
    if dbutils.widgets.get("create_sample_data") != "true":
        raise ValueError(f"Claims table {source} not found. Pass the right `schema`/`claims_table`, "
                         "or set create_sample_data=true for a synthetic table.")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    # ~5,000 synthetic claims over 2 years, with 25 duplicate rows and messy status values
    spark.sql(f"""
      CREATE TABLE {source} AS
      WITH base AS (
        SELECT id,
               date_sub(current_date(), CAST(rand(1) * 730 AS INT))                                       AS loss_date,
               element_at(array('Motor','Property','Liability','Marine','Health'), CAST(rand(2) * 5 AS INT) + 1) AS line_of_business,
               element_at(array('open','Open ','closed','CLOSED','reopened'), CAST(rand(3) * 5 AS INT) + 1)     AS claim_status,
               element_at(array('Broker','Direct','Agent'), CAST(rand(4) * 3 AS INT) + 1)                       AS channel,
               round(exp(7 + rand(5) * 3.5), 2)                                                           AS incurred_amount,
               rand(6)                                                                                    AS paid_ratio,
               CAST(rand(7) * 45 AS INT)                                                                  AS report_lag
        FROM range(5000)
      ),
      claims AS (
        SELECT concat('CLM-', lpad(CAST(id AS STRING), 7, '0'))        AS claim_id,
               concat('POL-', lpad(CAST(id % 1800 AS STRING), 6, '0')) AS policy_id,
               line_of_business, channel, loss_date,
               date_add(loss_date, report_lag)                          AS report_date,
               claim_status,
               incurred_amount,
               round(incurred_amount * paid_ratio, 2)                   AS paid_amount
        FROM base
      )
      SELECT * FROM claims
      UNION ALL SELECT * FROM claims WHERE claim_id <= 'CLM-0000024'
    """)
    print(f"Created synthetic {source}")

n = spark.table(source.replace("`", "")).count()
dbutils.jobs.taskValues.set(key="source_table", value=source)
dbutils.jobs.taskValues.set(key="target_schema", value=target)
print(f"source={source} rows={n} target={catalog}.{target}")
