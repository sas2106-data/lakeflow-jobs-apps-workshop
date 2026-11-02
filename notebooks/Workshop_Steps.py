# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Jobs & Databricks Apps workshop — guided, executable steps
# MAGIC
# MAGIC Run this notebook top to bottom on **serverless** compute from the workshop Git folder.
# MAGIC Each module does in code what the facilitator shows in the UI, so attendees can catch up
# MAGIC or re-run any step. Module 3 (DABs) runs from a terminal — see `WORKSHOP_STEPS.md`.
# MAGIC
# MAGIC | Module | What this notebook does |
# MAGIC |---|---|
# MAGIC | 1 · Orchestrate | Creates one Lakeflow Job (setup → bronze → silver → gold) over the claims table in the schema you pass, runs the whole job, then fails and **repairs** a run |
# MAGIC | 2 · Schedule | Adds a cron schedule (paused), timeout, retries and failure e-mail |
# MAGIC | 4 · Apps | Creates and deploys the Streamlit app from `app/` |
# MAGIC | 5 · Permissions | Gives a workspace group `CAN_USE` on the app; grants the app's service principal read access |
# MAGIC
# MAGIC Every resource gets your user name as a suffix, so attendees don't collide.

# COMMAND ----------

# The Apps API needs a recent Databricks SDK (older serverless images ship one without it)
%pip install -q --upgrade "databricks-sdk>=0.40"
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "1 · Catalog")
dbutils.widgets.text("schema", "claims", "2 · Schema with the claims table")
dbutils.widgets.text("claims_table", "claims", "3 · Claims table name")
dbutils.widgets.text("target_schema", "", "4 · Gold schema (blank = <schema>_<you>)")
dbutils.widgets.dropdown("create_sample_data", "false", ["false", "true"], "5 · Synthetic claims if missing")
dbutils.widgets.text("warehouse_id", "", "6 · SQL warehouse ID (for the app)")
dbutils.widgets.text("group_name", "workshop-users", "7 · Group for app access")
dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"], "8 · Delete resources at the end")

# COMMAND ----------

import os, re, time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, apps

w = WorkspaceClient()
me = w.current_user.me().user_name
suffix = re.sub(r"[^a-z0-9]", "-", me.split("@")[0].lower())[:16].strip("-")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")                       # where the claims table lives (read-only)
claims_table = dbutils.widgets.get("claims_table")
target_schema = (dbutils.widgets.get("target_schema").strip()
                 or f'{schema}_{suffix.replace("-", "_")}')   # one bronze/silver/gold schema per attendee
create_sample_data = dbutils.widgets.get("create_sample_data")
warehouse_id = dbutils.widgets.get("warehouse_id").strip() or next(
    (wh.id for wh in w.warehouses.list() if wh.enable_serverless_compute), None)
group_name = dbutils.widgets.get("group_name").strip()

# The Git folder root is two levels up from this notebook: <root>/notebooks/Workshop_Steps
nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
root = os.path.dirname(os.path.dirname(nb_path))
root = root if root.startswith("/Workspace") else f"/Workspace{root}"   # Apps API needs the /Workspace prefix
src = f"{root}/src"

job_name = f"claims-medallion-{suffix}"
app_name = f"claims-hello-{suffix}"[:30].rstrip("-")

print(f"user={me}\nclaims={catalog}.{schema}.{claims_table}\ngold schema={catalog}.{target_schema}\n"
      f"warehouse={warehouse_id}\nrepo root={root}\njob={job_name}\napp={app_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 1 · Orchestrate tasks in one Lakeflow Job
# MAGIC Four notebook tasks on serverless compute (no cluster definition needed):
# MAGIC `setup` (check source, create target schema) → `bronze` (land claims) → `silver` (dedupe, clean) →
# MAGIC `gold` (`gold_claims_by_lob`, `gold_claims_monthly`).
# MAGIC The **schema is a job parameter**: every task receives `catalog`, `schema`, `claims_table` and
# MAGIC `target_schema` as widgets. `bronze` publishes a **task value** (`row_count`) that `gold` reads.

# COMMAND ----------

def nb_task(key, path, upstream=None, **kw):
    return jobs.Task(task_key=key,
                     notebook_task=jobs.NotebookTask(notebook_path=f"{src}/{path}"),
                     depends_on=[jobs.TaskDependency(task_key=upstream)] if upstream else None, **kw)

tasks = [
    nb_task("setup", "00_setup"),
    nb_task("bronze", "01_bronze", "setup"),
    nb_task("silver", "02_silver", "bronze"),
    nb_task("gold", "03_gold", "silver"),
]
params = [jobs.JobParameterDefinition(name=k, default=v) for k, v in {
    "catalog": catalog, "schema": schema, "claims_table": claims_table, "target_schema": target_schema,
    "create_sample_data": create_sample_data,
    "run_date": "{{job.start_time.iso_date}}", "simulate_failure": "false"}.items()]

existing = [j for j in w.jobs.list(name=job_name)]
if existing:
    job_id = existing[0].job_id
    w.jobs.reset(job_id, new_settings=jobs.JobSettings(name=job_name, tasks=tasks, parameters=params,
                                                       max_concurrent_runs=1))
else:
    job_id = w.jobs.create(name=job_name, tasks=tasks, parameters=params, max_concurrent_runs=1).job_id
print(f"Job: {w.config.host}/jobs/{job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the whole job and watch the DAG
# MAGIC Open the link above → **Runs** to see each task go green. To point at a different schema, pass it at
# MAGIC run time: `w.jobs.run_now(job_id, job_parameters={"schema": "<other schema>"})` (UI: **Run now with different settings**).

# COMMAND ----------

run = w.jobs.run_now(job_id, job_parameters={"schema": schema}).result()
for t in run.tasks:
    print(f"{t.task_key:<10} {t.state.result_state.value}")
for tbl in ["bronze_claims", "silver_claims", "gold_claims_by_lob", "gold_claims_monthly"]:
    print(f"{tbl:<22} {spark.table(f'`{catalog}`.`{target_schema}`.{tbl}').count():>7} rows")
display(spark.table(f"`{catalog}`.`{target_schema}`.gold_claims_by_lob").orderBy("line_of_business"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Break it, then **Repair run**
# MAGIC Run again with `simulate_failure=true`: `silver` fails and `gold` is skipped
# MAGIC (*upstream failed*). Repair re-runs **only** those two tasks — `setup` and `bronze` are not repeated.
# MAGIC In the UI this is the **Repair run** button on the failed run.

# COMMAND ----------

from databricks.sdk.errors import OperationFailed

failed_run_id = w.jobs.run_now(job_id, job_parameters={"simulate_failure": "true"}).response.run_id
try:
    w.jobs.wait_get_run_job_terminated_or_skipped(run_id=failed_run_id)
except OperationFailed:
    pass
failed = w.jobs.get_run(failed_run_id)
print("Before repair:", {t.task_key: t.state.result_state.value for t in failed.tasks})

w.jobs.repair_run(run_id=failed_run_id, rerun_tasks=["silver", "gold"],
                  job_parameters={"simulate_failure": "false"}).result()
repaired = w.jobs.get_run(failed_run_id)
print("After repair: ", {t.task_key: t.state.result_state.value for t in repaired.tasks})
print(f"Run: {repaired.run_page_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 2 · Scheduling & triggers
# MAGIC Daily at 06:00 UTC, **paused** so the workshop doesn't create 13 recurring jobs (unpause to see it
# MAGIC fire). Also: one run at a time, a 1-hour timeout, failure e-mail, and a retry on `gold`.
# MAGIC
# MAGIC Other trigger types (Jobs UI → **Add trigger**):
# MAGIC * **File arrival** — start when new files land in a UC volume, e.g. `/Volumes/<catalog>/<schema>/landing/`
# MAGIC * **Table update** — start when upstream tables change
# MAGIC * **Continuous** — restart immediately after each run (streaming-style)

# COMMAND ----------

w.jobs.update(job_id, new_settings=jobs.JobSettings(
    schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="UTC",
                               pause_status=jobs.PauseStatus.PAUSED),
    timeout_seconds=3600,
    email_notifications=jobs.JobEmailNotifications(on_failure=[me]),
    # tasks are merged by task_key, so this only changes `gold`
    tasks=[nb_task("gold", "03_gold", "silver", max_retries=1, min_retry_interval_millis=30000)],
))
s = w.jobs.get(job_id).settings
print(f"schedule={s.schedule.quartz_cron_expression} {s.schedule.timezone_id} ({s.schedule.pause_status.value}), "
      f"timeout={s.timeout_seconds}s, retries(gold)="
      f"{[t.max_retries for t in s.tasks if t.task_key == 'gold'][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 3 · Deploy the job with Databricks Asset Bundles
# MAGIC Runs from a terminal (laptop, or the workspace web terminal). The bundle in `databricks.yml`
# MAGIC describes the same job plus the app:
# MAGIC ```bash
# MAGIC databricks bundle generate job --existing-job-id <job_id>   # compare UI-built job with the repo YAML
# MAGIC export BUNDLE_VAR_catalog=<catalog> BUNDLE_VAR_schema=<claims schema> BUNDLE_VAR_target_schema=<gold schema>
# MAGIC export BUNDLE_VAR_warehouse_id=<id> BUNDLE_VAR_app_name=claims-<you>
# MAGIC databricks bundle validate -t dev
# MAGIC databricks bundle deploy   -t dev
# MAGIC databricks bundle run claims_medallion -t dev     # runs the whole job
# MAGIC databricks bundle run claims_medallion -t dev --params schema=<other schema>   # override at run time
# MAGIC ```
# MAGIC `mode: development` prefixes names with `[dev <user>]` and **pauses the schedule**.

# COMMAND ----------

print(f"databricks bundle generate job --existing-job-id {job_id}\n"
      f"export BUNDLE_VAR_catalog={catalog} BUNDLE_VAR_schema={schema} BUNDLE_VAR_target_schema={target_schema} "
      f"BUNDLE_VAR_warehouse_id={warehouse_id} BUNDLE_VAR_app_name=claims-{suffix[:18]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 4 · Databricks Apps — Streamlit hello world
# MAGIC Creates the app (with the SQL warehouse as a resource) and deploys the code in `app/`
# MAGIC (`app.py` + `app.yaml`). First deployment takes ~2–4 minutes while compute starts.

# COMMAND ----------

wh_resource = [apps.AppResource(name="sql-warehouse", sql_warehouse=apps.AppResourceSqlWarehouse(
    id=warehouse_id, permission=apps.AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE))] if warehouse_id else None

try:
    app = w.apps.get(app_name)
except Exception:
    app = w.apps.create_and_wait(app=apps.App(name=app_name, resources=wh_resource,
                                              description="Workshop — Streamlit hello world"))

deployment = w.apps.deploy_and_wait(app_name=app_name,
                                    app_deployment=apps.AppDeployment(source_code_path=f"{root}/app"))
app = w.apps.get(app_name)
print(f"Deployment: {deployment.status.state.value}\nApp URL:    {app.url}")
print(f"In the app, set 'Gold table' to: {catalog}.{target_schema}.gold_claims_by_lob")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 5 · Permissions (if time)
# MAGIC Two different kinds of access:
# MAGIC 1. **Who can open the app** — `CAN_USE` (or `CAN_MANAGE`) for a user or **group** on the app.
# MAGIC 2. **What the app can read** — Unity Catalog grants to the **app's service principal**
# MAGIC    (or, with user authorization, the signed-in user's own grants).

# COMMAND ----------

group_exists = any(True for _ in w.groups.list(filter=f'displayName eq "{group_name}"'))
if group_exists:
    w.apps.update_permissions(app_name=app_name, access_control_list=[
        apps.AppAccessControlRequest(group_name=group_name, permission_level=apps.AppPermissionLevel.CAN_USE)])
    print(f"Granted CAN_USE on {app_name} to group '{group_name}'")
else:
    print(f"Group '{group_name}' not found — ask an admin to create it (or sync it from Entra ID), then re-run.")

sp = app.service_principal_client_id
for stmt in [f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp}`",
             f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{target_schema}` TO `{sp}`",
             f"GRANT SELECT ON TABLE `{catalog}`.`{target_schema}`.gold_claims_by_lob TO `{sp}`"]:
    spark.sql(stmt)
    print(stmt)

for acl in w.apps.get_permissions(app_name).access_control_list:
    who = acl.group_name or acl.user_name or acl.service_principal_name
    print(f"{who:<40} {[p.permission_level.value for p in acl.all_permissions]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC Set the **Delete resources at the end** widget to `yes` and run this cell.

# COMMAND ----------

if dbutils.widgets.get("cleanup") == "yes":
    w.apps.delete(app_name)
    w.jobs.delete(job_id)
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{target_schema}` CASCADE")   # gold schema only; source untouched
    print("Deleted app, job and schema")
else:
    print("Kept resources (cleanup = no)")
