# Databricks notebook source
# MAGIC %md
# MAGIC # Claims → Gold → Genie → App — guided, executable steps
# MAGIC
# MAGIC Run top to bottom on **serverless** compute from the workshop Git folder. Each module does in code what the
# MAGIC facilitator shows in the UI, so attendees can catch up or re-run any step. Module 5 (DABs + Git check-in)
# MAGIC runs from a terminal — see `WORKSHOP_STEPS.md`.
# MAGIC
# MAGIC | Module | What this notebook does |
# MAGIC |---|---|
# MAGIC | 1 · Connect | Points at your claims table, or creates a synthetic one |
# MAGIC | 2 · Lakeflow Job | Creates and runs one job: setup → bronze → silver → gold; repairs a failed run; adds a schedule |
# MAGIC | 3 · Genie | Creates a Genie Agent (Genie space) on the gold tables, with instructions and example SQL |
# MAGIC | 4 · App | Creates a Streamlit app with the Genie Agent as a resource and **user authorization** (`genie` scope) |
# MAGIC | 6 · Access control | Unity Catalog grants, Genie Agent `CAN RUN`, app `CAN USE` for a group |
# MAGIC
# MAGIC Every resource gets your user name as a suffix, so attendees don't collide.

# COMMAND ----------

# The Genie and Apps APIs need a recent Databricks SDK (older serverless images ship an older one)
%pip install -q --upgrade "databricks-sdk>=0.60"
dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "1 · Catalog")
dbutils.widgets.text("schema", "claims", "2 · Schema with the claims table")
dbutils.widgets.text("claims_table", "claims", "3 · Claims table name")
dbutils.widgets.text("target_schema", "", "4 · Gold schema (blank = <schema>_<you>)")
dbutils.widgets.dropdown("create_sample_data", "false", ["false", "true"], "5 · Synthetic claims if missing")
dbutils.widgets.text("warehouse_id", "", "6 · SQL warehouse ID (blank = first serverless)")
dbutils.widgets.text("group_name", "workshop-users", "7 · Group that gets access")
dbutils.widgets.dropdown("cleanup", "no", ["no", "yes"], "8 · Delete resources at the end")

# COMMAND ----------

import json, os, re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, apps

w = WorkspaceClient()
me = w.current_user.me().user_name
suffix = re.sub(r"[^a-z0-9]", "-", me.split("@")[0].lower())[:16].strip("-")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")                      # where the claims table lives (read-only)
claims_table = dbutils.widgets.get("claims_table")
target_schema = (dbutils.widgets.get("target_schema").strip()
                 or f'{schema}_{suffix.replace("-", "_")}')  # one bronze/silver/gold schema per attendee
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
genie_title = f"Claims Gold — Genie ({suffix})"
app_name = f"claims-genie-{suffix}"[:30].rstrip("-")
gold = f"{catalog}.{target_schema}"

print(f"user={me}\nclaims={catalog}.{schema}.{claims_table}\ngold schema={gold}\n"
      f"warehouse={warehouse_id}\nrepo root={root}\njob={job_name}\napp={app_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 1 · Connect to the claims table (or generate synthetic data)
# MAGIC The job's `setup` task checks `{catalog}.{schema}.{claims_table}`. If it's missing and
# MAGIC `create_sample_data = true`, setup creates ~5,000 synthetic claims (with planted duplicates and messy statuses).
# MAGIC Expected columns: `claim_id, policy_id, line_of_business, channel, loss_date, report_date, claim_status,
# MAGIC incurred_amount, paid_amount` — if yours differ, change only the `SELECT` in `src/02_silver.py`.

# COMMAND ----------

src_table = f"{catalog}.{schema}.{claims_table}"
if spark.catalog.tableExists(src_table):
    print(f"Found {src_table}: {spark.table(src_table).count()} rows")
    display(spark.table(src_table).limit(5))
elif create_sample_data == "true":
    print(f"{src_table} not found — the job's setup task will create a synthetic table.")
else:
    raise ValueError(f"{src_table} not found. Fix the widgets, or set 'Synthetic claims if missing' to true.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 2 · Lakeflow Job: claims → gold
# MAGIC Four notebook tasks on serverless compute (no cluster definition needed): `setup` → `bronze` (land as-is) →
# MAGIC `silver` (dedupe, clean) → `gold` (`gold_claims_by_lob`, `gold_claims_monthly`).
# MAGIC The **schema is a job parameter**: every task receives `catalog`, `schema`, `claims_table` and `target_schema`.

# COMMAND ----------

def nb_task(key, path, upstream=None, **kw):
    return jobs.Task(task_key=key,
                     notebook_task=jobs.NotebookTask(notebook_path=f"{src}/{path}"),
                     depends_on=[jobs.TaskDependency(task_key=upstream)] if upstream else None, **kw)

tasks = [nb_task("setup", "00_setup"), nb_task("bronze", "01_bronze", "setup"),
         nb_task("silver", "02_silver", "bronze"),
         nb_task("gold", "03_gold", "silver", max_retries=1, min_retry_interval_millis=30000)]
params = [jobs.JobParameterDefinition(name=k, default=v) for k, v in {
    "catalog": catalog, "schema": schema, "claims_table": claims_table, "target_schema": target_schema,
    "create_sample_data": create_sample_data,
    "run_date": "{{job.start_time.iso_date}}", "simulate_failure": "false"}.items()]
settings = dict(name=job_name, tasks=tasks, parameters=params, max_concurrent_runs=1, timeout_seconds=3600,
                email_notifications=jobs.JobEmailNotifications(on_failure=[me]),
                # daily 06:00 UTC, paused so the workshop doesn't create recurring runs — unpause to try it
                schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 * * ?", timezone_id="UTC",
                                           pause_status=jobs.PauseStatus.PAUSED))

existing = list(w.jobs.list(name=job_name))
if existing:
    job_id = existing[0].job_id
    w.jobs.reset(job_id, new_settings=jobs.JobSettings(**settings))
else:
    job_id = w.jobs.create(**settings).job_id
print(f"Job: {w.config.host}/jobs/{job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the whole job
# MAGIC Open the link above → **Runs** to watch each task go green (a few minutes on first start).
# MAGIC To use another schema: `w.jobs.run_now(job_id, job_parameters={"schema": "<other>"})`
# MAGIC (UI: **Run now with different settings**).

# COMMAND ----------

run = w.jobs.run_now(job_id).result()
for t in run.tasks:
    print(f"{t.task_key:<8} {t.state.result_state.value}")
for tbl in ["bronze_claims", "silver_claims", "gold_claims_by_lob", "gold_claims_monthly"]:
    print(f"{tbl:<22} {spark.table(f'{gold}.{tbl}').count():>7} rows")
display(spark.table(f"{gold}.gold_claims_by_lob").orderBy("line_of_business"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Break it, then **Repair run**
# MAGIC `simulate_failure=true` makes `silver` fail and `gold` is skipped. Repair re-runs only those two tasks.

# COMMAND ----------

from databricks.sdk.errors import OperationFailed

failed_run_id = w.jobs.run_now(job_id, job_parameters={"simulate_failure": "true"}).response.run_id
try:
    w.jobs.wait_get_run_job_terminated_or_skipped(run_id=failed_run_id)
except OperationFailed:
    pass
print("Before repair:", {t.task_key: t.state.result_state.value for t in w.jobs.get_run(failed_run_id).tasks})
w.jobs.repair_run(run_id=failed_run_id, rerun_tasks=["silver", "gold"],
                  job_parameters={"simulate_failure": "false"}).result()
print("After repair: ", {t.task_key: t.state.result_state.value for t in w.jobs.get_run(failed_run_id).tasks})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 3 · Genie Agent (Genie space) on the gold tables
# MAGIC In the UI: **Genie** → **New** → add the two gold tables → add instructions, example SQL and sample questions.
# MAGIC Here we create the same agent through the API. Genie needs a **pro or serverless SQL warehouse**, and
# MAGIC **partner-powered AI** must be on for the account and workspace.

# COMMAND ----------

nid = iter(f"{i:032x}" for i in range(1, 100))   # Genie needs 32-hex ids, sorted within each list
space = {
    "version": 2,
    "config": {"sample_questions": [{"id": next(nid), "question": [q]} for q in [
        "Which line of business has the most open claims?",
        "Show incurred vs paid by line of business",
        "How did monthly claim counts trend over the last 12 months?",
        "What is the average severity for Motor?"]]},
    "data_sources": {"tables": [
        {"identifier": f"{gold}.gold_claims_by_lob",
         "description": ["One row per line of business: claims, open claims, incurred, paid, outstanding, "
                         "average severity and report lag."]},
        {"identifier": f"{gold}.gold_claims_monthly",
         "description": ["Claim counts and incurred/paid amounts by loss month and line of business."]}]},
    "instructions": {
        "text_instructions": [{"id": next(nid), "content": [
            "You answer questions about insurance claims from the gold tables only.\n"
            "- 'Open claims' = claim_status is not 'closed' (column open_claims).\n"
            "- 'Severity' = average incurred amount per claim (avg_severity).\n"
            "- 'Outstanding' = incurred_amount - paid_amount (outstanding_amount).\n"
            "- For trends use gold_claims_monthly ordered by loss_month; for totals by line of business use gold_claims_by_lob.\n"
            "- Round money to whole units and ratios to one decimal."]}],
        "example_question_sqls": [
            {"id": next(nid), "question": ["Which line of business has the most open claims?"],
             "sql": [f"SELECT line_of_business, open_claims, claims FROM {gold}.gold_claims_by_lob ORDER BY open_claims DESC"]},
            {"id": next(nid), "question": ["Paid-to-incurred ratio by line of business"],
             "sql": [f"SELECT line_of_business, ROUND(100 * paid_amount / incurred_amount, 1) AS paid_to_incurred_pct "
                     f"FROM {gold}.gold_claims_by_lob ORDER BY paid_to_incurred_pct DESC"]}]},
}
body = {"title": genie_title, "warehouse_id": warehouse_id, "serialized_space": json.dumps(space),
        "description": "Ask questions about claims KPIs by line of business and by loss month (gold layer)."}

found = [s for s in w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", []) if s.get("title") == genie_title]
if found:
    space_id = found[0]["space_id"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body)
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body=body)["space_id"]
print(f"Genie Agent: {w.config.host}/genie/rooms/{space_id}")

# Smoke test: ask one question as you (the notebook user)
msg = w.genie.start_conversation_and_wait(space_id, "Which line of business has the most open claims?")
for att in msg.attachments or []:
    if att.query:
        print("Genie SQL:", att.query.query)
    if att.text:
        print("Genie says:", att.text.content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 4 · App with Genie
# MAGIC The Streamlit app in `app/` chats with the Genie Agent through the Genie Conversation API.
# MAGIC * **Resources:** the Genie Agent (`genie-space`, CAN RUN) and the SQL warehouse (`sql-warehouse`, CAN USE),
# MAGIC   injected as env vars by `app.yaml`.
# MAGIC * **User authorization:** scope `genie`, so Genie runs as the signed-in user with *their* Unity Catalog
# MAGIC   permissions. Users consent once when they first open the app.
# MAGIC
# MAGIC This does **not** need the Genie App Builder preview — that's a separate, no-code way of building apps.

# COMMAND ----------

resources = [
    apps.AppResource(name="genie-space", genie_space=apps.AppResourceGenieSpace(
        name="claims-genie", space_id=space_id, permission=apps.AppResourceGenieSpaceGenieSpacePermission.CAN_RUN)),
    apps.AppResource(name="sql-warehouse", sql_warehouse=apps.AppResourceSqlWarehouse(
        id=warehouse_id, permission=apps.AppResourceSqlWarehouseSqlWarehousePermission.CAN_USE)),
]
spec = apps.App(name=app_name, description="Claims Genie — ask questions about gold claims data",
                resources=resources, user_api_scopes=["genie"])
try:
    w.apps.get(app_name)
    w.apps.update(name=app_name, app=spec)
except Exception:
    w.apps.create_and_wait(app=spec)

deployment = w.apps.deploy_and_wait(app_name=app_name,
                                    app_deployment=apps.AppDeployment(source_code_path=f"{root}/app"))
app = w.apps.get(app_name)
print(f"Deployment: {deployment.status.state.value}\nApp URL:    {app.url}\nScopes:     {app.user_api_scopes}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 5 · DABs + Git check-in (terminal)
# MAGIC The bundle (`databricks.yml` + `resources/*.yml`) declares the same job, Genie Agent and app:
# MAGIC ```bash
# MAGIC export BUNDLE_VAR_catalog=<catalog> BUNDLE_VAR_schema=<claims schema> BUNDLE_VAR_target_schema=<gold schema>
# MAGIC export BUNDLE_VAR_warehouse_id=<id> BUNDLE_VAR_app_name=claims-genie-<you> BUNDLE_VAR_group_name=<group>
# MAGIC databricks bundle validate -t dev && databricks bundle deploy -t dev
# MAGIC databricks bundle run claims_medallion -t dev && databricks bundle run claims_app -t dev
# MAGIC git checkout -b feature/<you> && git add -A && git commit -m "Claims Genie app" && git push -u origin HEAD
# MAGIC ```

# COMMAND ----------

print(f"databricks bundle generate job --existing-job-id {job_id}\n"
      f"databricks bundle generate genie-space --existing-id {space_id}\n"
      f"databricks bundle generate app --existing-app-name {app_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Module 6 · Authorization & access control
# MAGIC Three layers, all granted to one group:
# MAGIC
# MAGIC | Layer | Grant | Why |
# MAGIC |---|---|---|
# MAGIC | Data (Unity Catalog) | `USE CATALOG`, `USE SCHEMA`, `SELECT` on gold | Genie always evaluates data access as the end user |
# MAGIC | Genie Agent | `CAN RUN` | Lets users ask questions (the warehouse uses the author's embedded credentials) |
# MAGIC | App | `CAN USE` | Lets users open the app |
# MAGIC
# MAGIC Identity: with **user authorization** the app calls Genie as the signed-in user, so row filters and column
# MAGIC masks apply per user, and Genie usage counts as user usage. Without it, every user shares the app's service
# MAGIC principal, which then needs its own grants.

# COMMAND ----------

# Unity Catalog grants need an ACCOUNT-level group (e.g. synced from Entra ID); workspace-local groups are rejected.
# The app's service principal is granted too, as the fallback when user authorization is off.
for p in [group_name, app.service_principal_client_id]:
    for stmt in [f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{p}`",
                 f"GRANT USE SCHEMA ON SCHEMA `{catalog}`.`{target_schema}` TO `{p}`",
                 f"GRANT SELECT ON SCHEMA `{catalog}`.`{target_schema}` TO `{p}`"]:
        try:
            spark.sql(stmt)
            print("OK  ", stmt)
        except Exception as e:
            print("SKIP", stmt, "→", str(e).splitlines()[0][:160])
            print("     Is this an account-level group? Workspace-local groups can't receive Unity Catalog grants.")
            break

# Genie Agent and app permissions accept account groups and workspace groups
group_exists = any(True for _ in w.groups.list(filter=f'displayName eq "{group_name}"')) or group_name == "account users"
if group_exists:
    w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{space_id}", body={
        "access_control_list": [{"group_name": group_name, "permission_level": "CAN_RUN"}]})
    w.apps.update_permissions(app_name=app_name, access_control_list=[
        apps.AppAccessControlRequest(group_name=group_name, permission_level=apps.AppPermissionLevel.CAN_USE)])
    print(f"Group '{group_name}': CAN RUN on the Genie Agent, CAN USE on the app")
else:
    print(f"Group '{group_name}' not found — ask an admin to create it (or sync it from Entra ID), then re-run.")

for acl in w.apps.get_permissions(app_name).access_control_list:
    who = acl.group_name or acl.user_name or acl.service_principal_name
    print(f"app   {who:<40} {[p.permission_level.value for p in acl.all_permissions]}")
for acl in w.api_client.do("GET", f"/api/2.0/permissions/genie/{space_id}").get("access_control_list", []):
    who = acl.get("group_name") or acl.get("user_name") or acl.get("service_principal_name")
    print(f"genie {who:<40} {[p['permission_level'] for p in acl.get('all_permissions', [])]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optional: prove per-user access with a row filter
# MAGIC With user authorization, a Unity Catalog row filter on the gold table changes what each user sees in the
# MAGIC app — with no app code changes. Example (members of `<group>` see only Motor and Property):
# MAGIC ```sql
# MAGIC CREATE FUNCTION <gold>.lob_filter(lob STRING) RETURN
# MAGIC   NOT is_account_group_member('<group>') OR lob IN ('Motor', 'Property');
# MAGIC ALTER TABLE <gold>.gold_claims_by_lob SET ROW FILTER <gold>.lob_filter ON (line_of_business);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean up
# MAGIC Set **Delete resources at the end** to `yes` and run this cell (drops only the gold schema; the source is untouched).

# COMMAND ----------

if dbutils.widgets.get("cleanup") == "yes":
    w.apps.delete(app_name)
    w.api_client.do("DELETE", f"/api/2.0/genie/spaces/{space_id}")
    w.jobs.delete(job_id)
    spark.sql(f"DROP SCHEMA IF EXISTS `{catalog}`.`{target_schema}` CASCADE")
    print("Deleted app, Genie Agent, job and gold schema")
else:
    print("Kept resources (cleanup = no)")
