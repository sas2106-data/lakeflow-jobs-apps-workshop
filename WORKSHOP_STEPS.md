# Lakeflow Jobs & Databricks Apps workshop: step-by-step

**Duration:** 3.5 hours with 2 breaks · **Compute:** serverless · **Plan:** this repo's `README.md`

## What's in this folder

| Path | What it is |
|---|---|
| `databricks.yml` | The bundle: one Lakeflow Job (`claims_medallion`: claims → bronze → silver → gold) and one Streamlit app (`claims_app`), with `dev` and `prod` targets. `schema` is a required variable |
| `src/00_setup.py` … `03_gold.py` | The four job tasks: setup (check claims table, create target schema) → bronze → silver → gold |
| `app/app.py`, `app/app.yaml` | The Streamlit "hello world" app, plus a stretch section that reads `gold_claims_by_lob` |
| `notebooks/Workshop_Steps.py` | **Executable guide.** Runs Modules 1, 2, 4 and 5 in code. Use it for the dry run, for catching up, or as the answer key |

## Before the day (facilitator / platform team)

1. Check the workspace has **serverless jobs**, a **serverless SQL warehouse**, and **Databricks Apps** enabled.
2. Create (or sync from Entra ID) the workspace group **`workshop-users`**, and add the attendees to it.
3. Give attendees these Unity Catalog grants:
   - `USE CATALOG` on the catalog;
   - `USE SCHEMA` and `SELECT` on the schema that holds the claims table;
   - `CREATE SCHEMA` for the gold schema, or pre-create one per attendee.
4. Push this repo to your Git provider (or fork it), and have each attendee clone it as a **Git folder** in the workspace.
5. Decide how attendees run the **Databricks CLI**: installed on their laptop, the workspace **web terminal**, or deploying the bundle from the Git folder in the workspace.
6. **Dry run:**
   - Open `notebooks/Workshop_Steps` and set the widgets: `catalog`, `schema` (where the claims table lives), `claims_table`, and `warehouse_id`.
   - Click **Run all**. It takes about 10–15 minutes and ends with a working app URL.
   - Then set `cleanup = yes` and run the last cell. This drops only the gold schema; the source is untouched.

> **Source data.** The job reads `{catalog}.{schema}.{claims_table}`, and `schema` is a **parameter**. It expects these columns:
> `claim_id, policy_id, line_of_business, channel, loss_date, report_date, claim_status, incurred_amount, paid_amount`.
> If your claims table uses other names, change only the `SELECT` in `src/02_silver.py`.
> For a dry run without real data, set `create_sample_data = true`. If the claims table doesn't exist, `00_setup` creates a synthetic one (about 5,000 claims, with duplicates and messy status values).

**Tables the job writes** (into `target_schema`, which defaults to `schema`; the guided notebook defaults it to `<schema>_<you>`):

| Layer | Table | Contents |
|---|---|---|
| Bronze | `bronze_claims` | The claims table as-is, plus `_loaded_at`, `_run_date` and `_source` |
| Silver | `silver_claims` | Duplicates removed (latest row per `claim_id`), status standardised, invalid rows dropped, plus `outstanding_amount` and `report_lag_days` |
| Gold | `gold_claims_by_lob` | Claims, open claims, incurred, paid, outstanding, average severity and average report lag per line of business |
| Gold | `gold_claims_monthly` | Claims and amounts by loss month and line of business |

---

## 0:00 · Welcome & context (15 min)

- Where Lakeflow Jobs and Databricks Apps sit in the lakehouse: jobs move and shape the data, and apps put it in front of users. Both are governed by Unity Catalog.
- Walk through the repo layout above and the source table in Catalog Explorer.

## 0:15 · Module 1 — Load claims to gold in one Lakeflow Job (40 min, hands-on)

**In the UI** (Jobs & Pipelines → **Create job**):

1. Name the job `claims-medallion-<you>`.
2. Add the four tasks. For each one choose **Type: Notebook**, set the path under `src/`, and leave compute on **Serverless**:

   | Task | Notebook | Depends on |
   |---|---|---|
   | `setup` | `00_setup` | — |
   | `bronze` | `01_bronze` | `setup` |
   | `silver` | `02_silver` | `bronze` |
   | `gold` | `03_gold` | `silver` |

3. Go to **Job details → Parameters** and add these. Job parameters reach every task as widgets.

   | Parameter | Value |
   |---|---|
   | `catalog` | `main` (or your catalog) |
   | `schema` | **the schema that holds the claims table** |
   | `claims_table` | `claims` |
   | `target_schema` | `<schema>_<you>`, where bronze, silver and gold are written |
   | `create_sample_data` | `false` (`true` only for a dry run without real data) |
   | `run_date` | `{{job.start_time.iso_date}}` |
   | `simulate_failure` | `false` |

4. **Run the whole job:**
   - Click **Run now**, open the run, and watch the graph. Click a task to see its output.
   - Check the four tables in Catalog Explorer under `target_schema`.
5. **A different schema, same job:** use **Run now with different settings** and change `schema`. The tasks read whatever schema you pass. There's nothing to edit.
6. **Task values:** `bronze` publishes `row_count`, and `gold` reads it. You can see it in the `bronze_rows` column of `gold_claims_by_lob`.
7. **Repair run:**
   - Click **Run now with different settings** and set `simulate_failure = true`. `silver` fails and `gold` shows *Upstream failed*.
   - Click **Repair run**, set `simulate_failure = false`, and run it.
   - Only `silver` and `gold` re-run. `setup` and `bronze` keep their earlier results.

**Same thing in code:** Module 1 cells in `Workshop_Steps`.

## 0:55 · Module 2 — Scheduling & triggers (25 min, hands-on)

1. Go to **Schedules & Triggers → Add trigger → Scheduled**:
   - **Advanced** → cron `0 0 6 * * ?`;
   - timezone **UTC** (or your local zone);
   - save it, then **pause** it.
2. Go to **Job details**:
   - **Max concurrent runs = 1**;
   - **Timeout = 1 h**;
   - **Notifications → On failure → your e-mail**.
3. Open the `gold` task and set **Retries = 1**, interval 30 s.
4. Discuss the other trigger types:
   - **File arrival:** start when new files land in a UC volume. This suits file drops from upstream systems.
   - **Table update:** start when the claims table changes. This is a natural fit for this job.
   - **Continuous:** for streaming-style workloads.

**Same thing in code:** Module 2 cell.

## 1:20 · Break (15 min)

## 1:35 · Module 3 — Deploy the job with DABs (40 min, hands-on)

From a terminal in the cloned repo:

```bash
databricks auth login --host https://<workspace-url>           # once
databricks bundle generate job --existing-job-id <your job id>  # turns your UI-built job into YAML
#   compare resources/*.yml it writes with claims_medallion in databricks.yml, then delete the generated file

# set the bundle variables once per terminal (works in bash and zsh)
export BUNDLE_VAR_catalog=main
export BUNDLE_VAR_schema=<claims schema>                 # the schema that holds the claims table
export BUNDLE_VAR_target_schema=<claims schema>_<you>    # where bronze / silver / gold are written
export BUNDLE_VAR_warehouse_id=<warehouse id>
export BUNDLE_VAR_app_name=claims-<you>              # lowercase, digits, hyphens; max 30 chars

databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run claims_medallion -t dev            # runs the whole job: setup → bronze → silver → gold

# same job, another schema, no redeploy (comma-separated job parameters)
databricks bundle run claims_medallion -t dev --params schema=<other schema>,target_schema=<other schema>_<you>
```

> **Tip:** `--var="schema=…"` on each command works too, as does putting the values under `targets.dev.variables`. The `export` form saves retyping them.

Things to point out:

- In the workspace the job appears as **`[dev <you>] claims-medallion`**. Development mode prefixes resource names and **pauses the schedule**.
- Edit a task notebook in Git, then run `bundle deploy` again. The job updates in place.
- **Promotion:** `prod` uses the same files. In a typical setup, CI (Azure DevOps or GitHub Actions) runs `databricks bundle deploy -t prod` as a **service principal** after a merge, with prod variables such as `catalog=main`, `schema=claims` and `target_schema=claims_gold`.

## 2:15 · Module 4 — Databricks Apps: Streamlit hello world, deployed with DABs (40 min, hands-on)

1. **UI first:**
   - Go to **Compute → Apps → Create app → Streamlit → Hello world**.
   - Name it `claims-hello-<you>` and click **Create**.
   - When it's running, open the URL and look at the **Logs** tab.
2. **Our app:** look at `app/app.yaml`. The `command` runs Streamlit. `DATABRICKS_WAREHOUSE_ID` comes from the app's `sql-warehouse` resource.
3. Edit `app/app.py`, for example the title or the greeting.
4. **Deploy with the bundle.** The `claims_app` app is already in `databricks.yml`:
   ```bash
   databricks bundle deploy -t dev                # creates or updates the app definition and uploads the code
   databricks bundle run claims_app -t dev        # deploys the code and starts the app
   ```
5. **Stretch:**
   - In the app, set **Gold table** to `<catalog>.<target_schema>.gold_claims_by_lob`.
   - It fails until Module 5 grants the app's service principal read access. That's the lead-in to Module 5.

**Same thing in code:** Module 4 cells. They create the app and deploy the `app/` folder.

## 2:55 · Break (10 min)

## 3:05 · Module 5 — App permissions with a Databricks group (15 min, if time)

There are two separate kinds of access:

1. **Who can open the app:**
   - In the UI: app → **Permissions** → add group `workshop-users` → **Can use**.
   - In the bundle: the same grant lives in the `permissions:` block of `claims_app`. Run `bundle deploy` again.
   - Test it: a colleague in the group can open the app; someone outside the group gets *access denied*.
2. **What the app can read:** the app runs as its own **service principal** (shown on the app's **Authorization** tab). Grant that service principal access in Unity Catalog:
   ```sql
   GRANT USE CATALOG ON CATALOG <catalog> TO `<app service principal id>`;
   GRANT USE SCHEMA  ON SCHEMA  <catalog>.<target_schema> TO `<app service principal id>`;
   GRANT SELECT      ON TABLE   <catalog>.<target_schema>.gold_claims_by_lob TO `<app service principal id>`;
   ```
   Refresh the app. The gold KPIs, chart and table now appear.
3. **Optional mention:** with **user authorization** (on-behalf-of the user), the app queries with each user's own Unity Catalog permissions instead of the service principal's.

**Same thing in code:** Module 5 cell.

## 3:20 · Recap & next steps (10 min)

- The path: build in the UI → capture it as a bundle → deploy to dev → CI deploys to prod as a service principal.
- Clean up: `databricks bundle destroy -t dev`, and run the notebook's clean-up cell with `cleanup = yes`.

---

## Recommendation: Genie App Builder ("Genie for Apps")

**Keep it off for this workshop and in production workspaces for now. Evaluate it in a sandbox workspace first.**

Genie App Builder is turned on by the workspace preview **Governed agentic app-building**, which is in **Beta**. It generates an app from a natural-language prompt inside an **App Space**.

**Why not now:**
- It builds AppKit (TypeScript/React) apps, not the code-first Streamlit apps with DABs that this workshop covers.
- Workspace Secure Egress Gateway and serverless Private Link settings don't apply to apps it builds. Your security team needs to review that first.
- It needs partner-powered AI on at account and workspace level, plus cross-Geo processing where the Geo requires it.
- The same toggle turns on App Spaces, a new governance boundary that should be designed deliberately.

**Why it's worth a follow-up:**
- App Spaces give the platform team central control over app permissions, data scopes and spend.
- Business users can build internal apps without code, and apps can run as the signed-in user.
- There's no charge during Beta.

**Next step:** enable it in a sandbox workspace in a supported region, then hold a 30-minute session with the platform and security teams.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `bundle validate` says *no value assigned to required variable* | Export `BUNDLE_VAR_schema` and `BUNDLE_VAR_warehouse_id` in this terminal (warehouse ID: SQL Warehouses → Connection details). In zsh, `V="--var=… --var=…"; … $V` silently passes nothing. |
| `bundle deploy` fails because the app name already exists | Set `BUNDLE_VAR_app_name=claims-<you>`. Dev mode prefixes job names but **not** app names. |
| The app shows "Couldn't read …" | The app's service principal is missing a grant from Module 5. Also check that the table name in the app is right. |
| The app deployment hangs or fails | Open the app's **Logs** tab. A typo in `app.yaml` or an import error in `app.py` shows up there. |
| `setup` fails with *Claims table … not found* | Check the `catalog`, `schema` and `claims_table` parameters, and your `SELECT` grant. For a dry run, use `create_sample_data=true`. |
| `silver` fails with *cannot resolve column* | Your claims table uses different column names. Map them in the `SELECT` in `src/02_silver.py`. |
| Two attendees overwrite each other | Everyone needs their own `target_schema` and app name. The guided notebook adds your user name to both automatically. The source schema is shared and only read. |
