# Lakeflow Jobs & Databricks Apps — hands-on workshop

A 3.5-hour, hands-on workshop on **Lakeflow Jobs** (multi-task orchestration and scheduling), **Databricks Apps** (a Streamlit "hello world"), and deploying both with **Databricks Asset Bundles (DABs)**.

**The scenario:** one scheduled Lakeflow Job takes a `claims` table through **bronze → silver → gold**. The source schema is passed in as a **job parameter**. A Streamlit app shows the gold KPIs and is shared with a workspace group.

```
Git repo (this bundle) ──databricks bundle deploy──▶ workspace
                                                      ├─ Lakeflow Job  claims_medallion   (cron, paused in dev)
                                                      │    setup → bronze → silver → gold   ← param: schema
                                                      │                         └─▶ gold_claims_by_lob, gold_claims_monthly
                                                      └─ Databricks App  claims_app (Streamlit) ── CAN_USE ─▶ workspace group
```

## What's here

| Path | What it is |
|---|---|
| `databricks.yml` | The bundle: job `claims_medallion` and app `claims_app`, with `dev` and `prod` targets |
| `src/00_setup.py` … `03_gold.py` | The job tasks: check the source and create the target schema → bronze → silver (dedupe, clean) → gold |
| `app/` | The Streamlit app (`app.py`, `app.yaml`). Stretch: it reads `gold_claims_by_lob` through a SQL warehouse |
| `notebooks/Workshop_Steps.py` | **Executable guide.** Builds the job in code, runs it, breaks and repairs a run, schedules it, deploys the app and sets permissions. Use it for dry runs or as the answer key |
| `WORKSHOP_STEPS.md` | Facilitator and attendee instructions: UI steps per module, commands and troubleshooting |

## Agenda (3.5 hours)

| Time | Module |
|---|---|
| 0:00–0:15 | Welcome and context |
| 0:15–0:55 | **1 · Load claims to gold in one Lakeflow Job**: dependent tasks, job parameters, task values, Repair run |
| 0:55–1:20 | **2 · Scheduling and triggers**: cron and timezone, retries, timeout, notifications; scheduled vs file-arrival vs table-update triggers |
| 1:20–1:35 | Break |
| 1:35–2:15 | **3 · Deploy the job with DABs**: `bundle generate`, `validate`, `deploy`, `run`; dev vs prod targets |
| 2:15–2:55 | **4 · Streamlit hello world on Databricks Apps**, then deploy it with DABs |
| 2:55–3:05 | Break |
| 3:05–3:20 | **5 · App permissions (if time)**: a group gets `CAN_USE`; the app's service principal gets Unity Catalog grants |
| 3:20–3:30 | Recap |

## Quick start

Prerequisites:
- a workspace with Unity Catalog, serverless jobs, a serverless SQL warehouse and Databricks Apps;
- a current Databricks CLI (`databricks auth login --host https://<workspace-url>`).

```bash
export BUNDLE_VAR_catalog=main
export BUNDLE_VAR_schema=claims                    # schema that holds the claims table
export BUNDLE_VAR_target_schema=claims_<you>       # where bronze / silver / gold are written
export BUNDLE_VAR_warehouse_id=<sql warehouse id>
export BUNDLE_VAR_app_name=claims-<you>            # unique per person; lowercase, max 30 chars
export BUNDLE_VAR_group_name=<workspace group>     # who may open the app

databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run claims_medallion -t dev      # run the whole job
databricks bundle run claims_app -t dev            # deploy and start the app
```

**No claims data yet?** Add `--params create_sample_data=true` to the first job run. If the claims table doesn't exist, `00_setup` creates a synthetic one (about 5,000 claims, with planted duplicates and messy status values).

**Your own claims table:** the job expects these columns:
`claim_id, policy_id, line_of_business, channel, loss_date, report_date, claim_status, incurred_amount, paid_amount`.
If yours differ, change only the `SELECT` in `src/02_silver.py`.

**Without a terminal:** import the repo as a Git folder, open `notebooks/Workshop_Steps`, set the widgets, and click **Run all**.

## Notes

- Use `export BUNDLE_VAR_…` rather than `V="--var=… --var=…"; … $V`. zsh doesn't word-split `$V`, so the variables are silently dropped.
- Dev mode prefixes **job** names per user and pauses schedules. It does **not** prefix **app** names, so set `app_name` per person.
- Clean up with `databricks bundle destroy -t dev`, and run the notebook's last cell with `cleanup = yes`.

All data in this repo is synthetic.
