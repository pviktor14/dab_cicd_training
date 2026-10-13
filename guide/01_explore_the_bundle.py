# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Explore the bundle (8 min)
# MAGIC
# MAGIC **Goal:** understand the two YAML files before you deploy them. Nothing to edit yet – just read.
# MAGIC
# MAGIC Open your bundle folder (step 0) in a second browser tab.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. `databricks.yml` – the bundle and its targets
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: cicd_lab
# MAGIC
# MAGIC include:
# MAGIC   - resources/*.yml            # the job lives in its own file
# MAGIC
# MAGIC variables:
# MAGIC   catalog:  { default: allianz_training }
# MAGIC   schema:   { }                # set per target
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     mode: development
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       schema: cicd_${workspace.current_user.short_name}_dev
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     variables:
# MAGIC       schema: cicd_${workspace.current_user.short_name}_prod
# MAGIC     workspace:  { root_path: /Workspace/Users/<you>/.bundle/cicd_lab/prod }
# MAGIC     presets:    { name_prefix: "[prod <you>] " }
# MAGIC     run_as:     { user_name: <you> }
# MAGIC     permissions: [...]
# MAGIC ```
# MAGIC *(shortened – open the real file)*
# MAGIC
# MAGIC | | dev | prod |
# MAGIC |---|---|---|
# MAGIC | Job name | `[dev <you>] cicd_lab_job` | `[prod <you>] cicd_lab_job` |
# MAGIC | Writes to | `allianz_training.cicd_<you>_dev` | `allianz_training.cicd_<you>_prod` |
# MAGIC | Schedule (daily 06:00) | **paused** automatically | **active** |
# MAGIC | Notebooks | **linked** to your Git folder – edits are live | **copied** at deploy time – frozen until the next deploy |
# MAGIC | Runs as | you | you *(real life: a service principal)* |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `resources/cicd_lab_job.yml` – the job as code
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     cicd_lab_job:                   # the KEY – how the bundle tracks this job
# MAGIC       name: cicd_lab_job            # what you see in the UI (+ prefix per target)
# MAGIC       parameters:                   # arrive in every notebook as widgets
# MAGIC         - name: catalog
# MAGIC           default: ${var.catalog}
# MAGIC         - name: schema
# MAGIC           default: ${var.schema}    # ← different per target
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 6 * * ?"
# MAGIC         timezone_id: Europe/Berlin
# MAGIC       tasks:
# MAGIC         - task_key: bronze
# MAGIC           notebook_task: { notebook_path: ../src/01_bronze.py }
# MAGIC         - task_key: silver
# MAGIC           depends_on: [ { task_key: bronze } ]
# MAGIC           notebook_task: { notebook_path: ../src/02_silver.py }
# MAGIC         - task_key: gold
# MAGIC           depends_on: [ { task_key: silver } ]
# MAGIC           notebook_task: { notebook_path: ../src/03_gold.py }
# MAGIC ```
# MAGIC
# MAGIC * No cluster anywhere → the tasks run on **serverless**.
# MAGIC * Paths are relative to **the YAML file** (`resources/`) → `../src/…`, **with** `.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. The code: notebooks + one tested Python module
# MAGIC
# MAGIC | File | Does |
# MAGIC |---|---|
# MAGIC | `src/01_bronze` | generates 300 orders (fixed seed), some messy → `bronze_orders` |
# MAGIC | `src/02_silver` | applies the rules from `order_rules.py` → `silver_orders` |
# MAGIC | `src/03_gold` | revenue per country × order size → `gold_sales_by_country` |
# MAGIC | `src/order_rules.py` | **pure Python** business rules (`normalize_country`, `order_size`, …) – no Spark |
# MAGIC | `tests/test_order_rules.py` | unit tests for those rules – what CI runs on every pull request |
# MAGIC | `src/04_quality_check` | data checks – not in the job yet (you add it in step 3) |
# MAGIC
# MAGIC 💡 Why a separate `.py` module? Logic in a plain function can be tested in milliseconds, without a cluster. Logic hidden inside a notebook cell can only be tested by running the whole job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✏️ Quick check (answer for yourself, 2 min)
# MAGIC
# MAGIC 1. Which schema will the **prod** job of user `training7` write to?
# MAGIC 2. You edit `src/03_gold` after a dev deploy. Do you need to deploy again before the dev job uses it? And for prod?
# MAGIC 3. Where would you change the time of the daily run?
# MAGIC
# MAGIC <details><summary>Answers</summary>
# MAGIC
# MAGIC 1. `allianz_training.cicd_training7_prod`
# MAGIC 2. Dev: **no** – dev deployments from the workspace are *source-linked* to your Git folder. Prod: **yes** – prod runs a copy made at deploy time. (YAML changes always need a deploy.)
# MAGIC 3. `schedule.quartz_cron_expression` in `resources/cicd_lab_job.yml` – and then deploy. Never in the Jobs UI: the next deploy would overwrite it.
# MAGIC </details>
# MAGIC
# MAGIC ➡️ Continue with `02_deploy_and_run_dev`.
