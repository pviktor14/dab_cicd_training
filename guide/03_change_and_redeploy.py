# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · The CI/CD loop: change → test → redeploy (15 min)
# MAGIC
# MAGIC **Story:** the business says *"an order is only **large** from **1 000 €** on, not from 500 €"*. You implement it the way a CI/CD team would:
# MAGIC
# MAGIC | # | Step | Real life: who does it |
# MAGIC |---|---|---|
# MAGIC | A | change the rule in code | you, on a feature branch |
# MAGIC | B | run the unit tests | **CI**, on every pull request |
# MAGIC | C | try it in dev | you (or CD into a dev/test workspace) |
# MAGIC | D | change the job definition (YAML) + redeploy | you + **CD** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## A · ✏️ Change the rule (2 min)
# MAGIC
# MAGIC Open `src/order_rules.py` in **your** bundle and change one line:
# MAGIC
# MAGIC ```python
# MAGIC LARGE_ORDER_THRESHOLD = 1000
# MAGIC ```
# MAGIC
# MAGIC Files in the workspace editor save automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B · Run the unit tests (3 min)
# MAGIC
# MAGIC Open `tests/run_tests` in your bundle → **Run all** (Serverless).
# MAGIC
# MAGIC ✅ **You should now see:** ❌ `test_order_size_boundaries` **FAILED** – the test still says `order_size(500) == "large"`.
# MAGIC
# MAGIC 💬 Is this bad? **No – this is CI doing its job.** The test documents the old rule. In a real pipeline this red test would block the pull request until somebody decides: is the code wrong or is the test outdated? Here the *test* is outdated → update it.
# MAGIC
# MAGIC ✏️ In `tests/test_order_rules.py`, change the boundaries in `test_order_size_boundaries`:
# MAGIC
# MAGIC ```python
# MAGIC     assert order_size(999.99) == "medium"
# MAGIC     assert order_size(1000) == "large"
# MAGIC ```
# MAGIC
# MAGIC Run `tests/run_tests` again.
# MAGIC
# MAGIC ✅ **You should now see:** `All 5 tests passed.`
# MAGIC
# MAGIC 🆘 Still red? Compare with `solutions/step3/src/order_rules.py` and `solutions/step3/tests/test_order_rules.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C · Try it in dev – without deploying (3 min)
# MAGIC
# MAGIC You changed **code**, not YAML. Dev is **source-linked** to your Git folder, so the dev job already uses the new code.
# MAGIC
# MAGIC 1. `databricks.yml` → 🚀 **Deployments** → target **dev** → **Bundle resources** → ▶ next to `cicd_lab_job`. **No deploy.**
# MAGIC 2. When the run is green, run the next two cells.

# COMMAND ----------

import re
me = spark.sql("SELECT current_user()").first()[0]
short = re.sub(r"[^a-z0-9]", "_", me.split("@")[0].lower())
DEV = f"allianz_training.cicd_{short}_dev"

display(spark.sql(f"""
  SELECT order_size, sum(orders) AS orders
  FROM {DEV}.gold_sales_by_country
  GROUP BY order_size ORDER BY order_size
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **You should now see:** fewer `large` orders than in step 2 (expected **93** instead of 179) and more `medium` ones.
# MAGIC
# MAGIC 🆘 Still 179? The run started before your edit, or you edited a different copy (e.g. `solutions/` instead of your bundle). Check the `silver` task output: it prints `LARGE_ORDER_THRESHOLD = …`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## D · ✏️ Change the job itself (YAML) + redeploy (5 min)
# MAGIC
# MAGIC The team wants the job to **fail** when the data looks wrong. The notebook `src/04_quality_check` already exists – wire it into the job.
# MAGIC
# MAGIC In `resources/cicd_lab_job.yml`, replace the `# ✏️ Step 3 …` comment at the end with a 4th task (same indentation as the `- task_key: gold` line):
# MAGIC
# MAGIC ```yaml
# MAGIC         - task_key: quality_check
# MAGIC           depends_on:
# MAGIC             - task_key: gold
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/04_quality_check.py
# MAGIC ```
# MAGIC
# MAGIC 1. Run the job **without** deploying first. 💬 How many tasks does the run have? *(Still 3 – YAML changes only reach the workspace with a deploy.)*
# MAGIC 2. 🚀 **Deployments** → **dev** → **Deploy** → **Deploy** in the dialog. The bundle *updates* the existing job (same key `cicd_lab_job`) – it does not create a second one.
# MAGIC 3. Run the job again.
# MAGIC
# MAGIC ✅ **You should now see:** 4 green tasks; `quality_check` prints four ✅ lines.
# MAGIC
# MAGIC 🆘 **If it fails:** deploy error → indentation (the `-` of the new task must line up with the other `- task_key:` lines). Compare with `solutions/step3/resources/cicd_lab_job.yml`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 Takeaway
# MAGIC
# MAGIC | You changed | Needed for dev | Needed for prod |
# MAGIC |---|---|---|
# MAGIC | code (`.py`, notebooks) | nothing – source-linked | a **deploy** to prod |
# MAGIC | job definition (YAML) | a **deploy** | a **deploy** |
# MAGIC
# MAGIC Every change went through: **edit → test → deploy → run → check**. That loop – automated – *is* CI/CD.
# MAGIC
# MAGIC ⭐ Optional: break the quality check on purpose (e.g. in `04_quality_check` change `> 0` to `> 1000000`), run the job, see the red task, undo.
# MAGIC
# MAGIC ➡️ Continue with `04_promote_to_prod`.