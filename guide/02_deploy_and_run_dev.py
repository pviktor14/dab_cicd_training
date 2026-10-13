# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Validate, deploy to dev, run (10 min)
# MAGIC
# MAGIC **Goal:** turn the YAML into a real job in the workspace – your private **dev** copy – and run it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validate + deploy to dev
# MAGIC
# MAGIC 1. In **your** bundle folder, click `databricks.yml` – it opens in the editor.
# MAGIC 2. Click the **Deployments** icon (🚀) in the panel on the left of the editor.
# MAGIC 3. In the **Deployments** pane, choose the target **dev**.
# MAGIC 4. Click **Deploy**. The bundle is **validated** first – the validation details appear in a dialog **Deploy to dev**.
# MAGIC 5. Read the dialog (which resources, which CLI version?) → click **Deploy**.
# MAGIC 6. Watch the **Project output** window.
# MAGIC
# MAGIC ✅ **You should now see:** the deploy finishes without errors, and the **Bundle resources** list shows `cicd_lab_job`.
# MAGIC
# MAGIC 🆘 **If it fails**
# MAGIC
# MAGIC | Symptom | Fix |
# MAGIC |---|---|
# MAGIC | **Deploy** greyed out / "Deployment target is not specified" | pick **dev** in the target dropdown |
# MAGIC | no 🚀 icon | you opened a file that is not in a Git folder, or not `databricks.yml` → see step 0 |
# MAGIC | error mentions a line in a `.yml` file | YAML indentation (2 spaces, no tabs) – compare with `solutions/` or re-copy the file |
# MAGIC | `notebook ... not found` | path in `resources/cicd_lab_job.yml` must be `../src/<name>.py` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Run the job
# MAGIC
# MAGIC In **Bundle resources**, click the ▶ (run) icon next to `cicd_lab_job`. Then open the run: **Jobs & Pipelines** (left sidebar) → `[dev <you>] cicd_lab_job` → **Runs**.
# MAGIC
# MAGIC The run takes ~2–4 minutes (serverless start-up + 3 small tasks). While you wait, look at the job page:
# MAGIC
# MAGIC ✅ **You should now see**
# MAGIC - Name **`[dev <your_user_name>] cicd_lab_job`** and a `dev` tag – added by `mode: development`.
# MAGIC - A banner that the job is **managed by a bundle** – editing it in the UI is discouraged; the next deploy would overwrite it.
# MAGIC - **Schedule: paused** – although the YAML has a daily schedule. Dev never fires on its own.
# MAGIC - **Parameters**: `catalog = allianz_training`, `schema = cicd_<you>_dev`.
# MAGIC - Three green tasks: `bronze` → `silver` → `gold`.
# MAGIC
# MAGIC 🆘 **If a task fails:** click the red task → read the error.
# MAGIC `PERMISSION_DENIED … CREATE SCHEMA` → ask your trainer for `CREATE SCHEMA` on `allianz_training`.
# MAGIC `ModuleNotFoundError: order_rules` in `silver` → make sure `order_rules.py` is in `src/` next to `02_silver`; otherwise tell your trainer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Check the result
# MAGIC
# MAGIC Run the next two cells (Serverless).

# COMMAND ----------

import re
me = spark.sql("SELECT current_user()").first()[0]
short = re.sub(r"[^a-z0-9]", "_", me.split("@")[0].lower())
DEV = f"allianz_training.cicd_{short}_dev"
PROD = f"allianz_training.cicd_{short}_prod"
print("dev schema :", DEV)
print("prod schema:", PROD)

# COMMAND ----------

display(spark.sql(f"""
  SELECT order_size, sum(orders) AS orders, round(sum(revenue), 2) AS revenue
  FROM {DEV}.gold_sales_by_country
  GROUP BY order_size ORDER BY order_size
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **You should now see:** three rows `large`, `medium`, `small` – 287 valid orders in total (13 of the 300 raw orders were dropped in silver). Expected: **179 `large`** orders. **Remember this number** – it changes in step 3.
# MAGIC
# MAGIC ## 4. Where did the deployment go?
# MAGIC
# MAGIC | Look at | What you see |
# MAGIC |---|---|
# MAGIC | Workspace → your home → `.bundle/cicd_lab/dev/` | the deployment **state**: the bundle remembers which job ID belongs to the key `cicd_lab_job` |
# MAGIC | the job → task `bronze` → notebook path | points into **your Git folder** (source-linked) |
# MAGIC
# MAGIC 💬 Deploy again without changing anything. How many jobs do you have now? *(Still one – same key = same job, updated in place.)*
# MAGIC
# MAGIC ➡️ Continue with `03_change_and_redeploy`.
