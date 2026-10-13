# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Promote to prod (8 min)
# MAGIC
# MAGIC **Goal:** deploy the **same** bundle – no file changes – to the **prod** target, and see what is different.
# MAGIC
# MAGIC > In real life there is *one* prod, deployed only by the CI/CD pipeline as a **service principal** after a merge to `main`. Nobody clicks *Deploy to prod*. Here 14 people share one workspace, so everybody deploys their own `[prod <you>]` copy – step 5 shows the real thing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Deploy to prod
# MAGIC
# MAGIC 1. Open `databricks.yml` → 🚀 **Deployments**.
# MAGIC 2. Target: **prod** → **Deploy** → read the **Deploy to prod** dialog → **Deploy**.
# MAGIC 3. **Bundle resources** (target prod) → ▶ next to `cicd_lab_job`.
# MAGIC
# MAGIC ✅ **You should now see:** in **Jobs & Pipelines** a second job **`[prod <your_user_name>] cicd_lab_job`** – 4 tasks, run green after ~3 min.
# MAGIC
# MAGIC 🆘 **If the deploy fails**
# MAGIC
# MAGIC | Message (roughly) | Meaning / fix |
# MAGIC |---|---|
# MAGIC | `target with 'mode: production' must set 'workspace.root_path'…` | someone removed `root_path` from the prod target – put it back (`solutions/` / original file) |
# MAGIC | permission error on `run_as` / service principal | you changed `run_as` – keep `user_name: ${workspace.current_user.userName}` |
# MAGIC | you cannot pick **prod** at all | ask the trainer – prod is then shown as a trainer demo |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Compare dev and prod
# MAGIC
# MAGIC Open both jobs side by side.
# MAGIC
# MAGIC | | `[dev <you>] cicd_lab_job` | `[prod <you>] cicd_lab_job` |
# MAGIC |---|---|---|
# MAGIC | `dev` tag | yes | no |
# MAGIC | Schedule | **Paused** | **Active** – daily 06:00 |
# MAGIC | Parameter `schema` | `cicd_<you>_dev` | `cicd_<you>_prod` |
# MAGIC | Task `bronze` → notebook path | your Git folder (`…/src/01_bronze`) | `…/.bundle/cicd_lab/prod/files/src/01_bronze` – a **copy** |
# MAGIC
# MAGIC ✅ Check the boxes yourself – click a task to see its notebook path.

# COMMAND ----------

import re
me = spark.sql("SELECT current_user()").first()[0]
short = re.sub(r"[^a-z0-9]", "_", me.split("@")[0].lower())
DEV = f"allianz_training.cicd_{short}_dev"
PROD = f"allianz_training.cicd_{short}_prod"

display(spark.sql(f"""
  SELECT 'dev' AS env, order_size, sum(orders) AS orders FROM {DEV}.gold_sales_by_country GROUP BY order_size
  UNION ALL
  SELECT 'prod', order_size, sum(orders) FROM {PROD}.gold_sales_by_country GROUP BY order_size
  ORDER BY order_size, env
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **You should now see:** identical numbers in dev and prod (93 `large`) – prod got exactly what you tested in dev.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. ✏️ Prove that prod is frozen (optional, 3 min)
# MAGIC
# MAGIC 1. In **your** `src/order_rules.py`, set `LARGE_ORDER_THRESHOLD = 2000` (no deploy!).
# MAGIC 2. Open the prod copy: Workspace → your home → `.bundle/cicd_lab/prod/files/src/order_rules.py`. 💬 Which value is in there? *(Still 1000 – prod only changes with a prod deploy.)*
# MAGIC 3. **Undo:** set the threshold back to `1000`.
# MAGIC
# MAGIC 💬 Why is this exactly what you want in production? *(Nobody's half-finished edit can reach prod. Only a deliberate, reviewed deploy can – in real life: only the pipeline.)*
# MAGIC
# MAGIC ➡️ Continue with `05_real_cicd_pipeline`.