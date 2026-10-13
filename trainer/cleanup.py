# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 Trainer clean-up – CI/CD lab (simple)
# MAGIC
# MAGIC Run as a **workspace admin** after the course. First with `mode = list` (dry run), then `mode = delete`.
# MAGIC
# MAGIC | Removes | How it is found |
# MAGIC |---|---|
# MAGIC | jobs `[dev …]` / `[prod …] cicd_lab_job` | job tag `course = cicd_lab_simple` |
# MAGIC | schemas `allianz_training.cicd_<user>_dev` / `_prod` (CASCADE) | name pattern |
# MAGIC | deployment folders `/Users/<user>/.bundle/cicd_lab` | path pattern |
# MAGIC
# MAGIC Source files (Git folders, `participants/…`) are **not** touched.

# COMMAND ----------

dbutils.widgets.dropdown("mode", "list", ["list", "delete"])
dbutils.widgets.text("catalog", "allianz_training")
DELETE = dbutils.widgets.get("mode") == "delete"
CATALOG = dbutils.widgets.get("catalog")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print("DELETE mode" if DELETE else "LIST mode (dry run)")

# COMMAND ----------

# 1. Jobs
for j in w.jobs.list():
    tags = (j.settings.tags or {}) if j.settings else {}
    if tags.get("course") == "cicd_lab_simple":
        print(f"job {j.job_id}: {j.settings.name}  (creator: {j.creator_user_name})")
        if DELETE:
            w.jobs.delete(job_id=j.job_id)

# COMMAND ----------

# 2. Schemas
import re
pattern = re.compile(r"^cicd_[a-z0-9_]+_(dev|prod)$")
for s in w.schemas.list(catalog_name=CATALOG):
    if pattern.match(s.name):
        print(f"schema {s.full_name}  (owner: {s.owner})")
        if DELETE:
            spark.sql(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{s.name}` CASCADE")

# COMMAND ----------

# 3. Bundle deployment folders (state + prod copies)
for u in w.workspace.list("/Users"):
    path = f"{u.path}/.bundle/cicd_lab"
    try:
        w.workspace.get_status(path)
    except Exception:
        continue
    print(f"folder {path}")
    if DELETE:
        w.workspace.delete(path, recursive=True)

print("done")
