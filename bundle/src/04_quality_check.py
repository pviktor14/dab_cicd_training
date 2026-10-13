# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Quality check – fail the job if the data looks wrong
# MAGIC
# MAGIC Not part of the job yet – you wire it in during lab step 3.
# MAGIC A failing assertion = a failed task = a red job run (and in real life: an alert instead of a wrong dashboard).

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "allianz_training")
dbutils.widgets.text("schema", "")

_user = spark.sql("SELECT current_user()").first()[0]
_short = re.sub(r"[^a-z0-9]", "_", _user.split("@")[0].lower())

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema") or f"cicd_{_short}_dev"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Checking {CATALOG}.{SCHEMA}")

# COMMAND ----------

checks = {
    "gold table is not empty":
        spark.sql("SELECT count(*) FROM gold_sales_by_country").first()[0] > 0,
    "silver has no invalid amounts":
        spark.sql("SELECT count(*) FROM silver_orders WHERE amount IS NULL OR amount <= 0").first()[0] == 0,
    "silver has only known order sizes":
        spark.sql("SELECT count(*) FROM silver_orders WHERE order_size NOT IN ('small', 'medium', 'large')").first()[0] == 0,
    "gold revenue = silver revenue (±1 for rounding)":
        abs(spark.sql("SELECT sum(revenue) FROM gold_sales_by_country").first()[0]
            - spark.sql("SELECT sum(amount) FROM silver_orders").first()[0]) < 1,
}

for name, ok in checks.items():
    print(("✅ " if ok else "❌ ") + name)

failed = [name for name, ok in checks.items() if not ok]
assert not failed, f"Quality check failed: {failed}"
