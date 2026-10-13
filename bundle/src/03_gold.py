# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold – sales by country and order size

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
print(f"Reading from / writing to {CATALOG}.{SCHEMA}")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_sales_by_country
COMMENT 'Orders and revenue per country and order size (CI/CD lab)'
AS
SELECT country,
       order_size,
       count(*)              AS orders,
       round(sum(amount), 2) AS revenue,
       round(avg(amount), 2) AS avg_order_value
FROM silver_orders
GROUP BY country, order_size
""")

display(spark.table("gold_sales_by_country").orderBy("country", "order_size"))
