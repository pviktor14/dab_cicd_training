# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze – raw orders
# MAGIC
# MAGIC Self-contained on purpose: the "source system" is simulated here, so the job never depends on files or volumes.
# MAGIC Same seed every run → same 300 orders, including a few messy ones (lower-case / blank countries, missing or negative amounts).
# MAGIC
# MAGIC **Parameters** (set by the job, defaults for interactive runs): `catalog`, `schema`.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "allianz_training")
dbutils.widgets.text("schema", "")

_user = spark.sql("SELECT current_user()").first()[0]
_short = re.sub(r"[^a-z0-9]", "_", _user.split("@")[0].lower())

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema") or f"cicd_{_short}_dev"   # interactive default = your dev schema

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Writing to {CATALOG}.{SCHEMA}")

# COMMAND ----------

import random
from datetime import datetime, timedelta

rng = random.Random(42)   # fixed seed -> reproducible data
countries = ["DE", "AT", "HU", "SK", "CZ"] * 3 + [" de", "at ", "", None, "XX"]   # 25 % messy
channels = ["web", "app", "store"]
start = datetime(2026, 1, 1)

rows = []
for i in range(1, 301):
    amount = round(rng.uniform(5, 1500), 2)
    if i % 37 == 0:
        amount = None          # missing amount
    elif i % 53 == 0:
        amount = -amount       # refund booked as an order -> invalid
    rows.append((
        f"O{i:05d}",
        start + timedelta(hours=7 * i),
        rng.choice(countries),
        rng.choice(channels),
        amount,
    ))

schema = "order_id STRING, order_ts TIMESTAMP, country_raw STRING, channel STRING, amount DOUBLE"
bronze = spark.createDataFrame(rows, schema)

bronze.write.mode("overwrite").saveAsTable("bronze_orders")
print(f"bronze_orders: {spark.table('bronze_orders').count()} rows")
