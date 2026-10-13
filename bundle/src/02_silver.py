# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver – clean orders
# MAGIC
# MAGIC The business rules are **not** written in this notebook – they live in the plain Python module `order_rules.py` next to it.
# MAGIC That module is unit-tested (`tests/test_order_rules.py`) – in CI, before anything is deployed.
# MAGIC
# MAGIC | Rule | Function |
# MAGIC |---|---|
# MAGIC | drop orders without a positive amount | `is_valid_amount` |
# MAGIC | `' de '` → `DE`, unknown → `UNKNOWN` | `normalize_country` |
# MAGIC | small / medium / large | `order_size` |

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

# Import the rules from order_rules.py (same folder as this notebook).
# The notebook's folder is normally on sys.path already – the lines below just make sure.
import os, sys

_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_dir = os.path.dirname(_nb_path)
for _p in (os.getcwd(), _nb_dir, "/Workspace" + _nb_dir):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import importlib
import order_rules
importlib.reload(order_rules)          # pick up edits when you re-run interactively
from order_rules import is_valid_amount, normalize_country, order_size

print("order_rules loaded from", order_rules.__file__)
print("LARGE_ORDER_THRESHOLD =", order_rules.LARGE_ORDER_THRESHOLD)

# COMMAND ----------

# Tiny lab data (300 rows) -> apply the Python rules on the driver.
# Real life with big data: Spark expressions, or ship the module as a wheel and use it in a UDF.
silver_rows = [
    (r.order_id, r.order_ts, normalize_country(r.country_raw), r.channel, r.amount, order_size(r.amount))
    for r in spark.table("bronze_orders").collect()
    if is_valid_amount(r.amount)
]

schema = "order_id STRING, order_ts TIMESTAMP, country STRING, channel STRING, amount DOUBLE, order_size STRING"
silver = spark.createDataFrame(silver_rows, schema)

silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_orders")
print(f"silver_orders: {spark.table('silver_orders').count()} rows (bronze: {spark.table('bronze_orders').count()})")
