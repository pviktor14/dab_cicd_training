# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ✅ SOLUTION · 02 · Silver layer – clean, conformed, trustworthy
# MAGIC
# MAGIC **Goal (35 min):** turn raw Bronze into clean, typed, de-duplicated entity tables that the rest of the company can trust.
# MAGIC
# MAGIC | Silver principle | How we implement it |
# MAGIC |---|---|
# MAGIC | Explicit schema (a *contract*) | `CREATE TABLE` with real types & comments |
# MAGIC | Correct types | `try_cast`, `try_to_timestamp`, `from_json` |
# MAGIC | Standardised values | trim / lower-case, country → ISO code, status → canonical |
# MAGIC | One row per business key | de-duplicate with a window, keep the latest `updated_at` |
# MAGIC | Bad data is isolated, not lost | data-quality rules → **quarantine** table |
# MAGIC | Idempotent & incremental-friendly | `MERGE INTO` (upsert) – safe to re-run |
# MAGIC
# MAGIC ```
# MAGIC bronze_customers ──► silver_customers
# MAGIC bronze_products  ──► silver_products
# MAGIC bronze_orders    ──► silver_orders  +  silver_order_items   (explode nested items)
# MAGIC                  └─► silver_quarantine (rows that fail DQ rules)
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Define the Silver tables (the contract)
# MAGIC Unlike Bronze, Silver tables are created **up front with an explicit schema**. If an upstream change breaks the contract, the write fails loudly instead of silently corrupting data.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_customers (
# MAGIC   customer_id   STRING NOT NULL COMMENT 'Business key from CRM',
# MAGIC   first_name    STRING,
# MAGIC   last_name     STRING,
# MAGIC   email         STRING COMMENT 'Lower-cased, trimmed; NULL if missing',
# MAGIC   phone         STRING COMMENT 'Digits and leading + only',
# MAGIC   city          STRING,
# MAGIC   country_code  STRING COMMENT 'ISO-3166 alpha-2',
# MAGIC   signup_date   DATE,
# MAGIC   updated_at    TIMESTAMP,
# MAGIC   _source_file  STRING,
# MAGIC   _processed_at TIMESTAMP
# MAGIC ) COMMENT 'Clean customer master data (SCD type 1)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_products (
# MAGIC   product_id    STRING NOT NULL,
# MAGIC   product_name  STRING,
# MAGIC   category      STRING,
# MAGIC   brand         STRING,
# MAGIC   price         DECIMAL(10,2),
# MAGIC   currency      STRING,
# MAGIC   color         STRING,
# MAGIC   weight_kg     DECIMAL(6,2),
# MAGIC   size          STRING,
# MAGIC   is_active     BOOLEAN,
# MAGIC   updated_at    TIMESTAMP,
# MAGIC   _processed_at TIMESTAMP
# MAGIC ) COMMENT 'Clean product catalogue (SCD type 1)';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_orders (
# MAGIC   order_id       STRING NOT NULL,
# MAGIC   customer_id    STRING,
# MAGIC   order_ts       TIMESTAMP,
# MAGIC   order_date     DATE,
# MAGIC   channel        STRING,
# MAGIC   payment_method STRING,
# MAGIC   status         STRING COMMENT 'placed | shipped | delivered | cancelled',
# MAGIC   coupon_code    STRING,
# MAGIC   updated_at     TIMESTAMP,
# MAGIC   _processed_at  TIMESTAMP
# MAGIC ) COMMENT 'One row per order, latest status';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_order_items (
# MAGIC   order_id      STRING NOT NULL,
# MAGIC   line_number   INT    NOT NULL,
# MAGIC   product_id    STRING,
# MAGIC   quantity      INT,
# MAGIC   unit_price    DECIMAL(10,2),
# MAGIC   line_amount   DECIMAL(12,2),
# MAGIC   _processed_at TIMESTAMP
# MAGIC ) COMMENT 'Order lines exploded from the nested items array';
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_quarantine (
# MAGIC   entity        STRING,
# MAGIC   business_key  STRING,
# MAGIC   failed_rules  ARRAY<STRING>,
# MAGIC   record        STRING COMMENT 'The offending row as JSON',
# MAGIC   _processed_at TIMESTAMP
# MAGIC ) COMMENT 'Rows rejected by Silver data-quality rules';

# COMMAND ----------

# A CHECK constraint is a last line of defence enforced by Delta on every write
props = {r.key: r.value for r in spark.sql("SHOW TBLPROPERTIES silver_order_items").collect()}
if "delta.constraints.quantity_positive" not in props:
    spark.sql("ALTER TABLE silver_order_items ADD CONSTRAINT quantity_positive CHECK (quantity > 0)")
print("constraints:", {k: v for k, v in props.items() if k.startswith("delta.constraints")} or "quantity_positive added")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helpers: data-quality split & JSON parsing
# MAGIC `split_by_quality` evaluates a dictionary of **named rules** (SQL expressions that must be TRUE).
# MAGIC Good rows continue; bad rows go to `silver_quarantine` together with the list of rules they broke.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


def split_by_quality(df, rules: dict, entity: str, key_col: str):
    """Return (valid_df, n_invalid). Invalid rows are written to silver_quarantine (replacing this entity's rows)."""
    checks = [F.when(~F.coalesce(F.expr(expr), F.lit(False)), F.lit(name)) for name, expr in rules.items()]
    flagged = df.withColumn("_failed_rules", F.filter(F.array(*checks), lambda x: x.isNotNull()))

    invalid = flagged.filter(F.size("_failed_rules") > 0)
    data_cols = [c for c in df.columns]
    (
        invalid.select(
            F.lit(entity).alias("entity"),
            F.col(key_col).cast("string").alias("business_key"),
            F.col("_failed_rules").alias("failed_rules"),
            F.to_json(F.struct(*data_cols)).alias("record"),
            F.current_timestamp().alias("_processed_at"),
        )
        .write.mode("overwrite")
        .option("replaceWhere", f"entity = '{entity}'")      # idempotent: replace only this entity's rows
        .saveAsTable("silver_quarantine")
    )
    n_invalid = spark.table("silver_quarantine").filter(F.col("entity") == entity).count()
    valid = flagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules")
    print(f"[{entity}] quarantined {n_invalid:,} row(s)")
    return valid, n_invalid


def as_json_string(df, col_name):
    """Bronze may hold nested data as a JSON string or as a struct of strings – normalise to a JSON string."""
    dtype = dict(df.dtypes)[col_name]
    return F.col(col_name) if dtype == "string" else F.to_json(F.col(col_name))


def latest_per_key(df, key_cols, order_cols):
    """Keep one row per business key – the most recent one."""
    w = Window.partitionBy(*key_cols).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Customers
# MAGIC ### ✏️ Exercise 3 – standardise the country (7 min)
# MAGIC Bronze contains `HU`, `Hungary`, `hungary`, ` Magyarország ` … for the same country.
# MAGIC Complete `COUNTRY_MAP` (keys are **lower-case, trimmed** spellings, values are ISO codes) so that every customer gets a valid `country_code`.
# MAGIC Countries in the data: Hungary, Austria, Germany, Czechia, Slovakia, Poland, Romania.
# MAGIC
# MAGIC *Tip:* run the profiling query below first to see every spelling.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT lower(trim(country)) AS spelling, count(*) AS n FROM bronze_customers GROUP BY ALL ORDER BY n DESC

# COMMAND ----------

COUNTRY_MAP = {
    "hu": "HU", "hungary": "HU", "magyarország": "HU",
    "at": "AT", "austria": "AT", "österreich": "AT",
    "de": "DE", "germany": "DE", "deutschland": "DE",
    "cz": "CZ", "czechia": "CZ", "czech republic": "CZ",
    "sk": "SK", "slovakia": "SK", "slovensko": "SK",
    "pl": "PL", "poland": "PL", "polska": "PL",
    "ro": "RO", "romania": "RO", "românia": "RO",
}
country_map_col = F.create_map(*[F.lit(x) for kv in COUNTRY_MAP.items() for x in kv])

# COMMAND ----------

bronze_customers = spark.table("bronze_customers")

customers_clean = bronze_customers.select(
    F.trim("customer_id").alias("customer_id"),
    F.trim("first_name").alias("first_name"),
    F.trim("last_name").alias("last_name"),
    F.expr("nullif(lower(trim(email)), '')").alias("email"),
    F.regexp_replace("phone", r"[^0-9+]", "").alias("phone"),
    F.trim("city").alias("city"),
    country_map_col[F.lower(F.trim("country"))].alias("country_code"),
    F.to_date(F.coalesce(                                            # two date formats in the source
        F.expr("try_to_timestamp(signup_date, 'yyyy-MM-dd')"),
        F.expr("try_to_timestamp(signup_date, 'dd/MM/yyyy')"),
    )).alias("signup_date"),
    F.expr("try_to_timestamp(updated_at, 'yyyy-MM-dd HH:mm:ss')").alias("updated_at"),
    F.col("_source_file"),
    F.col("_ingested_at"),
)

customers_latest = latest_per_key(customers_clean, ["customer_id"], ["updated_at", "_ingested_at"]).drop("_ingested_at")

customer_rules = {
    "customer_id_not_null": "customer_id IS NOT NULL",
    "valid_country":        "country_code IS NOT NULL",
    "valid_signup_date":    "signup_date IS NOT NULL",
    "valid_email_format":   "email IS NULL OR email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[a-z]{2,}$'",   # missing email is allowed
}
customers_valid, _ = split_by_quality(customers_latest, customer_rules, "customers", "customer_id")
customers_valid.withColumn("_processed_at", F.current_timestamp()).createOrReplaceTempView("customers_updates")

print(f"bronze rows: {bronze_customers.count():,}  ->  unique valid customers: {spark.table('customers_updates').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### MERGE = upsert
# MAGIC * new `customer_id` → **INSERT**
# MAGIC * existing `customer_id` with a newer `updated_at` → **UPDATE** (SCD type 1: we keep only the current version)
# MAGIC * same or older version → nothing happens → **re-running is safe**

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_customers AS t
# MAGIC USING customers_updates AS s
# MAGIC ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country_code, count(*) AS customers
# MAGIC FROM silver_customers GROUP BY ALL ORDER BY customers DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Products
# MAGIC Problems to fix: price is a string with a **decimal comma** in some rows (`19,99`) and `N/A` in others; category casing is inconsistent; `attributes` is nested.

# COMMAND ----------

attributes_schema = StructType([
    StructField("color", StringType()),
    StructField("weight_kg", StringType()),
    StructField("size", StringType()),
])

bronze_products = spark.table("bronze_products")

products_clean = (
    bronze_products
    .withColumn("attr", F.from_json(as_json_string(bronze_products, "attributes"), attributes_schema))
    .select(
        F.trim("product_id").alias("product_id"),
        F.trim("product_name").alias("product_name"),
        F.initcap(F.trim("category")).alias("category"),                                 # ELECTRONICS -> Electronics
        F.col("brand"),
        F.expr("try_cast(replace(price, ',', '.') AS DECIMAL(10,2))").alias("price"),   # 19,99 -> 19.99 ; N/A -> NULL
        F.upper("currency").alias("currency"),
        F.col("attr.color").alias("color"),
        F.expr("try_cast(attr.weight_kg AS DECIMAL(6,2))").alias("weight_kg"),
        F.col("attr.size").alias("size"),
        F.expr("try_cast(is_active AS BOOLEAN)").alias("is_active"),
        F.expr("try_to_timestamp(updated_at, \"yyyy-MM-dd'T'HH:mm:ssX\")").alias("updated_at"),
        F.col("_ingested_at"),
    )
)
products_latest = latest_per_key(products_clean, ["product_id"], ["updated_at", "_ingested_at"]).drop("_ingested_at")

product_rules = {
    "product_id_not_null": "product_id IS NOT NULL",
    "valid_price":         "price IS NOT NULL AND price > 0",
}
products_valid, _ = split_by_quality(products_latest, product_rules, "products", "product_id")
products_valid.withColumn("_processed_at", F.current_timestamp()).createOrReplaceTempView("products_updates")
display(spark.table("products_updates").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercise 4 – write the products MERGE (5 min)
# MAGIC Upsert `products_updates` into `silver_products` on `product_id`, updating only when the incoming `updated_at` is newer. (Look at the customers MERGE above.)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_products AS t
# MAGIC USING products_updates AS s
# MAGIC ON t.product_id = s.product_id
# MAGIC WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Orders & order items
# MAGIC Problems to fix: exact duplicates, two timestamp formats, status casing, missing customer, nested `items` array, invalid quantities.
# MAGIC Later (batch 2) the same `order_id` will arrive again with a **new status** – MERGE handles that.

# COMMAND ----------

items_schema = ArrayType(StructType([
    StructField("line_number", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", StringType()),
    StructField("unit_price", StringType()),
]))

bronze_orders = spark.table("bronze_orders")
coupon_col = F.col("coupon_code") if "coupon_code" in bronze_orders.columns else F.lit(None).cast("string")  # appears in batch 2

orders_typed = bronze_orders.select(
    F.trim("order_id").alias("order_id"),
    F.trim("customer_id").alias("customer_id"),
    F.coalesce(
        F.expr("try_to_timestamp(order_ts, \"yyyy-MM-dd'T'HH:mm:ssX\")"),
        F.expr("try_to_timestamp(order_ts, 'yyyy-MM-dd HH:mm:ss')"),
    ).alias("order_ts"),
    F.lower(F.trim("channel")).alias("channel"),
    F.lower(F.trim("payment_method")).alias("payment_method"),
    F.lower(F.trim("status")).alias("status"),
    coupon_col.alias("coupon_code"),
    F.expr("try_to_timestamp(updated_at, \"yyyy-MM-dd'T'HH:mm:ssX\")").alias("updated_at"),
    F.from_json(as_json_string(bronze_orders, "items"), items_schema).alias("items"),
    F.col("_ingested_at"),
).withColumn("order_date", F.to_date("order_ts"))

orders_latest = latest_per_key(orders_typed, ["order_id"], ["updated_at", "_ingested_at"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✏️ Exercise 5 – add a data-quality rule (3 min)
# MAGIC Add a rule named `valid_channel` that only accepts `web`, `mobile` or `store`.

# COMMAND ----------

order_rules = {
    "order_id_not_null":    "order_id IS NOT NULL",
    "customer_id_not_null": "customer_id IS NOT NULL",
    "valid_order_ts":       "order_ts IS NOT NULL",
    "valid_status":         "status IN ('placed', 'shipped', 'delivered', 'cancelled')",
    "valid_channel":        "channel IN ('web', 'mobile', 'store')",
}
orders_valid, _ = split_by_quality(orders_latest.drop("items", "_ingested_at"), order_rules, "orders", "order_id")

order_cols = spark.table("silver_orders").columns
orders_valid.withColumn("_processed_at", F.current_timestamp()).select(*order_cols).createOrReplaceTempView("orders_updates")

# COMMAND ----------

# Explode the nested items: one row per order line (only for orders that passed the rules)
items = (
    orders_latest.join(orders_valid.select("order_id"), "order_id")
    .select("order_id", F.explode("items").alias("i"))
    .select(
        "order_id",
        F.expr("try_cast(i.line_number AS INT)").alias("line_number"),
        F.trim("i.product_id").alias("product_id"),
        F.expr("try_cast(i.quantity AS INT)").alias("quantity"),
        F.expr("try_cast(i.unit_price AS DECIMAL(10,2))").alias("unit_price"),
    )
    .withColumn("line_amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"))
)

item_rules = {
    "line_number_not_null": "line_number IS NOT NULL",
    "product_id_not_null":  "product_id IS NOT NULL",
    "quantity_positive":    "quantity > 0",
    "price_not_negative":   "unit_price >= 0",
}
items_valid, _ = split_by_quality(items, item_rules, "order_items", "order_id")
items_valid.withColumn("_processed_at", F.current_timestamp()).createOrReplaceTempView("order_items_updates")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_orders AS t
# MAGIC USING orders_updates AS s
# MAGIC ON t.order_id = s.order_id
# MAGIC WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_order_items AS t
# MAGIC USING order_items_updates AS s
# MAGIC ON t.order_id = s.order_id AND t.line_number = s.line_number
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validate: Bronze → Silver reconciliation
# MAGIC Every Bronze record should be accounted for: it is either in Silver, a duplicate we collapsed, or in quarantine.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'customers' AS entity, (SELECT count(*) FROM bronze_customers) AS bronze_rows,
# MAGIC        (SELECT count(*) FROM silver_customers) AS silver_rows,
# MAGIC        (SELECT count(*) FROM silver_quarantine WHERE entity = 'customers') AS quarantined
# MAGIC UNION ALL
# MAGIC SELECT 'products', (SELECT count(*) FROM bronze_products), (SELECT count(*) FROM silver_products),
# MAGIC        (SELECT count(*) FROM silver_quarantine WHERE entity = 'products')
# MAGIC UNION ALL
# MAGIC SELECT 'orders', (SELECT count(*) FROM bronze_orders), (SELECT count(*) FROM silver_orders),
# MAGIC        (SELECT count(*) FROM silver_quarantine WHERE entity = 'orders')
# MAGIC UNION ALL
# MAGIC SELECT 'order_items', NULL, (SELECT count(*) FROM silver_order_items),
# MAGIC        (SELECT count(*) FROM silver_quarantine WHERE entity = 'order_items')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Why were rows rejected?
# MAGIC SELECT entity, rule, count(*) AS rows
# MAGIC FROM silver_quarantine LATERAL VIEW explode(failed_rules) AS rule
# MAGIC GROUP BY ALL ORDER BY entity, rows DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_quarantine LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💬 Discussion
# MAGIC Some orders reference a `customer_id` that does **not** exist in `silver_customers` (e.g. `C9xxxx`). We did **not** reject them. Why might that be the right call? (*Hint: late-arriving dimensions.*) We'll handle it in Gold.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS orders_with_unknown_customer
# MAGIC FROM silver_orders o LEFT ANTI JOIN silver_customers c USING (customer_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC * Silver has an **explicit schema**, real types and one row per business key.
# MAGIC * Bad records are **quarantined with a reason**, never silently dropped.
# MAGIC * `MERGE` makes the layer **idempotent** and ready for updates (we'll prove it in notebook 04).