# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ✅ SOLUTION · 01 · Bronze layer – raw, append-only, replayable
# MAGIC
# MAGIC **Goal (25 min):** ingest every file from the landing zone into Delta tables **as-is**, incrementally, with lineage metadata.
# MAGIC
# MAGIC | Bronze principle | How we implement it |
# MAGIC |---|---|
# MAGIC | Keep the data *as delivered* | All columns stay **STRING** – no casting, no cleaning |
# MAGIC | Never lose a record | Append-only + `_rescued_data` column for anything that doesn't fit |
# MAGIC | Know where each row came from | `_source_file`, `_file_modified_at`, `_ingested_at` |
# MAGIC | Only process new files | **Auto Loader** (`cloudFiles`) + checkpoints |
# MAGIC | Survive schema changes | `cloudFiles.schemaLocation` + schema evolution |
# MAGIC
# MAGIC ```
# MAGIC landing/customers/*.csv ──► bronze_customers
# MAGIC landing/products/*.json ──► bronze_products
# MAGIC landing/orders/*.json   ──► bronze_orders
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. A reusable Auto Loader ingestion function
# MAGIC
# MAGIC Key ideas:
# MAGIC * `format("cloudFiles")` = Auto Loader. It remembers which files it already processed (in the **checkpoint**).
# MAGIC * `cloudFiles.schemaLocation` – where Auto Loader stores the inferred schema and tracks its evolution.
# MAGIC * `trigger(availableNow=True)` – process everything that's new, then **stop**. Streaming semantics, batch cost. Perfect for scheduled jobs.
# MAGIC * `_metadata` – a hidden column with file path, size and modification time.
# MAGIC * If a **new column** shows up, Auto Loader updates the schema and stops the stream with `UnknownFieldException`. Re-running picks up the new schema – our helper does that retry automatically.
# MAGIC   Spark still writes the exception to the cell's log output (`... UnknownFieldException ... can be fixed by an automatic retry: true`). **That line is expected** – look for the `↻ attempt 1` message and the row count printed afterwards.

# COMMAND ----------

from pyspark.sql import functions as F


def ingest_to_bronze(source_folder: str, file_format: str, table: str, reader_options: dict = None) -> int:
    """Incrementally load new files from LANDING/<source_folder> into a bronze Delta table."""
    reader_options = reader_options or {}
    checkpoint = f"{CHECKPOINTS}/{table}"

    def _run():
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", file_format)
            .option("cloudFiles.schemaLocation", f"{checkpoint}/_schema")
            .option("cloudFiles.inferColumnTypes", "false")      # bronze = everything as STRING
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")   # default, made explicit: stop, add column, restart
            .options(**reader_options)
            .load(f"{LANDING}/{source_folder}")
            .select(
                "*",
                F.col("_metadata.file_path").alias("_source_file"),
                F.col("_metadata.file_modification_time").alias("_file_modified_at"),
                F.current_timestamp().alias("_ingested_at"),
            )
        )
        (
            stream.writeStream
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")                        # let the Delta table evolve too
            .trigger(availableNow=True)
            .toTable(table)
            .awaitTermination()
        )

    SCHEMA_CHANGE_MARKERS = ("UNKNOWN_FIELD_EXCEPTION", "UnknownFieldException", "NEW_FIELDS_IN_RECORD")
    before = spark.table(table).count() if spark.catalog.tableExists(table) else 0
    for attempt in range(1, 6):
        try:
            _run()
            break
        except Exception as e:                                    # schema evolved -> just run again
            if any(m in str(e) for m in SCHEMA_CHANGE_MARKERS):
                print(f"  ↻ attempt {attempt}: new column(s) found in {source_folder} – Auto Loader saved the new schema, "
                      f"restarting the stream (the UnknownFieldException in the log is expected)")
                continue
            raise
    else:
        raise RuntimeError(f"{table}: schema kept changing after 5 attempts – check {checkpoint}/_schema")
    after = spark.table(table).count()
    print(f"{table:20s}: +{after - before:,} rows  (total {after:,})")
    return after - before

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Ingest customers (CSV)

# COMMAND ----------

ingest_to_bronze("customers", "csv", "bronze_customers", {"header": "true"})

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Everything is a STRING, plus our lineage columns and _rescued_data
# MAGIC DESCRIBE TABLE bronze_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_customers LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest products (JSON)

# COMMAND ----------

ingest_to_bronze("products", "json", "bronze_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- No type guessing at all: price is a string ("19,99", "N/A" ...), and even the values
# MAGIC -- inside the nested attributes are strings. We will apply a proper schema in Silver.
# MAGIC SELECT product_id, product_name, category, price, attributes, updated_at
# MAGIC FROM bronze_products
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✏️ Exercise 1 – ingest the orders (5 min)
# MAGIC Use `ingest_to_bronze` to load `landing/orders` (JSON) into a table called **`bronze_orders`**.

# COMMAND ----------

ingest_to_bronze("orders", "json", "bronze_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Explore Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- items holds the order lines (nested JSON) – one order, many lines. We will explode it in Silver.
# MAGIC SELECT order_id, customer_id, order_ts, status, items
# MAGIC FROM bronze_orders
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lineage: rows per source file
# MAGIC SELECT regexp_extract(_source_file, '[^/]+$', 0) AS file, count(*) AS rows, max(_ingested_at) AS ingested_at
# MAGIC FROM bronze_orders
# MAGIC GROUP BY ALL
# MAGIC ORDER BY file
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Idempotency test
# MAGIC Run the ingestion again. Auto Loader checks its checkpoint, finds **no new files** and adds **0 rows**.
# MAGIC (If you see `+5,283 rows` here, Exercise 1 wasn't done – this cell just did the initial load for you.)

# COMMAND ----------

ingest_to_bronze("orders", "json", "bronze_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✏️ Exercise 2 – profile Bronze (5 min)
# MAGIC Write a query on `bronze_orders` that returns:
# MAGIC * total rows vs. distinct `order_id` (→ duplicates!)
# MAGIC * rows where `customer_id` is null
# MAGIC * the distinct values of `status` (→ casing issues!)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)                        AS total_rows,
# MAGIC        count(DISTINCT order_id)        AS distinct_orders,
# MAGIC        count(*) - count(DISTINCT order_id) AS duplicate_rows,
# MAGIC        count_if(customer_id IS NULL)   AS null_customer,
# MAGIC        array_sort(collect_set(status)) AS status_values
# MAGIC FROM bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta gives us a transaction log for free

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC * Bronze = **raw + metadata**, append-only, all strings.
# MAGIC * Auto Loader makes ingestion **incremental and idempotent** – safe to re-run.
# MAGIC * We did **not** fix anything yet – duplicates, bad dates and messy statuses are still there. That's Silver's job.