# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # ✅ SOLUTION · 03 · Gold layer – business-ready data products
# MAGIC
# MAGIC **Goal (25 min):** model Silver into a small **star schema** plus aggregated tables that answer real business questions. Gold is mostly **SQL** – it's where analysts and BI tools live.
# MAGIC
# MAGIC | Gold principle | How we implement it |
# MAGIC |---|---|
# MAGIC | Modelled for consumption | dimensions + fact table (star schema) |
# MAGIC | Business logic lives here | revenue excludes cancelled orders, customer segments |
# MAGIC | Pre-aggregated where useful | daily sales, customer 360, product performance |
# MAGIC | Documented | table & column comments → discoverable in Catalog Explorer |
# MAGIC | Reproducible | `CREATE OR REPLACE TABLE ... AS SELECT` from Silver |
# MAGIC
# MAGIC ```
# MAGIC                 gold_dim_customer
# MAGIC                        │
# MAGIC gold_dim_product ── gold_fact_sales ──► gold_daily_sales
# MAGIC                                     ──► gold_customer_360
# MAGIC                                     ──► gold_product_performance
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimensions
# MAGIC We add an **"unknown member"** row to `gold_dim_customer`. Orders whose customer is missing or not (yet) in the CRM point to it, so joins never drop revenue.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_dim_customer
# MAGIC COMMENT 'Customer dimension incl. unknown member'
# MAGIC AS
# MAGIC SELECT customer_id                               AS customer_key,
# MAGIC        concat_ws(' ', first_name, last_name)      AS customer_name,
# MAGIC        email,
# MAGIC        city,
# MAGIC        country_code,
# MAGIC        signup_date
# MAGIC FROM silver_customers
# MAGIC UNION ALL
# MAGIC SELECT 'UNKNOWN', 'Unknown customer', NULL, NULL, 'N/A', NULL;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_dim_product
# MAGIC COMMENT 'Product dimension incl. unknown member'
# MAGIC AS
# MAGIC SELECT product_id AS product_key, product_name, category, brand, price AS list_price, color, size
# MAGIC FROM silver_products
# MAGIC UNION ALL
# MAGIC SELECT 'UNKNOWN', 'Unknown product', 'Unknown', NULL, NULL, NULL, NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fact table – grain: one row per order line

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_fact_sales
# MAGIC COMMENT 'Sales fact at order-line grain'
# MAGIC AS
# MAGIC SELECT i.order_id,
# MAGIC        i.line_number,
# MAGIC        o.order_date,
# MAGIC        o.order_ts,
# MAGIC        coalesce(c.customer_id, 'UNKNOWN') AS customer_key,
# MAGIC        coalesce(p.product_id,  'UNKNOWN') AS product_key,
# MAGIC        o.channel,
# MAGIC        o.payment_method,
# MAGIC        o.status,
# MAGIC        o.status = 'cancelled'             AS is_cancelled,
# MAGIC        o.coupon_code,
# MAGIC        i.quantity,
# MAGIC        i.unit_price,
# MAGIC        i.line_amount
# MAGIC FROM silver_order_items i
# MAGIC JOIN silver_orders o           ON o.order_id = i.order_id
# MAGIC LEFT JOIN silver_customers c   ON c.customer_id = o.customer_id
# MAGIC LEFT JOIN silver_products  p   ON p.product_id = i.product_id;
# MAGIC
# MAGIC -- Column comments end up in Catalog Explorer and help BI users / AI assistants
# MAGIC ALTER TABLE gold_fact_sales ALTER COLUMN customer_key COMMENT 'FK to gold_dim_customer (UNKNOWN if missing)';
# MAGIC ALTER TABLE gold_fact_sales ALTER COLUMN product_key  COMMENT 'FK to gold_dim_product (UNKNOWN if missing)';
# MAGIC ALTER TABLE gold_fact_sales ALTER COLUMN line_amount  COMMENT 'quantity * unit_price in EUR';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Referential integrity check: how much revenue sits on unknown members?
# MAGIC SELECT customer_key = 'UNKNOWN' AS unknown_customer,
# MAGIC        product_key  = 'UNKNOWN' AS unknown_product,
# MAGIC        count(*) AS lines, sum(line_amount) AS revenue
# MAGIC FROM gold_fact_sales GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Aggregates
# MAGIC ### Daily sales (business rule: cancelled orders are **not** revenue)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_sales
# MAGIC COMMENT 'Daily KPIs – cancelled orders excluded from revenue'
# MAGIC AS
# MAGIC SELECT order_date,
# MAGIC        count(DISTINCT order_id) FILTER (WHERE NOT is_cancelled)             AS orders,
# MAGIC        count(DISTINCT order_id) FILTER (WHERE is_cancelled)                 AS cancelled_orders,
# MAGIC        sum(quantity)            FILTER (WHERE NOT is_cancelled)             AS units_sold,
# MAGIC        sum(line_amount)         FILTER (WHERE NOT is_cancelled)             AS revenue,
# MAGIC        round(sum(line_amount) FILTER (WHERE NOT is_cancelled)
# MAGIC              / count(DISTINCT order_id) FILTER (WHERE NOT is_cancelled), 2) AS avg_order_value,
# MAGIC        count(DISTINCT customer_key)                                         AS active_customers
# MAGIC FROM gold_fact_sales
# MAGIC GROUP BY order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 📈 Click "+" → Visualization → Line chart: X = order_date, Y = revenue
# MAGIC SELECT * FROM gold_daily_sales ORDER BY order_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customer 360 & segmentation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_customer_360
# MAGIC COMMENT 'One row per customer with lifetime KPIs and segment'
# MAGIC AS
# MAGIC WITH sales AS (
# MAGIC   SELECT customer_key,
# MAGIC          min(order_date)          AS first_order_date,
# MAGIC          max(order_date)          AS last_order_date,
# MAGIC          count(DISTINCT order_id) AS orders,
# MAGIC          sum(line_amount)         AS lifetime_revenue
# MAGIC   FROM gold_fact_sales
# MAGIC   WHERE NOT is_cancelled AND customer_key <> 'UNKNOWN'
# MAGIC   GROUP BY customer_key
# MAGIC )
# MAGIC SELECT c.customer_key,
# MAGIC        c.customer_name,
# MAGIC        c.country_code,
# MAGIC        c.signup_date,
# MAGIC        s.first_order_date,
# MAGIC        s.last_order_date,
# MAGIC        coalesce(s.orders, 0)                                AS orders,
# MAGIC        coalesce(s.lifetime_revenue, 0)                      AS lifetime_revenue,
# MAGIC        round(s.lifetime_revenue / s.orders, 2)              AS avg_order_value,
# MAGIC        datediff((SELECT max(order_date) FROM gold_fact_sales), s.last_order_date) AS days_since_last_order,
# MAGIC        CASE WHEN s.orders IS NULL              THEN 'No purchase'
# MAGIC             WHEN s.lifetime_revenue >= 1500    THEN 'VIP'
# MAGIC             WHEN s.lifetime_revenue >= 600     THEN 'Regular'
# MAGIC             ELSE 'Occasional' END                           AS segment
# MAGIC FROM gold_dim_customer c
# MAGIC LEFT JOIN sales s USING (customer_key)
# MAGIC WHERE c.customer_key <> 'UNKNOWN'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT segment, count(*) AS customers, round(sum(lifetime_revenue)) AS revenue,
# MAGIC        round(100 * sum(lifetime_revenue) / sum(sum(lifetime_revenue)) OVER (), 1) AS pct_of_revenue
# MAGIC FROM gold_customer_360 GROUP BY segment ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Product performance

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_product_performance
# MAGIC COMMENT 'Units and revenue per product with rank inside its category'
# MAGIC AS
# MAGIC SELECT p.category,
# MAGIC        p.product_key,
# MAGIC        p.product_name,
# MAGIC        coalesce(sum(f.quantity), 0)        AS units_sold,
# MAGIC        coalesce(sum(f.line_amount), 0)     AS revenue,
# MAGIC        dense_rank() OVER (PARTITION BY p.category ORDER BY coalesce(sum(f.line_amount), 0) DESC) AS rank_in_category
# MAGIC FROM gold_dim_product p
# MAGIC LEFT JOIN gold_fact_sales f ON f.product_key = p.product_key AND NOT f.is_cancelled
# MAGIC GROUP BY p.category, p.product_key, p.product_name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category, round(sum(revenue)) AS revenue, sum(units_sold) AS units
# MAGIC FROM gold_product_performance GROUP BY category ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✏️ Exercise 6 – sales by country and channel (7 min)
# MAGIC Create **`gold_sales_by_country_channel`** with one row per `country_code` × `channel` and the columns `orders`, `revenue`, `avg_order_value`.
# MAGIC Exclude cancelled orders. *Hint:* join `gold_fact_sales` to `gold_dim_customer`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_sales_by_country_channel
# MAGIC COMMENT 'Revenue by customer country and sales channel'
# MAGIC AS
# MAGIC SELECT c.country_code,
# MAGIC        f.channel,
# MAGIC        count(DISTINCT f.order_id)                                AS orders,
# MAGIC        sum(f.line_amount)                                        AS revenue,
# MAGIC        round(sum(f.line_amount) / count(DISTINCT f.order_id), 2) AS avg_order_value
# MAGIC FROM gold_fact_sales f
# MAGIC JOIN gold_dim_customer c ON c.customer_key = f.customer_key
# MAGIC WHERE NOT f.is_cancelled
# MAGIC GROUP BY ALL;
# MAGIC
# MAGIC SELECT * FROM gold_sales_by_country_channel ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✏️ Exercise 7 – top 3 products per category (5 min)
# MAGIC Return the 3 best-selling products (by revenue) in every category from `gold_product_performance`.
# MAGIC *Hint:* Databricks SQL supports `QUALIFY` to filter on a window function.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT category, product_name, revenue, ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) as rank_in_category
# MAGIC FROM gold_product_performance
# MAGIC WHERE rank_in_category <= 3
# MAGIC ORDER BY category, rank_in_category

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⭐ Stretch exercise 8 – 7-day moving average
# MAGIC Add a 7-day moving average of `revenue` to the daily sales (as a query or a view `gold_daily_sales_ma7`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold_daily_sales_ma7 AS
# MAGIC SELECT order_date, revenue,
# MAGIC        round(avg(revenue) OVER (ORDER BY order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_ma7
# MAGIC FROM gold_daily_sales;
# MAGIC
# MAGIC SELECT * FROM gold_daily_sales_ma7 ORDER BY order_date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make it discoverable
# MAGIC Open **Catalog Explorer** → your schema. Notice the table comments, the **Lineage** tab (Bronze → Silver → Gold is tracked automatically by Unity Catalog) and the sample data preview.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap
# MAGIC * Gold is **modelled for the consumer** (star schema + KPIs), not for the source system.
# MAGIC * Business rules (what counts as revenue, what is a VIP) are defined **once**, here.
# MAGIC * Unknown members keep facts and dimensions consistent even when upstream data is late.