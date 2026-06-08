from pyspark.sql.functions import (
    col, sum, count, avg, round
)

# ------------------------------------
# Read Silver Layer
# ------------------------------------
silver_df = spark.read.parquet(
    "/opt/spark-data/silver/retail_sales_clean.parquet"
)

# ------------------------------------
# Derived Column: total_amount
# ------------------------------------
silver_df = silver_df.withColumn(
    "total_amount",
    round(col("quantity") * col("unit_price") * (1 - col("discount_pct") / 100), 2)
)

# ------------------------------------
# 1️⃣ Daily Sales Metrics
# ------------------------------------
daily_sales_df = (
    silver_df
    .groupBy("order_date")
    .agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("transaction_id").alias("total_orders"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    )
)

(
    daily_sales_df
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/gold/daily_sales_metrics.parquet")
)

# ------------------------------------
# 2️⃣ Product Category Performance
# ------------------------------------
product_perf_df = (
    silver_df
    .groupBy("product_category")
    .agg(
        round(sum("total_amount"), 2).alias("category_revenue"),
        sum("quantity").alias("total_units_sold"),
        count("transaction_id").alias("order_count")
    )
)

(
    product_perf_df
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/gold/product_category_performance.parquet")
)

# ------------------------------------
# 3️⃣ City-Level Revenue Metrics
# ------------------------------------
city_revenue_df = (
    silver_df
    .groupBy("city", "state")
    .agg(
        round(sum("total_amount"), 2).alias("city_revenue"),
        count("transaction_id").alias("order_count"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    )
)

(
    city_revenue_df
    .write
    .mode("overwrite")
    .parquet("/opt/spark-data/gold/city_revenue_metrics.parquet")
)

print("✅ Gold layer created successfully")
