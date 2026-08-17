# Databricks notebook source

# CUSTOMERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ CUSTOMERS FROM BRONZE STREAMING TABLE
# ============================================================
df = spark.readStream.table(
    "structured_streaming.bronze.customers"
)

# ============================================================
# 3. REMOVE DUPLICATES, FILTER NULLS AND ADD TIMESTAMP
# ============================================================
df = (
    df
    .dropDuplicates(["customer_id"])
    .filter(col("customer_id").isNotNull())
    .withColumn("updated_timestamp", current_timestamp())
)

# ============================================================
# 4. WRITE TO SILVER STREAMING TABLE
# ============================================================
df.writeStream \
    .format("delta") \
    .outputMode("Append") \
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/silver/customers/"
    ) \
    .trigger(availableNow=True) \
    .toTable("structured_streaming.silver.customers")

# COMMAND ----------

# EMPLOYEES
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ EMPLOYEES FROM BRONZE STREAMING TABLE
# ============================================================
df = spark.readStream.table(
    "structured_streaming.bronze.employees"
)

# ============================================================
# 3. REMOVE DUPLICATES, FILTER NULLS AND ADD TIMESTAMP
# ============================================================
df = (
    df
    .dropDuplicates(["employee_id"])
    .filter(col("employee_id").isNotNull())
    .withColumn("updated_timestamp", current_timestamp())
)

# ============================================================
# 4. WRITE TO SILVER STREAMING TABLE
# ============================================================
df.writeStream \
    .format("delta") \
    .outputMode("Append") \
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/silver/employees/"
    ) \
    .trigger(availableNow=True) \
    .toTable("structured_streaming.silver.employees")

# COMMAND ----------

# JOINING CATEGORIES, PRODUCTS & SUPPLIERS IN ONE TABLE(products)
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ AND TRANSFORM CATEGORIES
# ============================================================
category = spark.readStream.table(
    "structured_streaming.bronze.categories"
)

category = (
    category
    .dropDuplicates(["category_id"])
    .filter(col("category_id").isNotNull())
)

# ============================================================
# 3. READ AND TRANSFORM PRODUCTS
# ============================================================
product = spark.readStream.table(
    "structured_streaming.bronze.products"
)

product = (
        product
        .withColumn("unit_price", col("unit_price").cast("double"))
        .withColumn("units_in_stock", col("units_in_stock").cast("integer"))
        .withColumn("units_on_order", col("units_on_order").cast("integer"))
        .withColumn("reorder_level", col("reorder_level").cast("integer"))
        .withColumn("discontinued", col("discontinued").cast("boolean"))
        .dropDuplicates(["product_id"])
        .filter(col("product_id").isNotNull())
)

# ============================================================
# 4. READ AND TRANSFORM SUPPLIERS
# ============================================================
supplier = spark.readStream.table(
    "structured_streaming.bronze.suppliers"
)

supplier = (
    supplier
    .dropDuplicates(["supplier_id"])
    .filter(col("supplier_id").isNotNull())
)

# ============================================================
# 5. JOIN PRODUCTS, CATEGORIES AND SUPPLIERS
# ============================================================
df = (
    product.alias("p")
    .join(
        category.alias("c"),
        col("p.category_id") == col("c.category_id")
    )
    .join(
        supplier.alias("s"),
        col("p.supplier_id") == col("s.supplier_id")
    )
    .select(
        col("p.product_id"),
        col("p.product_name"),
        col("c.category_name"),
        col("p.quantity_per_unit"),
        col("p.unit_price"),
        col("p.units_in_stock"),
        col("p.units_on_order"),
        col("p.reorder_level"),
        col("p.discontinued"),
        col("s.company_name").alias("supplier_name"),
        col("s.contact_name").alias("supplier_contact"),
        col("s.contact_title").alias("supplier_contact_title"),
        col("s.address").alias("supplier_address"),
        col("s.city").alias("supplier_city"),
        col("s.region").alias("supplier_region"),
        col("s.postal_code").alias("supplier_postal_code"),
        col("s.country").alias("supplier_country"),
        col("s.phone").alias("supplier_phone"),
        col("s.fax").alias("supplier_fax")
    )
)

# ============================================================
# 6. ADD UPDATED TIMESTAMP
# ============================================================
df = df.withColumn(
    "updated_time",
    current_timestamp()
)

# ============================================================
# 7. WRITE TO SILVER STREAMING TABLE
# ============================================================
df.writeStream \
    .format("delta") \
    .outputMode("Append") \
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/silver/products/"
    ) \
    .trigger(availableNow=True) \
    .toTable(
        "structured_streaming.silver.products"
    )

# COMMAND ----------

# JOINING ORDERS & ORDER DETAILS INTO ONE TABLE(orders)
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# ============================================================
# 2. READ ORDERS STREAM
# ============================================================
orders = (
    spark.readStream
    .table("structured_streaming.bronze.orders")
)

# ============================================================
# 3. READ ORDER DETAILS STREAM
# ============================================================
order_details = (
    spark.readStream
    .table("structured_streaming.bronze.order_details")
)

# ============================================================
# 4. PROCESS EACH MICRO-BATCH
# ============================================================
def process_orders(batch_df, batch_id):
    
  # Skip empty micro-batches
    if batch_df.isEmpty():
        return

    # ========================================================
    # 5. FIND AFFECTED ORDERS
    # ========================================================
    affected_orders = (
        batch_df
        .select("order_id")
        .filter(col("order_id").isNotNull())
        .distinct()
    )

    # ========================================================
    # 6. READ ALL ORDER DETAILS
    #    FOR AFFECTED ORDERS
    # ========================================================
    details = (
        spark.table(
            "structured_streaming.bronze.order_details"
        )
        .join(
            affected_orders,
            "order_id",
            "inner"
        )
    )

    # ========================================================
    # 7. CLEAN / CAST ORDER DETAIL COLUMNS
    # ========================================================
    details = (
        details
        .withColumn(
            "unit_price",
            col("unit_price").cast("double")
        )
        .withColumn(
            "quantity",
            col("quantity").cast("integer")
        )
        .withColumn(
            "discount",
            col("discount").cast("double")
        )
    )

    # ========================================================
    # 8. CALCULATE LINE AMOUNT
    # ========================================================
    # Grain: order_id + product_id
    details = details.withColumn(
        "line_amount",
        col("unit_price")
        * col("quantity")
        * (1 - col("discount"))
    )

    # ========================================================
    # 9. CALCULATE TOTAL ORDER AMOUNT
    # ========================================================
        # Grain: order_id
    order_window = Window.partitionBy("order_id")

    details = details.withColumn(
        "total_order_amount",
        sum("line_amount").over(order_window)
    )

    # ========================================================
    # 10. READ ORDERS
    # ========================================================
    orders_df = (
        spark.table(
            "structured_streaming.bronze.orders"
        )
        .select(
            col("order_id").alias("order_id_o"),
            col("customer_id"),
            col("freight").cast("double").alias("freight"),
            col("employee_id"),
            col("ship_via").alias("shipper_id"),
            col("order_date"),
            col("shipped_date")
        )
    )

    # ========================================================
    # 11. JOIN ORDER DETAILS WITH ORDERS
    # ========================================================
    details = (
        details.alias("od")
        .join(
            orders_df.alias("o"),
            col("od.order_id") == col("o.order_id_o"),
            "left"
        )
    )

    # ========================================================
    # 12. ALLOCATE FREIGHT
    # ========================================================
    details = details.withColumn(
        "allocate_freight",
        when(
            col("total_order_amount") != 0,
            round(
                col("o.freight")
                * col("od.line_amount")
                / col("od.total_order_amount"),
                2
            )
        ).otherwise(lit(0.0))
    )

    # ========================================================
    # 13. SELECT FINAL SILVER COLUMNS
    # ========================================================
    final_df = (
        details
        .select(
            col("od.order_id").alias("order_id"),
            col("o.customer_id"),
            col("od.product_id"),
            col("od.unit_price"),
            col("od.quantity"),
            col("od.discount"),
            col("od.line_amount"),
            col("od.total_order_amount"),
            col("allocate_freight"),
            col("o.employee_id"),
            col("o.shipper_id"),
            col("o.order_date"),
            col("o.shipped_date")
        )
    )

    # ========================================================
    # 14. WRITE TO SILVER
    # ========================================================
    (
        final_df
        .write
        .mode("append")
        .format("delta")
        .saveAsTable(
            "structured_streaming.silver.orders"
        )
    )

# ============================================================
# 15. START STREAM
# ============================================================
query = (
    order_details
    .writeStream
    .foreachBatch(process_orders)
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/silver/orders/"
    )
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# JOINING SHIPPERS & SHIPMENTS INTO ONE TABLE(shipments)
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ AND TRANSFORM SHIPPERS
# ============================================================
shipper = spark.readStream.table(
    "structured_streaming.bronze.shippers"
)

shipper = (
    shipper
    .dropDuplicates(["shipper_id"])
    .filter(col("shipper_id").isNotNull())
)

# ============================================================
# 3. READ AND TRANSFORM SHIPMENTS
# ============================================================
shipment = spark.readStream.table(
    "structured_streaming.bronze.shipments"
)

shipment = (
    shipment
    .dropDuplicates(["order_id"])
    .filter(col("order_id").isNotNull())
)

# ============================================================
# 4. JOIN SHIPPERS AND SHIPMENTS
# ============================================================
df = (
    shipment.alias("s")
    .join(
        shipper.alias("sh"),
        col("s.ship_via") == col("sh.shipper_id")
    )
    .select(
        col("s.order_id"),
        col("s.shipped_date"),
        col("s.ship_name"),
        col("sh.company_name").alias("shipper_name"),
        col("s.ship_address"),
        col("s.ship_city"),
        col("s.ship_region"),
        col("s.ship_postal_code"),
        col("s.ship_country"),
        col("sh.phone")))

# ============================================================
# 5. ADD UPDATED TIMESTAMP
# ============================================================
df = df.withColumn(
    "updated_time",
    current_timestamp()
)

# ============================================================
# 6. WRITE TO SILVER STREAMING TABLE
# ============================================================
df.writeStream \
    .format("delta") \
    .outputMode("Append") \
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/silver/shipments/"
    ) \
    .trigger(availableNow=True) \
    .toTable(
        "structured_streaming.silver.shipments"
    )
