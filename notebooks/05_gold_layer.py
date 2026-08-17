# Databricks notebook source

# CUSTOMERS WITH SCD2
# =================================================
# IMPORTS
# =================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable
# ============================================================
# 1. READ SILVER CUSTOMER TABLE
# ============================================================
silver_customer = (
    spark.readStream
    .table("structured_streaming.silver.customers")
)

# ============================================================
# 2. SCD TYPE 2 FUNCTION
# ============================================================
def scd2(batch_df, batch_id):

    # Skip empty micro-batches
    if batch_df.isEmpty():
        return

    # ========================================================
    # TARGET
    # ========================================================
    target_table = "structured_streaming.gold.dim_customers"

    target = DeltaTable.forName(
        spark,
        target_table
    )

    # ========================================================
    # PREPARE SOURCE DATA
    # ========================================================

    source_df = (
        batch_df
        .select(
            "customer_id",
            "company_name",
            "contact_name",
            "contact_title",
            "cust_address",
            "city",
            "region",
            "postal_code",
            "country",
            "phone",
            "fax",
            "ingestiontime",
            "updated_timestamp"
        )
        .withColumnRenamed(
            "ingestiontime",
            "ingestion_time"
        )
        .withColumnRenamed(
            "updated_timestamp",
            "updated_time"
        )
    )

    # ========================================================
    # CURRENT GOLD RECORDS
    # ========================================================
    current_df = (
        spark.table(target_table)
        .filter(col("is_current") == True)
        .select(
            "customer_id",
            "company_name",
            "contact_name",
            "contact_title",
            "cust_address",
            "city",
            "region",
            "postal_code",
            "country",
            "phone",
            "fax"
        )
    )

    # ========================================================
    # NEW CUSTOMERS
    # ========================================================

    new_df = (
        source_df.alias("s")
        .join(
            current_df.alias("t"),
            col("s.customer_id") == col("t.customer_id"),
            "left_anti"
        )
        .withColumn(
            "effective_from",
            current_timestamp()
        )
        .withColumn(
            "effective_to",
            lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            lit(True)
        )
    )

    # ========================================================
    # WRITE NEW CUSTOMERS
    # ========================================================
    if not new_df.isEmpty():

        new_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)


    # ========================================================
    # CHANGED CUSTOMERS
    # ========================================================
    changed_df = (
        source_df.alias("s")
        .join(
            current_df.alias("t"),
            col("s.customer_id") == col("t.customer_id"),
            "inner"
        )
        .filter(
            (coalesce(col("s.company_name"), lit("")) != coalesce(col("t.company_name"), lit("")))
            |
            (coalesce(col("s.contact_name"), lit("")) != coalesce(col("t.contact_name"), lit("")))
            |
            (coalesce(col("s.contact_title"), lit("")) != coalesce(col("t.contact_title"), lit("")))
            |

            (coalesce(col("s.cust_address"), lit("")) != coalesce(col("t.cust_address"), lit("")))
            |
            (coalesce(col("s.city"), lit("")) != coalesce(col("t.city"), lit("")))
            |
            (coalesce(col("s.region"), lit("")) != coalesce(col("t.region"), lit("")))
            |
            (coalesce(col("s.postal_code"), lit("")) != coalesce(col("t.postal_code"), lit("")))
            |
            (coalesce(col("s.country"), lit("")) != coalesce(col("t.country"), lit("")))
            |
            (coalesce(col("s.phone"), lit("")) != coalesce(col("t.phone"), lit("")))
            |
            (coalesce(col("s.fax"), lit("")) != coalesce(col("t.fax"), lit("")))
        )
        .select("s.*")
        .dropDuplicates(["customer_id"])
    )

    # ========================================================
    # EXPIRE OLD RECORDS
    # ========================================================
    if not changed_df.isEmpty():

        (
            target.alias("t")
            .merge(
                changed_df.alias("s"),
                """
                t.customer_id = s.customer_id
                AND t.is_current = true
                """
            )
            .whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "effective_to": "current_timestamp()"
                }
            )
            .execute()
        )

        # ====================================================
        # CREATE NEW VERSION
        # ====================================================
        new_version_df = (
            changed_df
            .withColumn(
                "effective_from",
                current_timestamp()
            )
            .withColumn(
                "effective_to",
                lit(None).cast("timestamp")
            )
            .withColumn(
                "is_current",
                lit(True)
            )
        )

        # ====================================================
        # INSERT NEW VERSION
        # ====================================================
        new_version_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)

# ============================================================
# 3. START STREAMING
# ============================================================
query = (
    silver_customer.writeStream
    .foreachBatch(scd2)
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/dim_customers"
    )
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# PRODUCTS WITH SCD2
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# ============================================================
# 2. READ SILVER PRODUCTS STREAM
# ============================================================
silver_products = (
    spark.readStream
    .table("structured_streaming.silver.products")
)

# ============================================================
# 3. SCD TYPE 2 FUNCTION
# ============================================================
def scd2(batch_df, batch_id):

    # Skip empty micro-batches
    if batch_df.isEmpty():
        return

    # --------------------------------------------------------
    # Target
    # --------------------------------------------------------
    target = DeltaTable.forName(
        spark,
        "structured_streaming.gold.dim_products"
    )

    # --------------------------------------------------------
    # Read current records from Gold
    # --------------------------------------------------------
    current_df = (
        spark.table("structured_streaming.gold.dim_products")
        .filter(col("is_current") == True)
    )

    # ========================================================
    # 4. FIND NEW PRODUCTS
    # ========================================================
    new_df = (
        batch_df.alias("s")
        .join(
            current_df.alias("t"),
            on="product_id",
            how="left_anti"
        )
        .withColumn(
            "effective_from",
            current_timestamp()
        )
        .withColumn(
            "effective_to",
            lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            lit(True)
        )
    )

    # ========================================================
    # 5. INSERT NEW PRODUCTS
    # ========================================================
    if not new_df.isEmpty():
        (
            new_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(
                "structured_streaming.gold.dim_products"
            )
        )

    # ========================================================
    # 6. FIND CHANGED PRODUCTS
    # ========================================================
    changed_df = (
        batch_df.alias("s")
        .join(
            current_df.alias("t"),
            on="product_id",
            how="inner"
        )
        .filter(
            ~(
                col("s.product_name").eqNullSafe(col("t.product_name")) &
                col("s.category_name").eqNullSafe(col("t.category_name")) &
                col("s.quantity_per_unit").eqNullSafe(col("t.quantity_per_unit")) &
                col("s.unit_price").eqNullSafe(col("t.unit_price")) &
                col("s.units_in_stock").eqNullSafe(col("t.units_in_stock")) &
                col("s.units_on_order").eqNullSafe(col("t.units_on_order")) &
                col("s.reorder_level").eqNullSafe(col("t.reorder_level")) &
                col("s.discontinued").eqNullSafe(col("t.discontinued")) &
                col("s.supplier_name").eqNullSafe(col("t.supplier_name")) &
                col("s.supplier_contact").eqNullSafe(col("t.supplier_contact")) &
                col("s.supplier_contact_title").eqNullSafe(col("t.supplier_contact_title")) &
                col("s.supplier_address").eqNullSafe(col("t.supplier_address")) &
                col("s.supplier_city").eqNullSafe(col("t.supplier_city")) &
                col("s.supplier_region").eqNullSafe(col("t.supplier_region")) &
                col("s.supplier_postal_code").eqNullSafe(col("t.supplier_postal_code")) &
                col("s.supplier_country").eqNullSafe(col("t.supplier_country")) &
                col("s.supplier_phone").eqNullSafe(col("t.supplier_phone")) &
                col("s.supplier_fax").eqNullSafe(col("t.supplier_fax"))
            )
        )
        .select("s.*")
    )

    # ========================================================
    # 7. EXPIRE OLD RECORDS
    # ========================================================
    if not changed_df.isEmpty():
        (
            target.alias("t")
            .merge(
                changed_df.alias("s"),
                """
                t.product_id = s.product_id
                AND t.is_current = true
                """
            )
            .whenMatchedUpdate(
                set={
                    "is_current": "false",
                    "effective_to": "current_timestamp()"
                }
            )
            .execute()
        )

        # ====================================================
        # 8. INSERT NEW VERSIONS
        # ====================================================
        new_versions_df = (
            changed_df
            .withColumn(
                "effective_from",
                current_timestamp()
            )
            .withColumn(
                "effective_to",
                lit(None).cast("timestamp")
            )
            .withColumn(
                "is_current",
                lit(True)
            )
        )

        (
            new_versions_df.write
            .format("delta")
            .mode("append")
            .saveAsTable(
                "structured_streaming.gold.dim_products"
            )
        )

# ============================================================
# 9. WRITE STREAM USING FOREACHBATCH
# ============================================================
query = (
    silver_products.writeStream
    .foreachBatch(scd2)
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/dim_products"
    )
    .outputMode("update")
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# EMPLOYEES WITH SCD2
# ============================================================
# 1. READ SILVER STREAM
# ============================================================
silver_employees = (
    spark.readStream
    .table("structured_streaming.silver.employees")
)

# ============================================================
# 2. SCD TYPE 2 FUNCTION
# ============================================================
def scd2(batch_df, batch_id):

    # Skip empty micro-batches
    if batch_df.isEmpty():
        return

    # --------------------------------------------------------
    # Target
    # --------------------------------------------------------
    target = DeltaTable.forName(
        spark,
        "structured_streaming.gold.dim_employees"
    )

    # --------------------------------------------------------
    # Get current records
    # --------------------------------------------------------
    current_df = (
        spark.table("structured_streaming.gold.dim_employees")
        .filter(col("is_current") == True)
    )

    # ========================================================
    # 4. FIND NEW EMPLOYEES
    # ========================================================
    new_df = (
        batch_df.alias("s")
        .join(
            current_df.alias("t"),
            "employee_id",
            "left_anti"
        )
        .select(
            col("s.employee_id"),
            col("s.employee_name"),
            col("s.title"),
            col("s.city"),
            col("s.country"),
            col("s.reportsTo"),
            col("s.updated_timestamp").alias("updated_time")
        )
        .withColumn(
            "effective_from",
            current_timestamp()
        )
        .withColumn(
            "effective_to",
            lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            lit(True)
        )
    )

    # --------------------------------------------------------
    # Write NEW employees
    # --------------------------------------------------------
    (
        new_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "structured_streaming.gold.dim_employees"
        )
    )

    # ========================================================
    # 5. FIND CHANGED EMPLOYEES
    # ========================================================
    changed_df = (
        batch_df.alias("s")
        .join(
            current_df.alias("t"),
            "employee_id"
        )
        .filter(
            (~col("s.employee_name").eqNullSafe(col("t.employee_name"))) |
            (~col("s.title").eqNullSafe(col("t.title"))) |
            (~col("s.city").eqNullSafe(col("t.city"))) |
            (~col("s.country").eqNullSafe(col("t.country"))) |
            (~col("s.reportsTo").eqNullSafe(col("t.reportsTo")))
        )
        .select(
            col("s.employee_id"),
            col("s.employee_name"),
            col("s.title"),
            col("s.city"),
            col("s.country"),
            col("s.reportsTo"),
            col("s.updated_timestamp").alias("updated_time")
        )
    )

    # ========================================================
    # 6. EXPIRE OLD RECORDS
    # ========================================================
    (
        target.alias("t")
        .merge(
            changed_df.alias("s"),
            """
            t.employee_id = s.employee_id
            AND t.is_current = true
            """
        )
        .whenMatchedUpdate(
            set={
                "is_current": "false",
                "effective_to": "current_timestamp()"
            }
        )
        .execute()
    )

    # ========================================================
    # 7. INSERT NEW VERSIONS
    # ========================================================
    new_versions_df = (
        changed_df
        .withColumn(
            "effective_from",
            current_timestamp()
        )
        .withColumn(
            "effective_to",
            lit(None).cast("timestamp")
        )
        .withColumn(
            "is_current",
            lit(True)
        )
    )

    (
        new_versions_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "structured_streaming.gold.dim_employees"
        )
    )

# ============================================================
# 8. WRITE STREAM USING FOREACHBATCH
# ============================================================
query = (
    silver_employees.writeStream
    .foreachBatch(scd2)
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/dim_employees"
    )
    .trigger(availableNow=True)
    .start()
)

# COMMAND ----------

# SHIPMENTS
# =================================================
# IMPORTS
# =================================================
from pyspark.sql.functions import *

# ============================================================
# 1. READ SILVER STREAM
# ============================================================
silver_shipments = (
    spark.readStream
    .table("structured_streaming.silver.shipments")
)

# ============================================================
# 2. WRITE STREAM
# ============================================================

query = (
    silver_shipments.writeStream
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/dim_shipments"
    )
    .outputMode("update")
    .trigger(availableNow=True)
    .start()
)



# COMMAND ----------

# FACT ORDERS
# =================================================
# IMPORTS
# =================================================
from pyspark.sql import functions as F

# ============================================================
# 1. READ SILVER FACT STREAM
# ============================================================
silver_fact = (
    spark.readStream
    .table("structured_streaming.silver.orders")
)

# ============================================================
# 2. PROCESS FACT BATCH
# ============================================================
def process_fact_batch(batch_df, batch_id):

    # Skip empty micro-batches
    if batch_df.isEmpty():
        return


    # ========================================================
    # 3. READ CURRENT GOLD DIMENSIONS
    # ========================================================
    dim_customer = (
        spark.table("structured_streaming.gold.dim_customers")
        .filter(F.col("is_current") == True)
        .select(
            "customer_id",
            "customer_sk"
        )
    )

    dim_product = (
        spark.table("structured_streaming.gold.dim_products")
        .filter(F.col("is_current") == True)
        .select(
            "product_id",
            "product_sk"
        )
    )

    dim_employee = (
        spark.table("structured_streaming.gold.dim_employees")
        .filter(F.col("is_current") == True)
        .select(
            "employee_id",
            "employee_sk"
        )
    )

    dim_shipments = (
        spark.table("structured_streaming.gold.dim_shipments")
        .filter(F.col("is_current") == True)
        .select(
            "order_id"
        )
    )

    # ========================================================
    # 4. JOIN SILVER FACT WITH GOLD DIMENSIONS
    # ========================================================
    fact_df = (
        batch_df.alias("f")
        # Customer
        .join(
            dim_customer.alias("c"),
            F.col("f.customer_id") == F.col("c.customer_id"),
            "left"
        )
        # Product
        .join(
            dim_product.alias("p"),
            F.col("f.product_id") == F.col("p.product_id"),
            "left"
        )
        # Employee
        .join(
            dim_employee.alias("e"),
            F.col("f.employee_id") == F.col("e.employee_id"),
            "left"
        )
        # Shipment
        .join(
            dim_shipments.alias("s"),
            F.col("f.order_id") == F.col("s.order_id"),
            "left"
        )
        # SELECT FACT COLUMNS
        .select(
            F.col("f.order_id"),
            F.col("f.product_id"),
            F.col("c.customer_sk"),
            F.col("p.product_sk"),
            F.col("e.employee_sk"),
            F.col("f.order_date"),
            F.col("f.quantity"),
            F.col("f.unit_price"),
            F.col("f.discount"),
            (
                F.col("f.quantity")
                * F.col("f.unit_price")
                * (1 - F.col("f.discount"))
            ).alias("sales_amount"),
            F.current_timestamp().alias("ingestion_time")
        )
    )

    # ========================================================
    # WRITE TO GOLD FACT TABLE
    # ========================================================
    (
        fact_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(
            "structured_streaming.gold.fact_orders"
        )
    )

# ============================================================
# 7. WRITE STREAM USING FOREACHBATCH
# ============================================================
query = (
    silver_fact.writeStream
    .foreachBatch(process_fact_batch)
    .option(
        "checkpointLocation",
        "/Volumes/structured_streaming/default/checkpoint/fact_orders"
    )
    .outputMode("append")
    .trigger(availableNow=True)
    .start()
)
