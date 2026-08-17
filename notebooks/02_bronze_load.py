# Databricks notebook source

# CUSTOMERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ CUSTOMERS USING AUTO LOADER
# ============================================================

bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/customers")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/customers")\
    .load("/Volumes/structured_streaming/default/source/customers")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (
        bronze
        .withColumnRenamed("CustomerID", "customer_id")
        .withColumnRenamed("CompanyName", "company_name")
        .withColumnRenamed("ContactName", "contact_name")
        .withColumnRenamed("ContactTitle", "contact_title")
        .withColumnRenamed("Address", "cust_address")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Region", "region")
        .withColumnRenamed("PostalCode", "postal_code")
        .withColumnRenamed("Country", "country")
        .withColumnRenamed("Phone", "phone")
        .withColumnRenamed("Fax", "fax")
    )

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))

# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint/bronze/customers")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.customers")
)

# COMMAND ----------

# CATEGORIES
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ CATEGORIES USING AUTO LOADER
# ============================================================
bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/categories")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/categories")\
    .load("/Volumes/structured_streaming/default/source/categories")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (bronze
            .withColumnRenamed("CategoryID", "category_id")
            .withColumnRenamed("CategoryName", "category_name")
            .withColumnRenamed("Description", "description")
            .withColumnRenamed("Picture", "picture"))

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))

# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint/bronze/categories")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.categories")
)

# COMMAND ----------

# EMPLOYEES
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ EMPLOYEES USING AUTO LOADER
# ============================================================
bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/employees")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/employees")\
    .load("/Volumes/structured_streaming/default/source/employees")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (bronze
            .withColumnRenamed("employeeID", "employee_id")
            .withColumnRenamed("employeeName", "employee_name")
            .withColumnRenamed("title", "title")
            .withColumnRenamed("city", "city")
            .withColumnRenamed("country", "country")
            .withColumnRenamed("reportsTo", "reportsTo"))

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))

# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint/bronze/employees")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.employees")
)

# COMMAND ----------

# PRODUCTS
# ============================================================
# 1. IMPORTS
# ============================================================

from pyspark.sql.functions import *

# ============================================================
# 2. READ PRODUCTS USING AUTO LOADER
# ============================================================
bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/products")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/products")\
    .load("/Volumes/structured_streaming/default/source/products")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (bronze
            .withColumnRenamed("ProductID", "product_id")
            .withColumnRenamed("ProductName", "product_name")
            .withColumnRenamed("SupplierID","supplier_id")
            .withColumnRenamed("CategoryID", "category_id")
            .withColumnRenamed("QuantityPerUnit", "quantity_per_unit")
            .withColumnRenamed("UnitPrice", "unit_price")
            .withColumnRenamed("UnitsInStock", "units_in_stock")
            .withColumnRenamed("UnitsOnOrder", "units_on_order")
            .withColumnRenamed("ReorderLevel", "reorder_level")
            .withColumnRenamed("Discontinued", "discontinued"))

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))


# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint/bronze/products")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.products")
)

# COMMAND ----------

# ORERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ ORDERS USING AUTO LOADER
# ============================================================
bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/orders")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/orders")\
    .load("/Volumes/structured_streaming/default/source/orders")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (bronze
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("CustomerID", "customer_id")
            .withColumnRenamed("EmployeeID", "employee_id")
            .withColumnRenamed("OrderDate", "order_date")
            .withColumnRenamed("RequiredDate", "required_date")
            .withColumnRenamed("ShippedDate", "shipped_date")    
            .withColumnRenamed("ShipVia", "ship_via")
            .withColumnRenamed("Freight", "freight")
            .withColumnRenamed("ShipName", "ship_name")
            .withColumnRenamed("ShipAddress", "ship_address")
            .withColumnRenamed("ShipCity", "ship_city")
            .withColumnRenamed("ShipRegion", "ship_region")
            .withColumnRenamed("ShipPostalCode", "ship_postal_code")
            .withColumnRenamed("ShipCountry", "ship_country"))

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))

# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint/bronze/orders")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.orders")
)

# COMMAND ----------

# ORDER_DETAILS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ ORDER DETAILS USING AUTO LOADER
# ============================================================
bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/order_details")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/order_details")\
    .load("/Volumes/structured_streaming/default/source/order_details")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (bronze
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("ProductID", "product_id")
            .withColumnRenamed("UnitPrice", "unit_price")
            .withColumnRenamed("Quantity", "quantity")
            .withColumnRenamed("Discount", "discount")
            .withColumnRenamed("Product Name","product_name"))

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))

# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint/bronze/order_details")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.order_details")
)

# COMMAND ----------

# SUPPLIERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ SUPPLIERS USING AUTO LOADER
# ============================================================
bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/suppliers")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/suppliers")\
    .load("/Volumes/structured_streaming/default/source/suppliers")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (bronze
            .withColumnRenamed("SupplierID", "supplier_id")
            .withColumnRenamed("CompanyName", "company_name")
            .withColumnRenamed("ContactName", "contact_name")
            .withColumnRenamed("ContactTitle", "contact_title")
            .withColumnRenamed("Address", "address")
            .withColumnRenamed("City", "city")
            .withColumnRenamed("Region", "region")
            .withColumnRenamed("PostalCode", "postal_code")
            .withColumnRenamed("Country", "country")
            .withColumnRenamed("Phone", "phone")
            .withColumnRenamed("Fax", "fax")
            .withColumnRenamed("HomePage", "home_page"))

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))


# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint//bronze/suppliers")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.suppliers")
)

# COMMAND ----------

# SHIPPERS
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ SHIPPERS USING AUTO LOADER
# ============================================================
bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/shippers")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/shippers")\
    .load("/Volumes/structured_streaming/default/source/shippers")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (bronze
            .withColumnRenamed("ShipperID", "shipper_id")
            .withColumnRenamed("CompanyName", "company_name")
            .withColumnRenamed("Phone", "phone"))

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))


# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint/bronze/shippers")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.shippers")
)

# COMMAND ----------

# SHIPMENTS 
# ============================================================
# 1. IMPORTS
# ============================================================
from pyspark.sql.functions import *

# ============================================================
# 2. READ SHIPMENTS USING AUTO LOADER
# ============================================================
bronze = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("quote", '"')\
    .option("escape", '"')\
    .option("multiLine", "true")\
    .option("cloudFiles.schemaLocation", "/Volumes/structured_streaming/default/schema/shipments")\
    .option("cloudFiles.cleanSource","MOVE")\
    .option("cloudFiles.cleanSource.moveDestination","/Volumes/structured_streaming/default/archive/shipments")\
    .load("/Volumes/structured_streaming/default/source/shipments")

# ============================================================
# 3. STANDARDIZE COLUMNS
# ============================================================
bronze = (bronze
            .withColumnRenamed("OrderID", "order_id")
            .withColumnRenamed("CustomerID", "customer_id")
            .withColumnRenamed("EmployeeID", "employee_id")
            .withColumnRenamed("OrderDate", "order_date")
            .withColumnRenamed("RequiredDate", "required_date")
            .withColumnRenamed("ShippedDate", "shipped_date")    
            .withColumnRenamed("ShipVia", "ship_via")
            .withColumnRenamed("Freight", "freight")
            .withColumnRenamed("ShipName", "ship_name")
            .withColumnRenamed("ShipAddress", "ship_address")
            .withColumnRenamed("ShipCity", "ship_city")
            .withColumnRenamed("ShipRegion", "ship_region")
            .withColumnRenamed("ShipPostalCode", "ship_postal_code")
            .withColumnRenamed("ShipCountry", "ship_country"))

# ============================================================
# 4. ADD INGESTION TIME AND SOURCE FILE
# ============================================================
bronze = bronze.withColumn("ingestiontime", current_timestamp())\
    .withColumn("source_file", col("_metadata.file_path"))

# ============================================================
# 5. WRITE STREAMING DATA TO BRONZE DELTA TABLE
# ============================================================
(
    bronze.writeStream.format("delta")\
    .option("checkpointLocation", f"/Volumes/structured_streaming/default/checkpoint/bronze/shipments")\
    .trigger(availableNow=True)\
    .toTable("structured_streaming.bronze.shipments")
)
