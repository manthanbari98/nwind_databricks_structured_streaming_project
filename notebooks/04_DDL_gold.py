# Databricks notebook source

# =================================================
# CUSTOMERS
# =================================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS structured_streaming.gold.dim_customers (
                customer_sk				        BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                customer_id				        STRING,
                company_name			        STRING,
                contact_name			        STRING,
                contact_title			        STRING,
                cust_address			        STRING,
                city					        STRING,
                region					        STRING,
                postal_code				        STRING,
                country					        STRING,
                phone					        STRING,
                fax					            STRING,
                ingestion_time                  TIMESTAMP,
                updated_time                    TIMESTAMP,
                effective_from                  TIMESTAMP,
                effective_to                    TIMESTAMP,
                is_current                      BOOLEAN)
                """)

# COMMAND ----------

# =================================================
# PRODUCTS
# =================================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS structured_streaming.gold.dim_products (
                product_sk			      BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                product_id			      INT,
                product_name		            STRING,
                category_name			      STRING,
                quantity_per_unit		      STRING,
                unit_price			      DOUBLE,
                units_in_stock		      INT,
                units_on_order		      INT,
                reorder_level		            INT,
                discontinued		            BOOLEAN,
                supplier_name			      STRING,
                supplier_contact		      STRING,
                supplier_contact_title          STRING,
                supplier_address		      STRING,
                supplier_city		            STRING,
                supplier_region		      STRING,
                supplier_postal_code            STRING,
                supplier_country		      STRING,
                supplier_phone		      STRING,
                supplier_fax		            STRING,
                updated_time                    TIMESTAMP,
                effective_from                  TIMESTAMP,
                effective_to                    TIMESTAMP,
                is_current                      BOOLEAN)
                """)

# COMMAND ----------

# =================================================
# EMPLOYEES
# =================================================
spark.sql("""
CREATE TABLE IF NOT EXISTS structured_streaming.gold.dim_employees (
    employee_sk       BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
    employee_id       INT,
    employee_name     STRING,
    title             STRING,
    city              STRING,
    country           STRING,
    reportsTo         INT,
    updated_time      TIMESTAMP,
    effective_from    TIMESTAMP,
    effective_to      TIMESTAMP,
    is_current        BOOLEAN
)
""")

# COMMAND ----------

# =================================================
# SHIPMENTS
# =================================================
spark.sql("""
          CREATE TABLE IF NOT EXISTS structured_streaming.gold.dim_shipments (
                order_id			      INT,
                customer_id			      STRING,
                employee_id			      INT,
                order_date			      STRING,
                required_date		            STRING,
                shipped_date		            STRING,
                shipper_name			      INT,
                freight				      DOUBLE,
                ship_name			      STRING,
                ship_address		            STRING,
                ship_city			      STRING,
                ship_region			      STRING,
                ship_postal_code	            STRING,
                ship_country		            STRING,
                phone				      STRING,
                updated_time                    TIMESTAMP,
                effective_from                  TIMESTAMP,
                effective_to                    TIMESTAMP,  
                is_current                      BOOLEAN)
          """)
