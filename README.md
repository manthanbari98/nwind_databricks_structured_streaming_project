# Structured Streaming Data Engineering Project

## 📌 Project Overview

This project demonstrates an end-to-end **Structured Streaming data engineering pipeline using Databricks, Apache Spark, Auto Loader, Delta Lake, and PySpark**.

The pipeline follows the **Medallion Architecture**:

```text
Source CSV Files
       │
       ▼
┌──────────────┐
│    BRONZE    │
│ Auto Loader  │
│  Raw Data    │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    SILVER    │
│ Cleaned Data │
│ Transformations
│   & Joins     │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│     GOLD     │
│ Dimensions   │
│   & Facts    │
│   SCD Type 2 │
└──────────────┘
```

The project processes data using Spark Structured Streaming and stores the processed data as Delta tables.

## 🛠️ Technologies Used
- Databricks
- Apache Spark
- PySpark
- Spark Structured Streaming
- Auto Loader
- Delta Lake
- SQL
- Medallion Architecture
- SCD Type 2
- Databricks Volumes
  
## 🏗️ Architecture

The project uses a three-layer Medallion Architecture:

### Bronze Layer

The Bronze layer ingests source CSV files using Databricks Auto Loader (cloudFiles).

### The ingestion process:

- Reads CSV files using Structured Streaming
- Uses schema inference
- Handles quoted and multiline CSV data
- Captures ingestion timestamps
- Captures source file information
- Writes the data into Delta tables
- Uses checkpoints for streaming fault tolerance
- Moves processed source files to an archive location

### The project creates Bronze tables for:

- Customers
- Categories
- Employees
- Products
- Orders
- Order Details
- Suppliers
- Shippers
- Shipments
### 🥈 Silver Layer

The Silver layer performs data cleansing and transformation on the Bronze streaming tables.

### Major transformations include:

- Removing duplicates
- Filtering invalid/null records
- Data type casting
- Column standardization
- Adding update timestamps
- Joining related datasets
- Calculating order-level metrics
- Allocating freight across order-detail records
- Order Detail Processing
```
The order-detail transformation uses:
order_id + product_id
as the grain.

Line amount is calculated as:

line_amount =
unit_price × quantity × (1 - discount)

The total order amount is then calculated at the:
order_id

Freight is allocated to individual order-detail records based on their contribution to the total order amount.

The Silver layer also uses foreachBatch for processing order-related transformations.
```
### 🥇 Gold Layer

The Gold layer contains dimensional tables designed for analytical use.

### The project creates:

- dim_customers
- dim_products
- dim_employees
- dim_shipments

The Gold tables use Delta Lake and implement Slowly Changing Dimension Type 2 (SCD Type 2) logic.

### 🔄 SCD Type 2 Implementation

SCD Type 2 is implemented using foreachBatch and Delta Lake operations.

### The process handles:

- New Records
- New business keys are inserted into the Gold dimension with:
```is_current = true
effective_from = current_timestamp()
effective_to = NULL
```

### Changed Records

### When an existing record changes:

- The current record is identified.
- The existing record is expired.
- is_current is changed to false.
- effective_to is populated.
- A new version of the record is inserted.
- The new record becomes the current version.

### Conceptually:
```
Old Version
--------------------------------
customer_id = C001
is_current  = false
effective_to = change timestamp

              ↓

New Version
--------------------------------
customer_id = C001
is_current  = true
effective_to = NULL

This allows historical changes to be preserved.
```

### 📊 Gold Data Model
```
Dimension Customers
dim_customers
├── customer_sk
├── customer_id
├── company_name
├── contact_name
├── contact_title
├── address
├── city
├── region
├── postal_code
├── country
├── phone
├── fax
├── ingestion_time
├── updated_time
├── effective_from
├── effective_to
└── is_current

Dimension Products
dim_products
├── product_sk
├── product_id
├── product_name
├── category_name
├── quantity_per_unit
├── unit_price
├── units_in_stock
├── units_on_order
├── reorder_level
├── discontinued
├── supplier information
├── updated_time
├── effective_from
├── effective_to
└── is_current

Dimension Employees
dim_employees
├── employee_sk
├── employee_id
├── employee_name
├── title
├── city
├── country
├── reportsTo
├── updated_time
├── effective_from
├── effective_to
└── is_current

Dimension Shipments
dim_shipments
├── order_id
├── customer_id
├── employee_id
├── order_date
├── required_date
├── shipped_date
├── shipper_name
├── freight
├── shipping information
├── updated_time
├── effective_from
├── effective_to
└── is_current
```
### 📂 Project Structure
```
structured-streaming-databricks/
│
├── README.md
│
├── notebooks/
│   ├── 01_init_database.py
│   ├── 02_bronze_load.py
│   ├── 03_silver_transformation.py
│   ├── 04_DDL_gold.py
│   └── 05_gold_layer.py
│
├── architecture/
│   └── architecture.png
│
├── data_model/
│   └── data_model.png
│
└── .gitignore
```
### 📓 Notebooks
```
01_init_database.py

Creates the structured_streaming catalog and the following schemas:

structured_streaming
├── bronze
├── silver
└── gold
02_bronze_load.py

Implements Auto Loader ingestion from CSV source files into Bronze Delta tables.

03_silver_transformation.py

Performs cleansing, deduplication, data type conversions, joins, calculations, and Silver-layer streaming transformations.

04_DDL_gold.py

Creates the Gold dimension tables and defines their schemas.

05_gold_layer.py

Implements SCD Type 2 processing for Gold dimensions using:

Structured Streaming
foreachBatch
Delta Lake
Delta Merge
Current/historical record tracking
🔁 Streaming Design

The project uses Spark Structured Streaming throughout the pipeline.

The streaming queries use:

.writeStream

and checkpoint locations to maintain streaming state.

For example:

.writeStream \
    .format("delta") \
    .option("checkpointLocation", "...") \
    .trigger(availableNow=True)

availableNow allows the pipeline to process currently available files while retaining a streaming architecture.

🔐 Data & Checkpoints

The actual source data, Databricks Volumes, checkpoints, schema locations, and archived files are not included in this repository.

The repository contains the transformation and pipeline code required to understand and reproduce the project structure.

Environment-specific paths such as:

/Volumes/structured_streaming/default/source/

and:

/Volumes/structured_streaming/default/checkpoint/

are specific to the Databricks environment used for this project.
```
### 🎯 Key Data Engineering Concepts Demonstrated

- Spark Structured Streaming
- Databricks Auto Loader
- Delta Lake
- Medallion Architecture
- Bronze/Silver/Gold data layers
- Streaming checkpoints
- foreachBatch
- Data cleansing
- Deduplication
- Stream transformations
- Stream-to-static joins
- Data type conversion
- Window functions within batch processing
- Order-level calculations
- Freight allocation
- Slowly Changing Dimension Type 2
- Delta Lake Merge
- Identity surrogate keys
- Historical data tracking

### 👨‍💻 Author

Manthan Bari

### Technologies:
Python | SQL | PySpark | Apache Spark | Databricks | Delta Lake | Structured Streaming
