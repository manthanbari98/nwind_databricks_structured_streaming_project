# Databricks notebook source
 
# SCRIPT PURPOSE : This script creates a DataBase 'structured_streaming' after checking if it exists already.
#	           The script also creates three schemas : bronze, silver, gold.

catalog_name = "structured_streaming"

# Drop if exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")

# Create schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.gold")
