# Databricks notebook source
catalog = "credit_card_project"
schema = "bronze"
volume = "raw_files"

raw_path = f"/Volumes/{catalog}/{schema}/{volume}/"

print(raw_path)

# COMMAND ----------

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

# Bronze ingestion — read as-is, add lineage metadata, write to Delta
from pyspark.sql.functions import current_timestamp, lit, input_file_name

volume_path = "/Volumes/credit_card_project/bronze/raw_files"

file_table_map = {
    "Dim_Customer.csv": "bronze.dim_customer",
    "Dim_Merchant.csv": "bronze.dim_merchant",
    "Dim_Card_Product.csv": "bronze.dim_card_product",
    "Dim_Date.csv": "bronze.dim_date",
    "Fact_Card_Transactions.csv": "bronze.fact_card_transactions",
}

for file_name, table_name in file_table_map.items():
    df = (spark.read
          .option("header", True)
          .option("inferSchema", True)   # Bronze: keep it permissive
          .csv(f"{volume_path}/{file_name}"))

    df = (df.withColumn("_ingestion_timestamp", current_timestamp())
            .withColumn("_source_file", lit(file_name)))

    df.write.format("delta").mode("overwrite").saveAsTable(f"credit_card_project.{table_name}")
    print(f"Loaded {table_name}: {df.count()} rows")