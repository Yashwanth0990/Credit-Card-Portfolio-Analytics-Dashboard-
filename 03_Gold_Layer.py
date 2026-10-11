# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE credit_card_project.gold.dim_customer AS
# MAGIC SELECT *,
# MAGIC   CASE
# MAGIC     WHEN Credit_Utilization_Ratio >= 0.80 OR Missed_Payments_Last_6M >= 2 THEN 'High Risk'
# MAGIC     WHEN Credit_Utilization_Ratio >= 0.50 OR Missed_Payments_Last_6M = 1 THEN 'Medium Risk'
# MAGIC     ELSE 'Low Risk'
# MAGIC   END AS Risk_Tier
# MAGIC FROM credit_card_project.gold.dim_customer;

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id, col

# --- Gold: dim_customer — add surrogate key ---
df_cust_gold = (spark.table("credit_card_project.silver.dim_customer")
    .withColumn("customer_sk", monotonically_increasing_id())
)
df_cust_gold.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.gold.dim_customer")

# --- Gold: dim_merchant — add surrogate key ---
df_merch_gold = (spark.table("credit_card_project.silver.dim_merchant")
    .withColumn("merchant_sk", monotonically_increasing_id())
)
df_merch_gold.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.gold.dim_merchant")

# --- Gold: dim_card_product ---
df_card_gold = (spark.table("credit_card_project.silver.dim_card_product")
    .withColumn("card_product_sk", monotonically_increasing_id())
)
df_card_gold.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.gold.dim_card_product")

# --- Gold: dim_date — Date_ID is already a clean integer key, keep as-is ---
spark.table("credit_card_project.silver.dim_date") \
    .write.format("delta").mode("overwrite").saveAsTable("credit_card_project.gold.dim_date")

# --- Gold: fact_card_transactions — join to surrogate keys ---
fact = spark.table("credit_card_project.silver.fact_card_transactions")
cust_lookup = df_cust_gold.select("Customer_ID", "customer_sk")
merch_lookup = df_merch_gold.select("Merchant_ID", "merchant_sk")

fact_gold = (fact
    .join(cust_lookup, on="Customer_ID", how="left")
    .join(merch_lookup, on="Merchant_ID", how="left")
)
fact_gold.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.gold.fact_card_transactions")