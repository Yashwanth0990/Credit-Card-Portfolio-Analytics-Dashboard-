# Databricks notebook source
from pyspark.sql.functions import col, to_date, trim, initcap, when, regexp_replace

# --- Silver: Dim_Customer ---
df_cust = spark.table("credit_card_project.bronze.dim_customer")

df_cust_silver = (df_cust
    .withColumn("Customer_ID", trim(col("Customer_ID")))
    .withColumn("First_Name", initcap(trim(col("First_Name"))))
    .withColumn("Last_Name", initcap(trim(col("Last_Name"))))
    .withColumn("Age", col("Age").cast("int"))
    .withColumn("Credit_Score", col("Credit_Score").cast("int"))
    .withColumn("Credit_Limit", col("Credit_Limit").cast("double"))
    .withColumn("Outstanding_Balance", col("Outstanding_Balance").cast("double"))
    .withColumn("Credit_Utilization_Ratio", col("Credit_Utilization_Ratio").cast("double"))
    .withColumn("Missed_Payments_Last_6M", col("Missed_Payments_Last_6M").cast("int"))
    .withColumn("Default_Status", col("Default_Status").cast("int"))     # 0/1 flag
    .withColumn("Customer_Since", to_date(col("Customer_Since")))
    .withColumn("Tenure_Years", col("Tenure_Years").cast("double"))
    .withColumn("Phone", regexp_replace(col("Phone").cast("string"), "\\.0$", ""))  # strip trailing .0
    .withColumn("Email", trim(col("Email")))
    .dropDuplicates(["Customer_ID"])
    .filter(col("Customer_ID").isNotNull())
)
df_cust_silver.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.silver.dim_customer")

# --- Silver: Dim_Merchant ---
df_merch = spark.table("credit_card_project.bronze.dim_merchant")
df_merch_silver = (df_merch
    .withColumn("Is_Online", col("Is_Online").cast("boolean"))
    .withColumn("Avg_Transaction_Value", col("Avg_Transaction_Value").cast("double"))
    .dropDuplicates(["Merchant_ID"])
)
df_merch_silver.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.silver.dim_merchant")

# --- Silver: Dim_Card_Product ---
df_card = spark.table("credit_card_project.bronze.dim_card_product")
df_card_silver = (df_card
    .withColumn("Annual_Fee", col("Annual_Fee").cast("double"))
    .withColumn("Reward_Rate_Pct", col("Reward_Rate_Pct").cast("double"))
    .withColumn("Cashback_Pct", col("Cashback_Pct").cast("double"))
    .dropDuplicates(["Card_Product_ID"])
)
df_card_silver.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.silver.dim_card_product")

# --- Silver: Dim_Date ---
df_date = spark.table("credit_card_project.bronze.dim_date")
df_date_silver = (df_date
    .withColumn("Date_ID", col("Date_ID").cast("int"))
    .withColumn("Full_Date", to_date(col("Full_Date")))
    .withColumn("Is_Weekend", col("Is_Weekend").cast("boolean"))
    .withColumn("Is_Holiday", col("Is_Holiday").cast("boolean"))
    .dropDuplicates(["Date_ID"])
)
df_date_silver.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.silver.dim_date")

# --- Silver: Fact_Card_Transactions ---
df_txn = spark.table("credit_card_project.bronze.fact_card_transactions")
df_txn_silver = (df_txn
    .withColumn("Date_ID", col("Date_ID").cast("int"))
    .withColumn("Transaction_Date", to_date(col("Transaction_Date")))
    .withColumn("Transaction_Amount", col("Transaction_Amount").cast("double"))
    .withColumn("Reward_Points_Earned", col("Reward_Points_Earned").cast("int"))
    .withColumn("Is_International", col("Is_International").cast("boolean"))
    .filter(col("Transaction_ID").isNotNull() & col("Customer_ID").isNotNull())
    .dropDuplicates(["Transaction_ID"])
)
df_txn_silver.write.format("delta").mode("overwrite").saveAsTable("credit_card_project.silver.fact_card_transactions")