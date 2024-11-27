from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, round

# Initialize Spark session
spark = SparkSession.builder.appName("ContractRevenueMetrics").getOrCreate()

df = spark.read.parquet("hdfs:///data/cleaned/clean_customerData.parquet")

churned_df = df.filter(col("churn") == 1)

contract_df = churned_df.groupBy("ContractType") \
    .agg(
        round(F.avg("MonthsSubscribed"),2).alias("Average Length"),
        round(F.avg("TotalCharges"),2).alias("Average CLTV"),
        F.count("customerID").alias("Customer Count")
    ) \
    .orderBy(F.col("Customer Count").desc())

contract_df.show(truncate=False)

payment_df = churned_df.groupBy("PaymentMethod") \
    .agg(
        round(F.avg("MonthsSubscribed"),2).alias("Average Length"),
        round(F.avg("TotalCharges"),2).alias("Average CLTV"),
        F.count("customerID").alias("Customer Count")
    ) \
    .orderBy(F.col("Customer Count").desc())

payment_df.show(truncate=False)

combined_df = churned_df.groupBy("ContractType", "PaymentMethod") \
    .agg(
        round(F.avg("MonthsSubscribed"),2).alias("Average Length"),
        round(F.avg("TotalCharges"),2).alias("Average CLTV"),
        F.count("customerID").alias("Customer Count")
    ) \
    .orderBy(F.col("Customer Count").desc())

combined_df.show(truncate=False)

contract_df.write.parquet("hdfs:///data/processed/contractAnalytics.parquet")

payment_df.write.parquet("hdfs:///data/processed/paymentAnalytics.parquet")

combined_df.write.parquet("hdfs:///data/processed/contractPaymentAnalytics.parquet")

spark.stop()
