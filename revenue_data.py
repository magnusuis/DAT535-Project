from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, round

# Initialize Spark session
spark = SparkSession.builder.appName("CreateRevenueData").getOrCreate()

df = spark.read.parquet("hdfs:///data/cleaned/clean_customerData.parquet")

churned_df = df.filter(col("churn") == 1)

metrics_df = churned_df.groupBy("Gender", "Senior", "Partner", "Dependents") \
    .agg(
        round(F.avg("MonthsSubscribed"),2).alias("Average Length"),
        round(F.avg("TotalCharges"),2).alias("Average CLTV"),
        F.count("customerID").alias("Customer Count")
    ) \
    .orderBy(F.col("Customer Count").desc())

metrics_df.show(truncate=False)

metrics_df.write.parquet("hdfs:///data/processed/customerAnalytics.parquet")

spark.stop()
