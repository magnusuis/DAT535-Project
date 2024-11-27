from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import round

# Initialize Spark session
spark = SparkSession.builder.appName("MonthlyChargeGroups").getOrCreate()

# Load the cleaned data from parquet
df = spark.read.parquet("hdfs:///data/cleaned/clean_customerData.parquet")

churned_df = df.filter(F.col("churn") == 1)

# Define the monthly charge ranges using the 'when' function
df_with_charge_range = churned_df.withColumn(
    "ChargeRange",
    F.when(F.col("MonthlyCost").between(0, 24), "0-24")  # 0 to 24
     .when(F.col("MonthlyCost").between(25, 48), "25-48")  # 25 to 48
     .when(F.col("MonthlyCost").between(49, 72), "49-72")  # 49 to 72
     .when(F.col("MonthlyCost").between(73, 96), "73-96")  # 73 to 96
     .otherwise("97+")  # 97 and above
)

# Group by the new charge range and count the number of customers in each range
charge_range_df = df_with_charge_range.groupBy("ChargeRange") \
    .agg(
        round(F.avg("MonthsSubscribed"),2).alias("Average Length"),
        F.count("customerID").alias("Customer Count")
    ) \
    .orderBy(F.col("ChargeRange").desc())

# Show the result
charge_range_df.show(truncate=False)

charge_range_df.write.parquet("hdfs:///data/processed/chargeAnalytics.parquet")

# Stop Spark session
spark.stop()

