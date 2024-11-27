from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import round

# Initialize Spark session
spark = SparkSession.builder.appName("FetchDashboardData").getOrCreate()

# Load the cleaned data from parquet
df = spark.read.parquet("hdfs:///data/cleaned/clean_customerData.parquet")

# Calculate count of active customers (churn = 0)
active_customers_count = df.filter(F.col("churn") == 0).count()

# Calculate total number of customers
total_customers_count = df.count()

# Calculate expected revenue next month (based on MonthlyCost for active customers)
expected_revenue = df.filter(F.col("churn") == 0) \
    .agg(round(F.sum("MonthlyCost"), 2).alias("Expected Revenue Next Month")) \
    .collect()[0]["Expected Revenue Next Month"]

# Create a summary DataFrame with the results
summary_df = spark.createDataFrame([(active_customers_count, total_customers_count, expected_revenue)], 
                                   ["Active Customers", "Total Customers", "Expected Revenue Next Month"])

# Show the summary DataFrame
summary_df.show(truncate=False)

summary_df.write.parquet("hdfs:///data/processed/summaryAnalytics.parquet")

spark.stop()
