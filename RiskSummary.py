from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, round as F_round

# Initialize Spark session
spark = SparkSession.builder.appName("RiskSummary").getOrCreate()

# Load the clean data and risk label Parquet files
df_clean = spark.read.parquet("hdfs:///data/cleaned/clean_customerData.parquet").filter(col("Churn") == 0)

# Calculate total non-churned customers
total_customers = df_clean.count()

# Load risk label files and get their counts
risk_data = [
    ("No Risk", spark.read.parquet("hdfs:///data/processed/no_risk.parquet").count()),
    ("Low Risk", spark.read.parquet("hdfs:///data/processed/low_risk.parquet").count()),
    ("Medium Risk", spark.read.parquet("hdfs:///data/processed/medium_risk.parquet").count()),
    ("High Risk", spark.read.parquet("hdfs:///data/processed/high_risk.parquet").count())
]

# Create a DataFrame from the risk data
risk_summary_df = spark.createDataFrame(risk_data, ["Risk Label", "Customer Count"])

# Calculate and add the percentage column
risk_summary_df = risk_summary_df.withColumn(
    "Percentage",
    F_round((col("Customer Count") / lit(total_customers)) * 100, 2)
)

# Show the result
risk_summary_df.show(truncate=False)

# Save the summary to Parquet
risk_summary_df.write.parquet("hdfs:///data/processed/risk_summary.parquet")

spark.stop()

