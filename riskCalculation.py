from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import col, round

# Initialize Spark session
spark = SparkSession.builder.appName("ContractRevenueMetrics").getOrCreate()

df_clean = spark.read.parquet("hdfs:///data/cleaned/clean_customerData.parquet")

# Add Risk Points column
df_clean = df_clean.withColumn("RiskPoints", F.lit(0))

# Load the 5 risk label partitions
df_demographic = spark.read.parquet("hdfs:///data/processed/demographicKMean.parquet")
df_charge = spark.read.parquet("hdfs:///data/processed/chargeKMean.parquet")
df_contract = spark.read.parquet("hdfs:///data/processed/contractKMean.parquet")
df_payment = spark.read.parquet("hdfs:///data/processed/paymentKMean.parquet")
df_services = spark.read.parquet("hdfs:///data/processed/servicesKMean.parquet")

# Function to add risk points for each of the five partitions
def add_risk_points(df_clean, df_partition):
    # Join the df_partition with df_clean on 'customerID' to get the 'Risk Label'
    df_with_risk = df_clean.join(df_partition.select("customerID", "Risk Label"), on="customerID", how="left_outer")
    
    # Add RiskPoints based on the 'Risk Label'
    df_with_risk = df_with_risk.withColumn(
        "RiskPoints",
        F.when(F.col("Risk Label") == "Risk", F.col("RiskPoints") + 1).otherwise(F.col("RiskPoints"))
    )
    
    # Remove the 'Risk Label' column as it's no longer needed
    df_with_risk = df_with_risk.drop("Risk Label")
    
    # Return the updated DataFrame with RiskPoints
    return df_with_risk

# Apply the function to each partition and accumulate the results
df_clean = add_risk_points(df_clean, df_demographic)
df_clean = add_risk_points(df_clean, df_charge)
df_clean = add_risk_points(df_clean, df_contract)
df_clean = add_risk_points(df_clean, df_payment)
df_clean = add_risk_points(df_clean, df_services)

# Show the updated DataFrame with the RiskPoints column
#df_clean.select("customerID", "RiskPoints").show(truncate=False)

def add_risk_label(df_clean):
    # Define Risk Label based on RiskPoints
    df_with_risk_label = df_clean.withColumn(
        "Risk Label",
        F.when(F.col("RiskPoints") == 0, "No risk")
        .when((F.col("RiskPoints") >= 1) & (F.col("RiskPoints") <= 2), "Low risk")
        .when(F.col("RiskPoints") == 3, "Medium risk")
        .when((F.col("RiskPoints") >= 4) & (F.col("RiskPoints") <= 5), "High risk")
        .otherwise("Unknown")  # In case RiskPoints is outside expected range
    )
    
    return df_with_risk_label

# Add Risk Label based on RiskPoints
df_clean = add_risk_label(df_clean)

# Show the final DataFrame with Risk Label
df_clean.select("customerID", "RiskPoints", "Risk Label").show(truncate=False)

# Filter df_clean to only include customers with Churn == 0
df_clean_no_churn = df_clean.filter(F.col("churn") == 0)

# Separate the DataFrame into different Risk Labels for customers with Churn == 0
df_no_risk = df_clean_no_churn.filter(F.col("Risk Label") == "No risk")
df_low_risk = df_clean_no_churn.filter(F.col("Risk Label") == "Low risk")
df_medium_risk = df_clean_no_churn.filter(F.col("Risk Label") == "Medium risk")
df_high_risk = df_clean_no_churn.filter(F.col("Risk Label") == "High risk")

# Save each DataFrame to separate Parquet files
df_no_risk.write.parquet("hdfs:///data/processed/no_risk.parquet")
df_low_risk.write.parquet("hdfs:///data/processed/low_risk.parquet")
df_medium_risk.write.parquet("hdfs:///data/processed/medium_risk.parquet")
df_high_risk.write.parquet("hdfs:///data/processed/high_risk.parquet")

# Save the updated DataFrame to a Parquet file
#df_clean.write.parquet("path/to/clean_data_with_risk.parquet")
