from pyspark.sql import SparkSession, functions as F
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerRiskClustering").getOrCreate()

# Load the cleaned data from parquet
df = spark.read.parquet("hdfs:///data/cleaned/clean_customerData.parquet")

df_0_24 = df.filter((F.col("MonthlyCost") >= 0) & (F.col("MonthlyCost") <= 24))
df_25_48 = df.filter((F.col("MonthlyCost") > 24) & (F.col("MonthlyCost") <= 48))
df_49_72 = df.filter((F.col("MonthlyCost") > 48) & (F.col("MonthlyCost") <= 72))
df_73_96 = df.filter((F.col("MonthlyCost") > 72) & (F.col("MonthlyCost") <= 96))
df_97_plus = df.filter(F.col("MonthlyCost") > 96)

# Prepare the dataset for clustering by creating a feature vector
assembler = VectorAssembler(inputCols=["MonthsSubscribed"], outputCol="features")

# Initialize an empty DataFrame to store the results from all partitions
final_combined_df = None

for df_combination in [df_0_24, df_25_48, df_49_72, df_73_96, df_97_plus]:
    clustering_data = assembler.transform(df_combination)

    # Perform K-Means clustering on the entire dataset
    kmeans = KMeans().setK(30).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(clustering_data)
    predictions = model.transform(clustering_data)

    # Classify customers as "Risk" or "Not Risk" based on cluster neighbors
    # Use cluster-wise churn average to determine risk
    risk_df = predictions.groupBy("prediction").agg(
        F.avg("churn").alias("Churn Rate")
    ).withColumn(
        "Risk Label", F.when(F.col("Churn Rate") >= 0.5, "Risk").otherwise("Not Risk")
    )

    # Join the Risk Labels back to the predictions
    final_df = predictions.join(risk_df, "prediction")

    # Filter to only show non-churned customers with their risk label
    non_churned_risk_df = final_df.filter(F.col("churn") == 0) \
        .select("customerID", "MonthlyCost", "MonthsSubscribed", "Risk Label")

    # Show the final results for non-churned customers
    non_churned_risk_df.show(truncate=False)

    # Combine the results into a single DataFrame
    if final_combined_df is None:
        final_combined_df = non_churned_risk_df
    else:
        final_combined_df = final_combined_df.union(non_churned_risk_df)

total_customers = final_combined_df.count()

# Calculate percentage of "Risk" and "Not Risk" customers within each cluster
risk_analysis_summary = final_combined_df.groupBy("Risk Label").agg(
	F.count("customerID").alias("Total Customers"),
    	F.round((F.count("customerID") / total_customers) * 100, 2).alias("Percentage")
)

risk_analysis_summary.show(truncate=False)

final_combined_df.write.parquet("hdfs:///data/processed/chargeKMean.parquet")

# Stop Spark session
spark.stop()
