from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, size, split, regexp_extract, mean

# Initialize Spark session
spark = SparkSession.builder.appName("CustomerDescriptionProcessing").getOrCreate()

# Read the CSV file into a DataFrame
contract_df = spark.read.csv("hdfs:///data/raw/contractData.csv", header=True, inferSchema=True)
customers_df = spark.read.csv("hdfs:///data/raw/customersData.csv", header=True, inferSchema=True)
services_df = spark.read.csv("hdfs:///data/raw/servicesData.csv", header=True, inferSchema=True)

# Perform the join operation on 'customerID'
combined_df = contract_df \
    .join(customers_df, "customerID", "inner") \
    .join(services_df, "customerID", "inner")

# Apply the transformation
df_transformed = combined_df.withColumn(
    "Gender", when(col("customerDescription").rlike("(?i)female"), "Female").otherwise("Male")) \
    .withColumn(
        "Senior", when(col("customerDescription").rlike("(?i)senior"), 1).otherwise(0)) \
    .withColumn(
        "Partner", when(col("customerDescription").rlike("(?i)partner"), 1).otherwise(0)) \
    .withColumn(
        "Dependents", when(col("customerDescription").rlike("(?i)uncertain dependents"), None).when(col("customerDescription").rlike("(?i)dependents"), 1).otherwise(0)) \
    .withColumn(
        "PhoneServiceCount", when(col("phoneDescription").rlike("(?i)multiple lines"), 2)
                               .when(col("phoneDescription").rlike("(?i)Phone service"), 1)
                               .otherwise(0)) \
    .withColumn(
        "InternetServiceCount", when(col("internetDescription").rlike("(?i)None"), 0)
                               .when(col("internetDescription").contains(","), size(split(col("internetDescription"), ",")).cast("int") + 1)
                               .when(col("internetDescription").rlike("(?i)with"), 2)
                               .otherwise(1)) \
    .withColumn(
        "AmountServices", col("PhoneServiceCount") + col("InternetServiceCount")
    ) \
    .withColumn("Churn", when(col("subscriptionDescription").rlike("(?i)Is subscribed"), 0)
                        .when(col("subscriptionDescription").rlike("(?i)Was subscribed"), 1)
                        .otherwise(None)) \
    .withColumn("MonthsSubscribed", regexp_extract(col("subscriptionDescription"), r"(\d+)\s+months", 1).cast("int")) \
    .withColumn("MonthlyCost", regexp_extract(col("subscriptionDescription"), r"monthly cost of\s+(\d+\.\d+)", 1).cast("double")) \
    .withColumn("PaymentMethod", when(col("contractDescription").rlike("(?i)Electronic check"), "Electronic check")
                .when(col("contractDescription").rlike("(?i)Credit card \\(automatic\\)"), "Credit card (automatic)")
                .when(col("contractDescription").rlike("(?i)Bank transfer \\(automatic\\)"), "Bank transfer (automatic)")
                .when(col("contractDescription").rlike("(?i)Mailed check"), "Mailed check")
                .otherwise("Unknown")) \
    .withColumn("ContractType", when(col("contractDescription").rlike("(?i)Month-to-month"), "Month-to-month")
                .when(col("contractDescription").rlike("(?i)One year"), "One year")
                .when(col("contractDescription").rlike("(?i)Two year"), "Two year")
                .otherwise("Unknown"))

# Calculate the mean of Dependents (ignoring NULLs)
dependents_mean = df_transformed.select(mean("Dependents")).collect()[0][0]

# Replace NULL values with the mean
df_transformed = df_transformed.fillna({"Dependents": dependents_mean})

# Show the transformed DataFrame
# df_transformed.select("customerDescription", "Gender", "Senior", "Partner", "Dependents").show(truncate=False)

# df_transformed.select("internetDescription", "phoneDescription", "AmountServices", "PhoneServiceCount", "InternetServiceCount").show(truncate=False)

# df_transformed.select("subscriptionDescription", "Churn", "MonthsSubscribed", "MonthlyCost").show(truncate=False)

# df_transformed.select("contractDescription", "ContractType", "PaymentMethod").show(truncate=False)

df_cleaned = df_transformed.select("customerId", "Churn", "Gender", "Senior", "Partner", "Dependents", "AmountServices", "MonthsSubscribed", "MonthlyCost", "TotalCharges", "ContractType", "PaymentMethod")

# df_cleaned.show(truncate=False)

# Write the transformed data to Parquet
df_cleaned.write.parquet('hdfs:///data/cleaned/clean_customerData.parquet')

# Stop Spark session
spark.stop()


