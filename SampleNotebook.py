# Databricks notebook source
# Sample data
data = [("Raj", 24), ("priya", 30), ("supra", 29)]

# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Show data
df.show()


# COMMAND ----------

# Load the CSV into a DataFrame
df = spark.read.option("header", True).csv("/FileStore/tables/dirty_employees.csv")

# Show the raw dirty data
df.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import col, trim, lower, regexp_replace

# Step 1: Load CSV into DataFrame
df = spark.read.option("header", True).csv("/FileStore/tables/dirty_employees.csv")

# Step 2: Show raw dirty data
print("Raw Data:")
df.show(truncate=False)

# Step 3: Drop duplicate rows
df = df.dropDuplicates()

# Step 4: Trim whitespaces from all string columns
for column in df.columns:
    df = df.withColumn(column, trim(col(column)))

# Step 5: Handle missing/null values (replace with default or drop)
df = df.na.fill({
    "age": "0",
    "salary": "0",
    "department": "Unknown"
})

# Step 6: Standardize case (e.g. lowercase names or departments)
df = df.withColumn("name", lower(col("name")))
df = df.withColumn("department", lower(col("department")))

# Step 7: Remove unwanted characters (e.g. ₹, $, commas)
df = df.withColumn("salary", regexp_replace(col("salary"), "[^0-9.]", ""))

# Step 8: Convert columns to appropriate types (if needed)
from pyspark.sql.types import IntegerType, DoubleType

df = df.withColumn("age", col("age").cast(IntegerType()))
df = df.withColumn("salary", col("salary").cast(DoubleType()))

# Final cleaned data
print("Cleaned Data:")
df.show(truncate=False)

# Optional: Print schema
df.printSchema()
