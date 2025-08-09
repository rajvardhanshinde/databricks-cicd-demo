# Sample data
data = [("Rajvardhan", 24), ("priya", 30), ("Sara", 29)]

# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Show data
df.show()
