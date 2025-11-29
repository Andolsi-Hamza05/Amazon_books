from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Mens Jeans ETL") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read.csv("/home/hamza/data/bronze/mens_jeans_bronze.csv", header=True, inferSchema=True)
df.show(10, truncate=False)

# Save to silver — appears on your Windows machine instantly
df.write.mode("overwrite").parquet("/home/hamza/data/silver/mens_jeans")
print("Saved to ../data/silver/mens_jeans")