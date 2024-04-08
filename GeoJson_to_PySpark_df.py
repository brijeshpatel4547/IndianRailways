from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("GeoJSON FeatureCollection Loading") \
    .getOrCreate()

# Define the schema based on the GeoJSON properties
feature_schema = StructType([
    StructField("type", StringType(), True),
    StructField("properties", StringType(), True),
    StructField("geometry", StringType(), True)
])

collection_schema = StructType([
    StructField("type", StringType(), True),
    StructField("features", ArrayType(feature_schema), True)
])

# Load the raw GeoJSON data
raw_df = spark.read.text("path_to_your_file.geojson")

# Parse the JSON data
json_df = raw_df.select(from_json(col("value"), collection_schema).alias("data"))

# Flatten the DataFrame
flattened_df = json_df.select(
    col("data.type"),
    explode(col("data.features")).alias("features")
)

# Extract the properties of each feature
final_df = flattened_df.select(
    col("type"),
    col("features.type").alias("feature_type"),
    col("features.properties").alias("properties"),
    col("features.geometry").alias("geometry")
)

# Show the DataFrame
final_df.show()