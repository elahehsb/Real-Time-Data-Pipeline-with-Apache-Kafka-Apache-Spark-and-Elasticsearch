from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, LongType
from elasticsearch import Elasticsearch, helpers

# Initialize Spark session
spark = SparkSession.builder \
    .appName("RealTimeDataProcessing") \
    .getOrCreate()

# Define schema for Kafka messages
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("timestamp", LongType())
])

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .load()

# Parse JSON data
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Perform some basic transformations
processed_df = parsed_df.withColumn("temperature_fahrenheit", col("temperature") * 9/5 + 32)

# Function to save to Elasticsearch
def save_to_elasticsearch(df, epoch_id):
    es = Elasticsearch(['http://localhost:9200'])
    actions = [
        {
            "_index": "sensor_data",
            "_source": {
                "sensor_id": row.sensor_id,
                "temperature": row.temperature,
                "temperature_fahrenheit": row.temperature_fahrenheit,
                "timestamp": row.timestamp
            }
        }
        for row in df.collect()
    ]
    helpers.bulk(es, actions)

# Write to Elasticsearch
query = processed_df.writeStream \
    .foreachBatch(save_to_elasticsearch) \
    .outputMode("update") \
    .start()

query.awaitTermination()
