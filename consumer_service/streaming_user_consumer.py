# consumer_service/streaming_user_consumer.py

from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.functions import ( # type: ignore
    from_json, col, current_timestamp, regexp_replace, when, lit
)
from pyspark.sql.types import StructType, StructField, StringType # type: ignore
import sys, os, time

# Configs
sys.path.append("/app/configs")
from hudi_config import HUDI_OPTIONS, HUDI_BASE_PATH # type: ignore

KAFKA_TOPIC = "first-topic"
CHECKPOINT_DIR = f"/app/checkpoints/{KAFKA_TOPIC}"
COUNTER_FILE = "/app/batch_count.txt"

def update_cumulative_count(batch_count):
    total = 0
    if os.path.exists(COUNTER_FILE):
        with open(COUNTER_FILE, "r") as f:
            try:
                total = int(f.read().strip())
            except ValueError:
                total = 0
    total += batch_count
    with open(COUNTER_FILE, "w") as f:
        f.write(str(total))
    return total

user_schema = StructType([
    StructField("id", StringType(), True),
    StructField("firstname", StringType(), True),
    StructField("lastname", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zipcode", StringType(), True),
    StructField("country", StringType(), True),
])

spark = SparkSession.builder \
    .appName("KafkaToHudiStreaming") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
    .config("spark.jars", "/opt/extra_jars/hudi-spark3.4-bundle_2.12-0.14.1.jar") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

json_df = kafka_df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), user_schema).alias("data")) \
    .select("data.*") \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("country", when(col("country").isNull(), lit("unknown")).otherwise(col("country"))) \
    .withColumn("state", when(col("state").isNull(), lit("unknown")).otherwise(col("state"))) \
    .withColumn("city", when(col("city").isNull(), lit("unknown")).otherwise(col("city"))) \
    .withColumn("country", regexp_replace("country", "[^a-zA-Z0-9-_]", "_")) \
    .withColumn("state", regexp_replace("state", "[^a-zA-Z0-9-_]", "_")) \
    .withColumn("city", regexp_replace("city", "[^a-zA-Z0-9-_]", "_"))

def write_to_hudi(df, batch_id):
    start = time.time()
    count = df.count()
    if count == 0:
        print(f"[Batch {batch_id}] Empty batch. Skipping write.")
        return

    print(f"[Batch {batch_id}] Processing {count} records...")

    df.write.format("hudi") \
        .options(**HUDI_OPTIONS) \
        .mode("append") \
        .save(HUDI_BASE_PATH)

    duration = time.time() - start
    total = update_cumulative_count(count)
    print(f"{total} total records written | Batch time: {duration:.2f}s")

query = json_df.writeStream \
    .trigger(processingTime="10 seconds") \
    .foreachBatch(write_to_hudi) \
    .outputMode("update") \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .start()

try:
    print(f"Streaming started on topic: {KAFKA_TOPIC}. Press Ctrl+C to stop.")
    query.awaitTermination()
except Exception as e:
    print(f"Error: {e}")
    query.stop()
    spark.stop()
    print("Stream stopped and Spark session closed.")