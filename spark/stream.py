import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lag, stddev, lit
from pyspark.sql.types import StructType, StructField, DoubleType, TimestampType
from pyspark.sql.window import Window

KAFKA_BROKER = "kafka:9092"
TOPIC = "gold_stream"
JDBC_URL = "jdbc:postgresql://postgres:5432/gold_prediction"
PG_PROPERTIES = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

schema = StructType([
    StructField("timestamp", TimestampType()),
    StructField("gold_price", DoubleType()),
    StructField("oil_price", DoubleType()),
    StructField("dxy", DoubleType()),
])

spark = (
    SparkSession.builder
    .appName("GoldStreamProcessor")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING) as json_str")
)

parsed_df = (
    stream_df
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

window_spec = Window.orderBy("timestamp")

features_df = (
    parsed_df
    .withColumn("lag1", lag("gold_price", 1).over(window_spec))
    .withColumn("lag5", lag("gold_price", 5).over(window_spec))
    .withColumn("return", (col("gold_price") - col("lag1")) / col("lag1"))
    .withColumn("volatility", stddev("return").over(window_spec.rowsBetween(-5, 0)))
)


def write_to_postgres(df, epoch_id):
    if df.count() > 0:
        df.write.jdbc(url=JDBC_URL, table="gold_features", mode="append", properties=PG_PROPERTIES)


query = (
    features_df
    .writeStream
    .foreachBatch(write_to_postgres)
    .outputMode("update")
    .trigger(processingTime="60 seconds")
    .start()
)

query.awaitTermination()
