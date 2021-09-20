from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession \
    .builder \
    .appName("cryptoData") \
    .getOrCreate()


schema = StructType().add("pair", StringType()).add("value", StringType())

incoming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","crypto") \
    .option("startingOffsets", "latest") \
    .load()

raw_df = incoming_df.selectExpr("CAST(value AS STRING)")

s_df = raw_df.select(from_json(raw_df.value, schema).alias("data"))

btc_df = s_df.filter(col("data.pair") == 'btc-usdt')

output_df = btc_df.select(to_json(struct(col("data.*"))).alias("value"))

query = output_df.writeStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("checkpointLocation", "/tmp/spark_checkpoint").option("topic", "output").outputMode("update").start()

query.awaitTermination()