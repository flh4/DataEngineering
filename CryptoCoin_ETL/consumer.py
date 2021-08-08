from pyspark.sql import SparkSession
from kafka import KafkaConsumer

TOPIC = "cointest"

ss = SparkSession \
	.builder \
	.appName("CoinConsumer") \
	.master("local[*]") \
	.getOrCreate()

df = ss \
	.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "localhost:9099") \
	.option("subscribe", TOPIC) \
	.load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

query = df \
	.writeStream \
	.outputMode("update")
	.format("console")
	.start()

query.awaitTermination()







### spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 consumer.py
