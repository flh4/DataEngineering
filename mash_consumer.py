from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from argparse import ArgumentParser
import json

def args():
	parser = ArgumentParser()
	parser.add_argument("--nights", 
		help="Include the number of nights you wish to stay.")
	args = parser.parse_args()
	if args.nights:
		nights = args.nights
	else:
		nights = 1
	return nights

def Process(rdd):
	if not rdd.isEmpty():
		global ss 
		df = ss.createDataFrame(rdd, schema=["id", "property_type",
			"airbnb_neighborhood", "zip", "night_price", "capacity_of_people", 
			"lat", "lon", "name"]).show()

		df.write.saveAsTable(name="rentals",
			format="hive", mode="append")

sc = SparkContext("local[*]", "RentalData")
ssc = StreamingContext(sc, 7)

ss = SparkSession.builder \
	.appName(sc.appName) \
	.config("spark.sql.warehouse.dir",
		"/user/hive/warehouse") \
	.config("hive.metastore.uris",
		"thrift://localhost:9083") \
	.enableHiveSupport() \
	.getOrCreate()

kafkastream = KafkaUtils.createStream(ssc, "localhost:2181", 
	"rentals", {"rentals": 1})

parsed = kafkastream.map(lambda x: json.loads(x[1]))

content = parsed.map(lambda x: x.get("content")) \
	.flatMap(lambda x: x.get("properties")) \
	.map(lambda x: (x.get("id"), x.get("property_type"), 
	x.get("airbnb_neighborhood"), x.get("zip"), x.get("night_price"), 
	x.get("capacity_of_people"), x.get("lat"), x.get("lon"), x.get("name")))

filtered.foreachRDD(Process)


## Create a new DF based on user defined input of number of desired nights to stay.

nights = args()

hive_context = HiveContext(sc)
df = hive_context.table("default.rentals")

new = df.select("id", "property_type" "night_price") \
	.withColumn("total_price", nights*col("night_price")) \
	.drop(col("night_price")) \
	.show()

ssc.start()
ssc.awaitTermination()



### spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 mash_consumer.py