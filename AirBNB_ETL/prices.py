from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark.sql.functions import col
from argparse import ArgumentParser

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

sc = SparkContext("local[*]", "RentalData")

ssc = StreamingContext(sc, 5)

ss = SparkSession.builder \
	.appName(sc.appName) \
	.config("spark.sql.warehouse.dir",
		"/user/hive/warehouse") \
	.config("hive.metastore.uris",
		"thrift://localhost:9083") \
	.enableHiveSupport() \
	.getOrCreate()

nights = args()

hive_context = HiveContext(sc)

df = hive_context.table("default.rentals")

new = df.select("id", "property_type" "night_price") \
	.withColumn("total", nights*col("night_price")) \
	.drop(col("night_price"))

new.show()


ssc.start()
ssc.awaitTermination()


##spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 prices.py