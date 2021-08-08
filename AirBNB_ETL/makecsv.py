from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pandas import *


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

hive_context = HiveContext(sc)
df = hive_context.table("default.rentals")

df.toPandas().to_csv('mycsv.csv')

print(rentals)

ssc.start()
ssc.awaitTermination()


##spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 make_map.py