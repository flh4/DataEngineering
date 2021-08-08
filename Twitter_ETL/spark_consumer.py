from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json

def Process(rdd):
	if not rdd.isEmpty():
		global ss 
		#lines = filtered.foreachRDD(lambda rdd: rdd.toDF())
		df = ss.createDataFrame(rdd , schema=["id", "text"])
		df.show()
		df.write.saveAsTable(name="tweets",
			format="hive", mode="append")

sc = SparkContext("local[*]", "TwitterData")
ssc = StreamingContext(sc, 10)

ss = SparkSession.builder \
	.appName(sc.appName) \
	.config("spark.sql.warehouse.dir",
		"/user/hive/warehouse") \
	.config("hive.metastore.uris",
		"thrift://localhost:9083") \
	.enableHiveSupport() \
	.getOrCreate()

kafkastream = KafkaUtils.createStream(ssc, "localhost:2181", 
	"tweets", {"tweets": 1})

parsed = kafkastream.map(lambda x: json.loads(x[1]))
#parsed.pprint()

filtered = parsed.filter(lambda x: x.get("lang") == "en") \
	.map(lambda x: (x.get("id"), x.get("text")))



parsed.count().map(lambda x:'# OF TWEETS IN BATCH: %s' % x).pprint()


filtered.foreachRDD(Process)



ssc.start()
ssc.awaitTermination()



### spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark_twitter_consumer.py
