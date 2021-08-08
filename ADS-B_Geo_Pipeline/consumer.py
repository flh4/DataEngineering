from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.types import StructType, StructField
from json import loads

topic = "adsb"
Z_K = "localhost:2181"
AppName = "Aircraft"

# define schema
Schema = StructType([StructField('icao', StringType(), True),
					StructField('reg', StringType(), True),
					StructField('opicao', StringType(), True),
					StructField('call', StringType(), True),
					StructField('sqk', StringType(), True),
					StructField('alt', StringType(), True),
					StructField('galt', StringType(), True),
					StructField('spd', StringType(), True),
					StructField('vsi', StringType(), True),
					StructField('wtc', StringType(), True),
					StructField('gnd', StringType(), True),
					StructField('mil', StringType(), True),
					StructField('trak', StringType(), True),
					StructField('ttrk', StringType(), True),           
					StructField('dst', StringType(), True),
					StructField('lat', StringType(), True),
					StructField('lon', StringType(), True),
					StructField('postime', StringType(), True)])

def Process(rdd):
	if not rdd.isEmpty():
		global ss
		# create dataframe, replacing empty values with **
		df = ss.createDataFrame(rdd, Schema) \
		.selectExpr("icao as ICAO", "reg as RegNum",
					"opicao as Oprtr", "call as CallNum", 
					"sqk as TrspdNum", "alt as Alt", 
					"galt as AltSea", "spd as GndSpd", 
					"vsi as VertSpd", "wtc as WTC", 
					"gnd as OnGround", "mil as IsMil", 
					"trak as TrkAngle", "ttrk as ApHead",
					"dst as Dist", "lat as LAT", "lon as LON", 
					"postime as Timestmp") \
		.replace("", "**N/A**")
		
		df.write.saveAsTable(name="aircraft",
			format="hive", mode="overwrite")

# create the SparkContext, StreamingContext, and SparkSession
sc = SparkContext("local[*]", AppName)
ssc = StreamingContext(sc, 7)
ss = SparkSession.builder \
	.appName(sc.appName) \
	.config("spark.sql.warehouse.dir",
		"/user/hive/warehouse") \
	.config("hive.metastore.uris",
		"thrift://localhost:9083") \
	.enableHiveSupport() \
	.getOrCreate() 

# create Kafka createStream object	
kafkastream = KafkaUtils.createStream(ssc, Z_K, AppName, {topic: 1})
# get raw json 
raw_json = kafkastream.map(lambda x: loads(x[1]))
# perform mappings to get desired fields
aircraft = raw_json.flatMap(lambda x: x.get("ac")) \
	.map(lambda x: (x.get("icao"), x.get("reg"), x.get("opicao"), 
			x.get("call"), x.get("sqk"), x.get("alt"), x.get("galt"), 
			x.get("spd"), x.get("vsi"), x.get("wtc"), x.get("gnd"), 
			x.get("mil"), x.get("trak"), x.get("ttrk"), x.get("dst"),
			x.get("lat"), x.get("lon"), x.get("postime")))

aircraft.foreachRDD(Process)

ssc.start()
ssc.awaitTermination()
