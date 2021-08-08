from kafka import KafkaProducer
from requests import request 
from time import sleep 
import json

def makeUrls(ids):
	ids = ids
	urls = []
	for coin in ids:
		urls.append("https://coinranking1.p.rapidapi.com/coin/"+str(ids[coin]))
	return urls

def getApi_Key():
	with open("keys.txt") as f:
		return f.readline().rstrip()

def fetchCoins():
	with open ("coins.json") as json_f:
		return json.load(json_f)

#TOPIC = "cointest"
TOPIC = "coin"
#Sandbox
Broker0 = "sandbox-hdp.hortonworks.com:6667"
#Local
Broker1 = "localhost:9099"
#Multiple Brokers
BROKERS = ["localhost:9092","localhost:9093", "localhost:9094"]
RUNNING = True
producer = KafkaProducer(bootstrap_servers = Broker1)
identifiers = fetchCoins()
urls = makeUrls(identifiers)
api_key = getApi_Key()

headers = {
    'x-rapidapi-key': api_key,
    'x-rapidapi-host': "coinranking1.p.rapidapi.com"
    }

while RUNNING:
	for url in urls:
		sleep(5)
		resp = request("GET", url, headers=headers).text
		#print(resp)
		producer.send(TOPIC, resp.encode('utf-8'))
		print("Message Sent.")
