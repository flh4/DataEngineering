from kafka import SimpleProducer, KafkaClient
import requests

def makeQuery(State, City):
	with open("key.txt") as f:
		api_key = f.read()
	url = "https://mashvisor-api.p.rapidapi.com/airbnb-property/active-listings"
	querystring = {"state":State,"page":"1","city":City,"items":"100"}

	headers = {
    	'x-rapidapi-key': api_key,
    	'x-rapidapi-host': "mashvisor-api.p.rapidapi.com"
    }

	response = requests.request("GET", url, headers=headers, params=querystring)
	return response.text



TOPIC = "rentals"
resp = makeQuery("GA", "Atlanta")

kafka = KafkaClient("localhost:9099")
producer = SimpleProducer(kafka)

producer.send_messages(TOPIC, resp.encode('utf-8'))






