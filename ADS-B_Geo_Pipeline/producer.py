from kafka import KafkaProducer
from time import sleep
from requests import request 
from argparse import ArgumentParser
from geopy.geocoders import Nominatim
from time import sleep

def getApi_Key():
	with open("key.txt") as f:
		return f.readline().rstrip()

# get the user defined cl arguments
def get_args():
	parser = ArgumentParser()
	parser.add_argument(
		"--lat", 
		choices = range(-90,91),
		default = Atl_coord[0],
		type = int,
		metavar="[-90-90]",
		help = "Include a specific latitude")

	parser.add_argument(
		"--lon",
		choices = range(-180,181),
		default = Atl_coord[1],
		type = int,
		metavar="[-180-180]",
		help = "Include a specific longitude")

	parser.add_argument(
		"--dist",
		choices = [1,5,10,25,100,250],
		default = 250,
		type = int,
		help = "Include the radial distance")
	parser.add_argument(
		"--loc",
		default = 1,
		help = "Include a location i.e. name of a City i.e.: Portugal.")

	args = parser.parse_args()

	return str(args.lat), str(args.lon), str(args.dist), args.loc

#Kafka Topic
TOPIC = "adsb"
#Broker for Sandbox
#Broker0 = "sandbox-hdp.hortonworks.com:6667"
#Single Broker
#Broker1 = "localhost:9099"

#Multiple Brokers
BROKERS = ["localhost:9092","localhost:9093", "localhost:9094"]
RUNNING = True
api_key = getApi_Key()

# LAT x LON of Atlanta (DEFAULT)
geolocator = Nominatim(user_agent="Aircraft")
Atl = geolocator.geocode("Atlanta, Georgia")
Atl_coord = [Atl.latitude, Atl.longitude]

# Get user args
lat, lon, distance, loc = get_args()

# Set the lat and lon based on user defined inputs
if loc != 1:
	location = geolocator.geocode(loc)
	lat, lon = str(location.latitude), str(location.longitude)
else:
	latlon = lat+', '+lon
	location = geolocator.reverse(latlon).address

print("Getting ADSB data within a (("+distance+" Mile)) Radius of :: "+str(location))

# create Kafka Producer Object
producer = KafkaProducer(bootstrap_servers = BROKERS)
# Api url and headers
url = "https://adsbexchange-com1.p.rapidapi.com/json/lat/"+lat+"/lon/"+lon+"/dist/"+distance+"/"

headers = {
    'x-rapidapi-key': api_key,
    'x-rapidapi-host': "adsbexchange-com1.p.rapidapi.com"
    }

# start making the calls
while RUNNING:
	sleep(5)
	resp = request("GET", url, headers=headers).text
	#print(resp)
	producer.send(TOPIC, resp.encode('utf-8'))
	
	print()
	print("Message sent to Kafka.")
	sleep(25)

