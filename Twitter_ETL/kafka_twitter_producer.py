from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
from keys import Tokens

tokens = []
for key in Keys.keys():
    tokens.append(Tokens[key])

ACCESS_TOKEN = tokens[0]
TOKEN_SECRET = tokens[1]
CONSUMER_KEY = tokens[2]
CONSUMER_SECRET = tokens[3]


TOPIC = "tweets"

class StdOutListener(StreamListener):
	def on_data(self, data):
		producer.send_messages(TOPIC, data.encode('utf-8'))
		print (data)
		return True
	def on_error(self, status):
		print (status)

kafka = KafkaClient("localhost:9099")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, TOKEN_SECRET)
stream = Stream(auth, l)
stream.filter(track=TOPIC)

