import requests
from time import sleep

url = "https://coinranking1.p.rapidapi.com/coins"

headers = {
    'x-rapidapi-key': "96ad9f7179msh73f50c9342c17e4p126cf0jsn4eba9eefe58d",
    'x-rapidapi-host': "coinranking1.p.rapidapi.com"
    }

while True:
	response = requests.request("GET", url, headers=headers)
	print(response.text)
	sleep(20)