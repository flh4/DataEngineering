import requests
from time import sleep

url = "https://coinranking1.p.rapidapi.com/coin/1"

headers = {
    'x-rapidapi-key': "96ad9f7179msh73f50c9342c17e4p126cf0jsn4eba9eefe58d",
    'x-rapidapi-host': "coinranking1.p.rapidapi.com"
    }

while True:
	response = requests.request("GET", url, headers=headers)
	print(response.text)
	print()
	print("END")
	print()
	print()
	print()
	sleep(10)