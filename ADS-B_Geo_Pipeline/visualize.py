from pyhive import hive
import pandas as pd
from tabulate import tabulate
import folium
import subprocess

# open connection
conn = hive.Connection(host='localhost', port= 10000)

# query the table to a new dataframe
dataframe = pd.read_sql("SELECT ICAO, \
	RegNum, Oprtr, CallNum, TrspdNum\
	 Alt, AltSea, GndSpd, VertSpd, \
	 WTC, OnGround, IsMil, TrkAngle, ApHead, \
	 Dist, LAT, LON, Timestmp FROM default.aircraft", conn)

# display the data 
print(dataframe)
print(tabulate(dataframe.head(20), headers = 'keys', tablefmt = 'psql'))

# extract needed data
coords = list(zip(dataframe["lat"], dataframe["lon"]))
info = list(zip(dataframe["oprtr"], dataframe["alt"], 
		dataframe["gndspd"], dataframe["wtc"], 
		dataframe["vertspd"], dataframe['ismil'], 
		dataframe["icao"])
		)
data = list(zip(info, coords))

# create map centered at defined location
m = folium.Map(location=[coords[0][0], coords[0][1]], 
	tiles='Stamen Terrain', zoom_start=7)
tooltip = 'Click for more info.'
m.add_child(folium.LatLngPopup())

# loop through data
i = 0
while i <= len(data) - 1:
# Color code based on wake turbulence category
	if data[i][0][3] == '0':
		color = 'gray'
	elif data[i][0][3] == '1':
		color = 'blue'
	elif data[i][0][3] == '2':
		color = 'darkblue'
	else:
		color = 'darkpurple'

# change icon based on whether the aircraft is civ or military
	if data[i][0][5] == '1':
		icn = 'fighter-jet'
	else:
		icn = 'plane'
# plot all the locations
	folium.Marker([data[i][1][0], data[i][1][1]],
				popup='<strong>'+"Op: "+data[i][0][0]+
				'\n'+"ICAO: "+data[i][0][6]+
				'\n'+"Alt: "+data[i][0][1]+ " ft."+
				'\n'+"Spd: "+data[i][0][2]+" kn."+
				'\n'+"VrtSpd: "+data[i][0][4]+ " ft/min"'</strong>',
				tooltip=tooltip,
				icon=folium.Icon(icon=icn, color=color, prefix='fa')).add_to(m)
	i+=1

m.save("map.html")

try:
	subprocess.call("firefox map.html", shell=True)
except:
	print("error")
