import csv
import folium

#Create map object centered at Houston, TX
m = folium.Map(location=[33.7490, -84.3880], zoom_start=10)
tooltip = 'Click for more info'

with open('mycsv.csv', newline='') as f:
    reader = csv.reader(f)
    data = list(reader)
    
data.pop(0)
coord = {}

for i in data:
	coord[i[9]] = (i[7], i[8])

for k, v in coord.items() :
	folium.Marker([v[0], v[1]],
				popup='<strong>'+str(k)+'</strong>',
				tooltip=tooltip,
				icon=folium.Icon(icon='star')).add_to(m)


m.save('map.html')
