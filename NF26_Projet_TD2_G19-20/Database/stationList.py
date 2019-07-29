import csv

def getStationsFromCsv(filename):
    station = [] 
    longitude = []
    latitude = []
    with open(filename) as f:
        for r in csv.DictReader(f):
            if r["station"] not in station:
                station.append(r["station"])
                longitude.append(r["lon"])
                latitude.append(r["lat"])
    return station, longitude, latitude

def createStationsList(station, longitude, latitude):
    with open('stationList.csv', 'w+') as csvfile:
        filewriter = csv.writer(csvfile)
        filewriter.writerow(['Station', 'Longitude', 'Latitude'])
        for i in range(0, len(station)):
            filewriter.writerow([station[i], longitude[i], latitude[i]])

        
#station, lon, lat = getStationsFromCsv('asos.csv')
#createStationsList(station, lon, lat)