import csv
import re
import cassandra.cluster
import datetime

def convertIfNotNull(var, varType):
    
    if (var == "   " or var == "null"):
        return 'NULL'
    
    if varType == "float":
        return float(var)
    if varType == "string" or varType == "date":
        return "'" + var + "'"
    else:
        return var
    
def loadcsv(filename):
    dateparser = re.compile(
        "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
            match_date = dateparser.match(r["valid"])
            if not match_date:
                continue
            date = match_date.groupdict()
            
            data = {}
            data["station"] = "'" + r["station"] + "'"
            data["valid"] = (
                int(date["year"]),
                int(date["month"]),
                int(date["day"]),
                int(date["hour"]),
                int(date["minute"])
            )
            data["lon"] = float(r["lon"])
            data["lat"] = float(r["lat"])
            
            data["tmpf"] = convertIfNotNull(r["tmpf"],"float")
            data["dwpf"] = convertIfNotNull(r["dwpf"],"float")
            data["relh"] = convertIfNotNull(r["relh"],"float")
            data["drct"] = convertIfNotNull(r["drct"],"float")
            data["sknt"] = convertIfNotNull(r["sknt"],"float")
            data["p01i"] = convertIfNotNull(r["p01i"],"float")
            data["alti"] = convertIfNotNull(r["alti"],"float")
            data["mslp"] = convertIfNotNull(r["mslp"],"float")
            data["vsby"] = convertIfNotNull(r["vsby"],"float")
            data["gust"] = convertIfNotNull(r["gust"],"float")
            data["skyc1"] = convertIfNotNull(r["skyc1"],"string")
            data["skyc2"] = convertIfNotNull(r["skyc2"],"string")
            data["skyc3"] = convertIfNotNull(r["skyc3"],"string")
            data["skyc4"] = convertIfNotNull(r["skyc4"],"string")
            data["skyl1"] = convertIfNotNull(r["skyl1"],"float")
            data["skyl2"] = convertIfNotNull(r["skyl2"],"float")
            data["skyl3"] = convertIfNotNull(r["skyl3"],"float")
            data["skyl4"] = convertIfNotNull(r["skyl4"],"float")
            data["wxcodes"] = convertIfNotNull(r["wxcodes"],"string")
            data["ice_accretion_1hr"] = convertIfNotNull(r["ice_accretion_1hr"],"float")
            data["ice_accretion_3hr"] = convertIfNotNull(r["ice_accretion_3hr"],"float")
            data["ice_accretion_6hr"] = convertIfNotNull(r["ice_accretion_6hr"],"float")
            data["peak_wind_gust"] = convertIfNotNull(r["peak_wind_gust"],"float")
            data["peak_wind_drct"] = convertIfNotNull(r["peak_wind_drct"],"float")
            data["peak_wind_time"] = convertIfNotNull(r["peak_wind_time"], "date")
            data["feel"] = convertIfNotNull(r["feel"],"float")
            data["observation"] = convertIfNotNull(r["metar"],"string")
            yield data
            
def writeStation(dataToInsert):
    cluster = cassandra.cluster.Cluster(['localhost'])
    session = cluster.connect('adamloic_lixiang_projet')
    
    i = 0
    for data in dataToInsert:
        insertionQuery = f"""
            INSERT INTO AsosItalyStation (station, year, month, day, hour, 
                                          minute, longitude, latitude, temperatureF, 
                                          dewPoIntF, relativeHumidity, windDirection, 
                                          windSpeed, precipitation, pressure, seaLevelPressure, 
                                          visibility, windGust, skyLevel1coverage, skyLevel2coverage, 
                                          skyLevel3coverage, skyLevel4coverage, skyLevel1altitude, 
                                          skyLevel2altitude, skyLevel3altitude, skyLevel4altitude, 
                                          weatherCode, iceAccretion1, iceAccretion3, iceAccretion6, 
                                          peakWindGust, peakWindDirection, peakWindTime, temperature, 
                                          observation) 
            VALUES(
                {data["station"]},
                {data["valid"][0]},
                {data["valid"][1]},
                {data["valid"][2]},
                {data["valid"][3]},
                {data["valid"][4]},
                {data["lon"]},
                {data["lat"]},
                {data["tmpf"]},
                {data["dwpf"]},
                {data["relh"]},
                {data["drct"]},
                {data["sknt"]},
                {data["p01i"]},
                {data["alti"]},
                {data["mslp"]},
                {data["vsby"]},
                {data["gust"]},
                {data["skyc1"]},
                {data["skyc2"]},
                {data["skyc3"]},
                {data["skyc4"]},
                {data["skyl1"]},
                {data["skyl2"]},
                {data["skyl3"]},
                {data["skyl4"]},
                {data["wxcodes"]},
                {data["ice_accretion_1hr"]},
                {data["ice_accretion_3hr"]},
                {data["ice_accretion_6hr"]},
                {data["peak_wind_gust"]},
                {data["peak_wind_drct"]},
                {data["peak_wind_time"]},
                {data["feel"]},
                {data["observation"]}
            )
        """
        session.execute(insertionQuery)
        i = i+1
        print(i)
		
def writeTime(dataToInsert):
    cluster = cassandra.cluster.Cluster(['localhost'])
    session = cluster.connect('adamloic_lixiang_projet')
    i = 0
    for data in dataToInsert:
        
        dateToInsert = datetime.datetime(data["valid"][0], data["valid"][1], data["valid"][2], 
                        data["valid"][3], data["valid"][4]).strftime('%Y-%m-%d %H:%M')
        
        insertionQuery = f"""
            INSERT INTO AsosItalyTime (year, month, day, hour, minute, longitude, latitude, validDate, station, temperatureF, 
                                          dewPoINTF, relativeHumidity, windDirection, 
                                          windSpeed, precipitation, pressure, seaLevelPressure, 
                                          visibility, windGust, skyLevel1coverage, skyLevel2coverage, 
                                          skyLevel3coverage, skyLevel4coverage, skyLevel1altitude, 
                                          skyLevel2altitude, skyLevel3altitude, skyLevel4altitude, 
                                          weatherCode, iceAccretion1, iceAccretion3, iceAccretion6, 
                                          peakWindGust, peakWindDirection, peakWindTime, temperature, 
                                          observation) 
            VALUES(
                {data["valid"][0]},
                {data["valid"][1]},
                {data["valid"][2]},
                {data["valid"][3]},
                {data["valid"][4]},
                {data["lon"]},
                {data["lat"]},
                '{dateToInsert}',
                {data["station"]},
                {data["tmpf"]},
                {data["dwpf"]},
                {data["relh"]},
                {data["drct"]},
                {data["sknt"]},
                {data["p01i"]},
                {data["alti"]},
                {data["mslp"]},
                {data["vsby"]},
                {data["gust"]},
                {data["skyc1"]},
                {data["skyc2"]},
                {data["skyc3"]},
                {data["skyc4"]},
                {data["skyl1"]},
                {data["skyl2"]},
                {data["skyl3"]},
                {data["skyl4"]},
                {data["wxcodes"]},
                {data["ice_accretion_1hr"]},
                {data["ice_accretion_3hr"]},
                {data["ice_accretion_6hr"]},
                {data["peak_wind_gust"]},
                {data["peak_wind_drct"]},
                {data["peak_wind_time"]},
                {data["feel"]},
                {data["observation"]}
            )
        """
        session.execute(insertionQuery)
        i = i+1
        print(i)
        
#data = loadcsv('asos.csv')
#writeStation(data)
#writeStation(data)