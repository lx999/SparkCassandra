import findspark
findspark.init('/opt/spark')
from pyspark import SparkContext, SparkConf
import numpy as np
import math
conf = SparkConf().setAppName('PySparkShell').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf=conf)
import cassandra.cluster
import itertools

def get_session():
    cluster = cassandra.cluster.Cluster(['localhost'])
    session = cluster.connect()
    session.set_keyspace('adamloic_lixiang_projet')
    return session

def notNull(p):
    flag = True
    for i in p:
        if i == None:
            flag = False
            break
    if flag:
        return p

def coordToList(l):
    res = [[],[]]
    for i in l:
        res[0].append(i[0])
        res[1].append(i[1])
    return res

### Avg
def avgYear(tmp, station, firstYear, lastYear):
    listGenerator= []
    session = get_session()
    for year in range(int(firstYear), int(lastYear +1)):
        sql = f"""
            SELECT {TMP},year
            FROM asositalystation 
            WHERE station = '{station}' 
            AND year = {year}
            """
        listGenerator.append(session.execute(sql))
    t = itertools.chain(*listGenerator)
    D = sc.parallelize(t)
    moy = D.filter(notNull).map(lambda data:(data[1],np.array([1,data[0]]))).reduceByKey(lambda a,b: a+b).map(lambda truc: (truc[0], truc[1][1]/truc[1][0])).collect()
    session.shutdown()
    return coordToList(moy)

def avgSeason(tmp,station,year):
    session = get_session()
    sql = f"""
        SELECT {tmp}, year, month 
        FROM asositalystation 
        WHERE station = '{station}' 
        AND year = {year}
        """
    t = session.execute(sql)
    D = sc.parallelize(t)
    moy = D.filter(notNull).map(lambda data:(math.ceil(data[2]/3),np.array([1,data[0]]))).reduceByKey(lambda a,b: a+b).map(lambda truc: (truc[0], truc[1][1]/truc[1][0])).collect()
    moy = sorted(moy)
    session.shutdown()
    return coordToList(moy)

def avgMonth(tmp,station, year):
    session = get_session()
    sql = f"""
        SELECT {tmp}, year, month
        FROM asositalystation 
        WHERE station = '{station}'
        AND year = {year}
        """
    t = session.execute(sql)
    D = sc.parallelize(t)
    moy = D.filter(notNull).map(lambda data:(data[2],np.array([1,data[0]]))).reduceByKey(lambda a,b: a+b).map(lambda truc: (truc[0], truc[1][1]/truc[1][0])).collect()
    moy = sorted(moy)
    session.shutdown()
    return coordToList(moy)

def avgMonthYear(tmp,station,firstYear, lastYear):
    listGenerator= []
    session = get_session()
    for year in range(int(firstYear), int(lastYear +1)):
        sql = f"""
            SELECT {tmp}, year, month
            FROM asositalystation 
            WHERE station = '{station}'
            AND year = {year}
            """
        listGenerator.append(session.execute(sql))
    t = itertools.chain(*listGenerator)
    D = sc.parallelize(t)
    moy = D.filter(notNull).map(lambda data:((data[1],data[2]),np.array([1,data[0]]))).reduceByKey(lambda a,b: a+b).map(lambda truc: (truc[0], truc[1][1]/truc[1][0]))
    moy = moy.sortByKey()
    moy = moy.map(lambda data:(data[0][0],data[1])).groupByKey().collect()
    session.shutdown()
    return moy

### Min Max
def minmaxMonth(minmax,tmp,station, year):
    session = get_session()
    sql = f"""
            SELECT {tmp}, year, month
            FROM asositalystation 
            WHERE station = '{station}'
            AND year = {year}
            """
    t = session.execute(sql)
    D = sc.parallelize(t)
    if minmax=='min':
        extreme = D.filter(notNull).map(lambda data: (data[2],data[0])).reduceByKey(lambda a,b: min(a,b)).collect()
    elif minmax=='max':
        extreme = D.filter(notNull).map(lambda data: (data[2],data[0])).reduceByKey(lambda a,b: max(a,b)).collect()
    extreme = sorted(extreme)
    session.shutdown()
    return coordToList(extreme)


def minmaxSeason(minmax,tmp,station, year):
    session = get_session()
    sql = f"""
            SELECT {tmp}, year, month
            FROM asositalystation 
            WHERE station = '{station}'
            AND year = {year}
            """
    t = session.execute(sql)
    D = sc.parallelize(t)
    if minmax=='min':
        extreme = D.filter(notNull).map(lambda data: (math.ceil(data[2]/3),data[0])).reduceByKey(lambda a,b: min(a,b)).collect()
    elif minmax=='max':
        extreme = D.filter(notNull).map(lambda data: (math.ceil(data[2]/3),data[0])).reduceByKey(lambda a,b: max(a,b)).collect()
    extreme = sorted(extreme)
    session.shutdown()
    return coordToList(extreme)


###median 
def mymap(data):
    tmp, year, month = data
    return (math.ceil(month/3),(1,tmp,tmp,tmp))

def rdc(x,y):
    (countx,tmpx,tmpx1,tmpx2) = x
    (county,tmpy,tmpy1,tmpy2) = y
    return(countx+county,tmpx+tmpy,min(tmpx1,tmpy1),max(tmpx2,tmpy2))

def mean_variance_map(x):
    (saison,(nb,sumTemp,m,M)) = x
    return(saison,(sumTemp/nb,m,M))

def median_moyenne(x):
    (saison,(mean,min,max)) = x
    return (saison,((min+max)/2, min, max))

def func_median(x):
    (saison,(tmp,(mean,min,max)))=x
    return (saison,(int(tmp<mean),int(tmp>=mean)))

def reduce_median_sum(x,y):
    (a0,b0) = x
    (a1,b1) = y
    return (a0+a1,b0+b1)

def reduce_median_update(x,res_array):
    (saison,(normal,(mean,left,right))) = x
    (nb_left,nb_right) = res_array[saison] 
   
    if abs(nb_left-nb_right) <= 1:
        return (saison,(normal,(mean,left,right)))
    elif nb_left<nb_right:
        left = mean
    else:
        right = mean
    mean = (float(left)+float(right))/2
    return (saison,(normal,(mean,left,right)))

def medianSeason(tmp, station, year):
    session = get_session()
    #LIBT LICU LIRI
    sql = f"""
            SELECT {tmp}, year, month
            FROM asositalystation 
            WHERE station = '{station}'
            AND year = {year}
            """
    t = session.execute(sql)
    D = sc.parallelize(t)
    #moy de min/max, min, max
    median_initalize = D.filter(notNull).map(mymap).reduceByKey(rdc).map(mean_variance_map).map(median_moyenne)
    data = D.filter(notNull).map(lambda data: (math.ceil(data[2]/3),(data[0])))
    #data, moy de min/max, min, max
    result = data.join(median_initalize)
    res_array = None
    for iter in range(10):
        res_array  = result.map(func_median).reduceByKey(reduce_median_sum).collectAsMap()
        result = result.map(lambda row:reduce_median_update(row,res_array)).sortByKey()
    res = [[],[]]
    for i in result.collectAsMap():
        res[0].append(i)
        res[1].append(result.collectAsMap()[i][1][0])
    return res

#boxPlot
def boxSaison(tmp, station, year):
    session = get_session()
    #LIBT LICU LIRI
    sql = f"""
            SELECT {tmp}, year, month
            FROM asositalystation 
            WHERE station = '{station}'
            AND year = {year}
            """
    t = session.execute(sql)
    D = sc.parallelize(t)
    box = D.filter(notNull).map(lambda data: (math.ceil(data[2]/3),data[0])).groupByKey().map(lambda x : (x[0], list(x[1])))
    return box.collect()

#change season
def seasonToSeason(data):
    data[0][0] = 'printemps'
    data[0][1] = 'été'
    data[0][2] = 'automne'
    data[0][3] = 'hiver'
    return data

#Find closest station.
import csv
def findClosestStation(longitude, latitude):
    
    distanceMin = 1000000
    stationMin = "Erreur"
    with open('stationList.csv') as f:
        for row in csv.DictReader(f):
            x = (longitude - float(row["Longitude"])) * math.cos(
                    (latitude + float(row["Latitude"]))/2)
            y = latitude - float(row["Latitude"])
            z = math.sqrt(x**2 + y**2)
            distance = 1.852 * 60 * z
            if distance < distanceMin:
                distanceMin = distance
                stationMin = row["Station"]
    return stationMin

###draw
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import os

def drawStation(latitude,longitude,year):
    
    station = findClosestStation(longitude, latitude)
    print("La station la plus proche est : " + station + ".")
    #tmpf
    #box
    
    if not os.path.exists("./images"):
        os.makedirs("./images")
    
    t = boxSaison('temperatureF',station,year)
    plt.boxplot([t[0][1],t[1][1],t[2][1],t[3][1]])
    plt.xlabel('Saison')
    plt.ylabel('Temperature (F)')
    plt.title('Temperature par saison de '+station+' de '+str(year))
    plt.savefig("./images/boxTmpSeason.png")
    plt.close() 
    #month tmpf
    moy = avgMonth('temperatureF',station, year)
    mini = minmaxMonth('min','temperatureF',station, year)
    maxi = minmaxMonth('max','temperatureF',station, year)
    plt.plot(moy[0],moy[1], label='moyenne')
    plt.plot(mini[0],mini[1], label='minimum')
    plt.plot(maxi[0],maxi[1], label='maximum')
    plt.xlabel('Mois')
    plt.ylabel('Temperature (F)')
    plt.legend()
    plt.title('Temperature par mois de '+station+' de '+str(year))
    plt.savefig("./images/TmpMonth.png")
    plt.close() 
    #season tmpf
    moy = avgSeason('temperatureF',station, year)
    moy = seasonToSeason(moy)
    mini = minmaxSeason('min','temperatureF',station, year)
    mini = seasonToSeason(mini)
    maxi = minmaxSeason('max','temperatureF',station, year)
    maxi = seasonToSeason(maxi)
    median = medianSeason('temperatureF',station, year)
    median = seasonToSeason(median)
    plt.plot(moy[0],moy[1], label='moyenne')
    plt.plot(mini[0],mini[1], label='minimum')
    plt.plot(maxi[0],maxi[1], label='maximum')
    plt.plot(median[0],median[1], label='median')
    plt.xlabel('Saison')
    plt.ylabel('Temperature (F)')
    plt.legend()
    plt.title('Temperature par saison de '+station+' de '+str(year))
    plt.savefig("./images/TmpSeason.png")
    plt.close()   
    #historique
    t = coordToList(avgMonthYear('temperatureF',station,2005,2014))
    for i in range(len(t[0])):
        z = []
        for c in t[1][i]:
            z.append(c)
        plt.plot([ii for ii in range(1,13)],z, label=t[0][i])
    plt.xlabel('Mois')
    plt.ylabel('Temperature (F)')
    plt.title('Historique de temperature par mois de '+station)
    plt.legend()
    plt.savefig("./images/HistoriqueTmp.png")
    plt.close()

#drawStation(12.3, 42.24, 2011)