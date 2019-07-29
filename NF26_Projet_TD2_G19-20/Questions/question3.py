# -*- coding: utf-8 -*-
import folium
import cassandra.cluster
import random
import itertools
import findspark
findspark.init('/opt/spark')
from pyspark import SparkContext, SparkConf
import numpy as np
conf = SparkConf().setAppName('PySparkShell').setMaster('local[*]')
sc = SparkContext.getOrCreate(conf=conf)        

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

def myMap(data):
    lon,lat,ind1,ind2,ind3,ind4,ind5 = data
    return ((lon,lat),np.array([1,ind1,ind1,ind1,ind1**2,ind2,ind2,ind2,ind2**2,ind3,ind3,ind3,ind3**2,ind4,ind4,ind4,ind4**2,ind5,ind5,ind5,ind5**2]))

def myReduce(a,b):
    (count, tmpf11,tmpf12,tmpf13,tmpfSq,tmp11,tmp12,tmp13,tmpSq,dwp11,dwp12,dwp13,dwpSq,ind41,ind42,ind43,ind4Sq,ind51,ind52,ind53,ind5Sq)=a
    (countb, tmpf11b,tmpf12b,tmpf13b,tmpfSqb,tmp11b,tmp12b,tmp13b,tmpSqb,dwp11b,dwp12b,dwp13b,dwpSqb,ind41b,ind42b,ind43b,ind4Sqb,ind51b,ind52b,ind53b,ind5Sqb)=b
    return (count+countb,min(tmpf11,tmpf11b),max(tmpf12,tmpf12b),tmpf13+tmpf13b,tmpfSq+tmpfSqb,min(tmp11,tmp11b),max(tmp12,tmp12b),tmp13+tmp13b,tmpSq+tmpSqb,min(dwp11,dwp11b),max(dwp12,dwp12b),dwp13+dwp13b,dwpSq+dwpSqb,min(ind41,ind41b),max(ind42,ind42b),ind43+ind43b,ind4Sq+ind4Sqb,min(ind51,ind51b),max(ind52,ind52b),ind53+ind53b,ind5Sq+ind5Sqb)

def calcul(truc):
    ((longitude, latitude),(count, tmpfMin,tmpfMax,tmpfMoy,tmpfSq,tmpMin,tmpMax,tmpMoy,tmpSq,dwpMin,dwpMax,dwpMoy,dwpSq,ind4Min,ind4Max,ind4Moy,ind4Sq,ind5Min,ind5Max,ind5Moy,ind5Sq)) = truc
    return ((longitude, latitude), (tmpfMin,tmpfMax,tmpfMoy/count,tmpfSq/count,tmpMin,tmpMax, tmpMoy/count,tmpSq/count,dwpMin,dwpMax, dwpMoy/count,dwpSq/count,ind4Min,ind4Max,ind4Moy/count,ind4Sq/count,ind5Min,ind5Max,ind5Moy/count,ind5Sq/count))

def result(truc):
    ((longitude, latitude), (minTmpf, maxTmpf, avgTmpf, avgTmpfSq, minTmp, maxTmp, avgTmp, avgTmpSq, minDwp, maxDwp, avgDwp, avgDwpSq, minInd4,maxInd4,avgInd4,avgInd4Sq,minInd5,maxInd5,avgInd5,avgInd5Sq)) = truc
    return ((longitude, latitude), (minTmpf, maxTmpf,avgTmpf,avgTmpfSq-avgTmpf**2,minTmp, maxTmp,avgTmp,avgTmpSq-avgTmp**2,minDwp, maxDwp,avgDwp,avgDwpSq-avgDwp**2,minInd4,maxInd4,avgInd4,avgInd4Sq-avgInd4**2,minInd5,maxInd5,avgInd5,avgInd5Sq-avgInd5**2))

def distance(x,y):
    assert len(x) == len(y),"the length of two array should be equal"
    sum = 0
    for i in range(len(x)):
        sum += (x[i]-y[i])**2
    return sum

def strToTime(time):
    time = time + ' 00:00:00.000000'
    return time


def getData(jour1,jour2,ind1,ind2,ind3,ind4,ind5):
    jour1=jour1+'-01'
    jour2=jour2+'-31'
    listGenerator= []
    data = {}
    session = get_session()
    for year in range(int(jour1[0:4]),int(jour2[0:4])+1):
        if year == int(jour1[0:4]):
            for month in range(int(jour1[5:7]),13):
                sql = f"""
                    SELECT longitude,latitude,{ind1},{ind2},{ind3},{ind4},{ind5} 
                    FROM asositalytime 
                    WHERE year = {year} and month = {month} and validdate >= '{jour1}' and validdate <= '{jour2}'
                    """
                listGenerator.append(session.execute(sql))
                
        elif year == int(jour2[0:4]):
            for month in range(1,int(jour2[5:7])+1):
                sql = f"""
                    SELECT longitude,latitude,{ind1},{ind2},{ind3},{ind4},{ind5} 
                    FROM asositalytime 
                    WHERE year = {year} and month = {month} and validdate >= '{strToTime(jour1)}' and validdate <= '{strToTime(jour2)}'
                    """
                listGenerator.append(session.execute(sql))

        else:
            for month in range(1,13):
                sql = f"""
                    SELECT longitude,latitude,{ind1},{ind2},{ind3},{ind4},{ind5} 
                    FROM asositalytime 
                    WHERE year = {year} and month = {month} and validdate >= '{strToTime(jour1)}' and validdate <= '{strToTime(jour2)}'
                    """
                listGenerator.append(session.execute(sql))
    t = itertools.chain(*listGenerator)
    D = sc.parallelize(t)

    #for each station, we take the avg of theire data for kmeans
    data = D.filter(notNull).map(myMap).reduceByKey(myReduce).map(calcul).map(result).collectAsMap()
    return data

def Kmeans(data,k=3,times=10,nbInd=5):
    #initialiser position et dist
    col = nbInd*4

    position = []
    dist = dict()
    for pst in data:
        position.append(pst)
        data[pst] = list(data[pst])
        data[pst].append(0)
        dist[pst] = []


    Center = []

    random.shuffle(position)

    for i in range(k):
        Center.append(data[position[i]][0:col])   

    for p in position:
        for c in Center:
            dist[p].append(distance(data[p][0:col],c))
        clust = dist[p].index(min(dist[p]))
        data[p][col] = clust

    #recalucler center
    arrayAssit = np.zeros(col+1).tolist()
    for iteration in range(times):
        s = []
        for i in range(k):
            s.append(arrayAssit)
        for p in position:
            for i in range(k):
                if data[p][col] == i:
                    s[i] += np.append(np.array(data[p][0:col]),1)

        for i in range(k):
            s[i][0:col] /= s[i][col]
            Center[i] = s[i][0:col]
            
        #recalculer distance and clust
        for p in position:
            dist[p] = []
            for c in Center:
                dist[p].append(distance(data[p][0:col],c))
            clust = dist[p].index(min(dist[p]))
            data[p][col] = clust    
    return position, data
            
def drawMap(position, data, nbInd):
    
    longtitude = 7.3687
    latitude = 45.7385 

    col = nbInd*4
    COLOR = {0: "lightblue", 1: "lightgreen", 2: "orange", 3: "red", 4: "cadetblue", 5: "pink", 6: "white", 7: "lightgray"}

    tmap = folium.Map(
    location=[latitude, longtitude],
    zoom_start=6,
    tiles="cartodbpositron",
    )

    for p in position:
        test = folium.Html('<b>tmpf:{}</b></br> <b>dwpf:{}</b></br> <b>humidity:{}</b></br>  <b>windspeed:{}</b></br> <b>pressure:{}</b></br> <b>lon:{}</b></br> <b>lat:{}</b></br> '.format(data[p][2],data[p][6],data[p][10],data[p][14],data[p][18],p[0],p[1]),script=True)
        popup = folium.Popup(test, max_width=2650)
        folium.Marker(
            location= [p[1], p[0]],
            icon=folium.Icon(color=COLOR[data[p][col]], icon="cloud"),
            popup=popup
        ).add_to(tmap)

    tmap.save("Q3.html")


def partitionMap(jourDepart, jourFin, nbK):
    data = getData(jour1=jourDepart,jour2=jourFin,ind1='temperatureF',ind2='dewpointf',ind3='relativehumidity',ind4='windspeed',ind5='pressure')
    p,d=Kmeans(data,k=nbK,times=10,nbInd=5)
    drawMap(p,d,5)

#partitionMap('2011-01', '2011-10', 3)