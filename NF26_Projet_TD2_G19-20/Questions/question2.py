from datetime import datetime
from mpl_toolkits.basemap import Basemap
import cassandra.cluster
import matplotlib.pyplot as plt
import os

def getDataFromDatabase(yearValue, monthValue, dateValue, varToPick):
    
    cluster = cassandra.cluster.Cluster(['localhost'])
    session = cluster.connect('adamloic_lixiang_projet')
    
    selectQuery = f"""
        SELECT longitude, latitude, {varToPick}
        FROM asosItalyTime
        WHERE year = {yearValue}
        AND month = {monthValue}
        AND validDate = '{dateValue}'
        """
    return session.execute(selectQuery)
            
def drawmap(yearValue, monthValue, dayValue, hourValue, minuteValue, varToPick):
    
    if not os.path.exists("./images"):
        os.makedirs("./images")
        
    plt.rcParams.update({'font.size': 10})
    date = datetime(yearValue, monthValue, dayValue, hourValue, 
                    minuteValue).strftime('%Y-%m-%d %H:%M')
    posX, posY, values = getDataForMap(yearValue, monthValue, date, varToPick)
    
    if (len(values) == 0):
        print("Aucune valeur disponible à cet instant.")
        return
    
    fig, ax = plt.subplots(figsize=(10,20))
    m = Basemap(resolution='l',
            projection='merc',
            llcrnrlon=6.6, llcrnrlat= 35.25, urcrnrlon=18.84, urcrnrlat=47.1)
    m.drawmapboundary(color='#A9A9A9', fill_color='#46bcec')
    m.fillcontinents(color='#f2f2f2',lake_color='#46bcec')
    m.drawcountries(linewidth=0.25)
    m.drawcoastlines()  
    x, y = m(posX, posY)
    m.plot(x, y, 'o', markersize = 2, color='#8B0000')
    
    k = 0
    for i,j in zip(x,y):
        if isinstance(values[k], float):
            values[k] = round(values[k], 2)
        ax.text(i+100,j+100, str(values[k]), color='#006400')
        k = k+1
    
    plt.title("Asos variable '" + varToPick + "' in Italy, " + date)
    plt.show()
    fig.savefig("./images/" + str(yearValue) + str(monthValue) + str(dayValue) + str(hourValue) + 
                str(minuteValue) + "_" + varToPick + ".pdf", format = 'pdf')
    plt.close()
    
def getDataForMap(yearValue, monthValue, dateValue, varToPick):
    
    rows = getDataFromDatabase(yearValue, monthValue, dateValue, varToPick)

    posX=[]
    posY=[]
    values=[]

    for element in rows:
        
        if(element[2] is not None):
            
            posX.append(element[0])
            posY.append(element[1])
            values.append(element[2])
    
    return posX, posY, values

#drawmap(2013,7,14,14,50, 'windDirection')       
