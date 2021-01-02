from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
   
sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

import re

def getDataCount(path):
    return spark.read.text([path]).count()

def getData(dataNumber,path):
    logs = spark.read.text([path])
    return[item['value'] for item in logs.take(dataNumber)]

def getSites(data):
    return [re.search("http[s]?://[^./]+\.[^.%/ ]+", item).group()
           if re.search("http[s]?://[^./]+\.[^.%/? ]+", item)
           else 'no website'
           for item in data]
def getTopSites(runkNumber,sites):
    rdd = sc.parallelize(sites)
    return rdd.filter(lambda x: x!="no website").map(lambda x: (x,1)).reduceByKey(lambda x1,x2 : x1+x2  ).map(lambda aTuple: (aTuple[1], aTuple[0])).sortByKey(ascending=False).map(
    lambda aTuple: (aTuple[0], aTuple[1])).take(runkNumber)

if __name__ == "__main__":
    count=getDataCount('access.log')
    print(" count of log file: "+str(count))
    web_sites = getData(30000,'access.log')
    sites = getSites(web_sites)
    result = getTopSites(10,sites)
    for i in result:
        print(i)