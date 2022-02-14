from pyspark import SparkContext
from pyspark import SparkConf

def safe_str(obj):
    try:
        s2 = str(obj)
    except:
        s2 = obj.encode('utf-8').strip()
    return s2

conf = SparkConf().setAppName("CALCULATE NB RATINGS  - Pyhon Spark")
sc=SparkContext(conf=conf)
dataRatings =sc.textFile("/user/student/MiniProject/ratings.dat")
dataRatingSplitted = dataRatings.map(lambda line: line.split("::"))
rc = dataRatingSplitted.map(lambda line: (line[2],1))
rc_reduce = rc.reduceByKey(lambda a,b: a+b)
csvrc_reduce=rc_reduce.map(lambda list: ','.join(safe_str(elt) for elt in list))
#Stockage sous HDFS
csvrc_reduce.saveAsTextFile("hdfs:///user/student/output-RalingsVaue/")
sc.stop()
