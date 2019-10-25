import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
		print("----------=========- %s -=========----------" % str(time))
		try:
			row_rdd = rdd.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
			row_rdd = row_rdd.map(lambda w: print(w[0], w[1]))
		except:
			e = sys.exc_info()[0]
			print("Error: %s" % e)

def tmp(x):
	return (x.split(';')[0],1)

conf=SparkConf()
conf.setAppName("A2")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)
# dataStream.pprint()
#tweet=dataStream.map(tmp)
# OR
tweet=dataStream.map(lambda w:(w.split(';')[7],1))
#count=tweet.reduceByKey(lambda x,y:x+y)
#count.pprint()

#TO maintain state
totalcount=tweet.updateStateByKey(aggregate_tweets_count)
#totalcount.pprint()

totalcount = totalcount.sort

#To Perform operation on each RDD
# totalcount.foreachRDD(process_rdd)
totalcount.pprint()

ssc.start()
ssc.awaitTermination(2)
ssc.stop()
