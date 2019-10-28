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

def takeAndPrint(rdd):
	taken = rdd.take(4)
	i = 0
	for record in taken[:3]:
		if(i != 2):
			print(record[0], end = ", ")
		else:
			print(record[0])
		i+=1

conf=SparkConf()
conf.setAppName("A2")
sc=SparkContext(conf=conf)

ssc=StreamingContext(sc,2)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweet=dataStream.map(lambda w:(w.split(';')[7],1))

totalcount=tweet.updateStateByKey(aggregate_tweets_count)

sorted_ = totalcount.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))

# sorted_.pprint()
# row_rdd = sorted_.map(lambda w: Row(tweetid=w[0], no_of_tweets=w[1]))
sorted_.foreachRDD(takeAndPrint)

ssc.start()
ssc.awaitTermination(2)
ssc.stop()
