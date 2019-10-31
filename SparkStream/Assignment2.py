
import findspark
findspark.init()

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests

def aggregate_tweets_count(new_values, total_sum):
	return sum(new_values) + (total_sum or 0)


def takeAndPrint(rdd):
	taken = rdd.take(4)
	taken = taken.transform(lambda rdd: rdd.sortBy(lambda x: x[0], ascending = False))
	i = 0
	for record in taken[:5]:
		if(i != 2):
			print(record[0], end = ", ")
		else:
			print(record[0])
		i+=1

def cleanData(x):
	hashtags = x.split(",")
	clean = []
	for hashtag in hashtags:
		if hashtag == " " or hashtag == "  " or hashtag == "":
		  	pass
		else:
		 	clean.append(hashtag)
	return clean

conf=SparkConf()
conf.setAppName("A2")
sc=SparkContext(conf=conf)

c = float(sys.argv[1])
t = float(sys.argv[2])

ssc=StreamingContext(sc, c)
ssc.checkpoint("/checkpoint_BIGDATA")

dataStream=ssc.socketTextStream("localhost",9009)

tweet=dataStream.map(lambda w: w.split(';')[7])
tweet1 = tweet.flatMap(lambda w: cleanData(w))
tweet1 = tweet1.map(lambda x: (x, 1))


totalcount = tweet1.updateStateByKey(aggregate_tweets_count)

sorted_ = totalcount.transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending = False))

sorted_.foreachRDD(takeAndPrint)


ssc.start()
ssc.awaitTermination(t)
ssc.stop()
