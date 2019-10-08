from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    parts = re.split(',', urls)
    return parts[0], parts[1]

def parseAvg(urls):
    parts = re.split(',', urls)
    return parts[0], float(int(parts[2]))/float(int(parts[3])) 

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: pagerank <file> <iterations> <rank update rule>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Task 1")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    ranks = lines.map(lambda string: parseAvg(string))\
					.reduceByKey(add)\
					.mapValues(lambda x: 1.0 if x < 1 else x)\
					.cache()

    prev_lambda = 0
    curr_lambda = ranks.max()[1]
    itr = 0

    if int(sys.argv[2]) != 0:

        for iteration in range(int(sys.argv[2])):

            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * (sys.argv[3])/100.0 + 1-(sys.argv[3])/100.0)

    else:
        while(True):
            prev_lambda = curr_lambda

            contribs = links.join(ranks)\
                            .flatMap(lambda item: computeContribs(item[1][0], item[1][1]))
            
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)
            
            curr_lambda = ranks.max()[1]

            if abs(prev_lambda - curr_lambda) < 0.00001:
                print(prev_lambda, curr_lambda)
                break

            itr += 1


    ranks = ranks.map(lambda x:(x[1],x[0]))\
					.sortByKey(False)\
					.map(lambda x:(x[1],x[0]))

    for (link, rank) in ranks.collect():
        print("%s,%s" % (link, rank))

    spark.stop()
