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
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: pagerank <file> <iterations>", file=sys.stderr)
        sys.exit(-1)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    prevRanks = []

    it = 0
    while(1):
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

        it += 1
        prevRanks = ranks.collect()
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
        rankList = ranks.collect()
        if(len(prevRanks) == len(rankList)):
            b = True
            for i in range(len(prevRanks)):
                if round(prevRank[i], 5) != round(rankList[i], 5):
                    b = False
            if b:
                break
        print("iteration")

    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))
    print("iteration: ", it)
    spark.stop()
