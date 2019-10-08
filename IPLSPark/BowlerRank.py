from __future__ import print_function

import re
import sys
from operator import add

from pyspark.sql import SparkSession


def computeContribs(urls, rank):
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


def parseNeighbors(urls):
    """Parses a urls pair string into urls pair."""
    parts = re.split(',', urls)
    return parts[0], parts[1]

def parseAvg(urls):
    parts = re.split(',', urls)
    return parts[0], float(int(parts[2]))/float(int(parts[3])) 

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pagerank <file> and optional <iterations>", file=sys.stderr)
        sys.exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
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

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    if len(sys.argv) == 3:
        for iteration in range(int(sys.argv[2])):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)

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
        print("%s has rank: %s." % (link, rank))

    spark.stop()
