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

def parseBowls(urls):
    parts = re.split(',', urls)
    return float(parts[2])/float(parts[3]), parts[3]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: pagerank <file> and optional <iterations>", file=sys.stderr)
        sys.exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    # Initialize the spark context.
    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    avgs = lines.map(lambda urls: parseBowls(urls)).distinct().groupByKey().cache()

    for (link, rank) in avgs.collect():
        print("%s IS OF: %s." % (link, rank))
    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], avgs.collect()[url_neighbors[0]][0]))

    prev_lambda_list = [1]
    prev_lambda = 0
    iter = 0

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    if len(sys.argv) == 3:
        for iteration in range(int(sys.argv[2])):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    else:
        while(True):
            prev_lambda = min(prev_lambda_list)
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
            prev_lambda_list.append(contribs.collect()[1][1])
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
            new_lambda = min(prev_lambda_list)
            if(prev_lambda - new_lambda < 0.00001):
                print(new_lambda, prev_lambda)
                break
            iter+=1
    # Collects all URL ranks and dump them to console.
    for (link, rank) in ranks.collect():
        print("%s has rank: %s." % (link, rank))

    spark.stop()