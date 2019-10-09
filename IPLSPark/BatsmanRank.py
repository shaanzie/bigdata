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

def parseBat(urls):
    parts = re.split(',', urls)
    return float(parts[2])/float(parts[3]), parts[0] + ":" + parts[1] 


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

    avgs = lines.map(lambda urls: parseBat(urls)).distinct().groupByKey().cache()
    final_map = {}
    for (link, rank) in avgs.collect():
        for i in rank:
         bat_guy = i.split(":")[0]
         bowl_guy = i.split(":")[1]

        #  print("batsman %s and bowler %s and the type is %s" % (bat_guy, bowl_guy, type(link)))
         if(bowl_guy not in final_map):
             final_map[bowl_guy] = link
         else:
             final_map[bowl_guy] += link
    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], max(1, final_map[url_neighbors[0]])))

    prev_lambda_list = [1]
    prev_lambda = 0
    iter = 0

    weight = int(sys.argv[3])
    weight = weight/100.0

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    if int(sys.argv[2]) != 0:
        for iteration in range(int(sys.argv[2])):
            # Calculates URL contributions to the rank of other URLs.
            contribs = links.join(ranks).flatMap(
                lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))

            # Re-calculates URL ranks based on neighbor contributions.
            ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1- weight))

    else:
        iterations = 0
        t1 = 1
        t2 = 0
        while(abs(t1 - t2) > 0.00001):
          iterations+=1
          contribs = links.join(ranks).flatMap(
              lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1]))
          prev_ranks = ranks
          ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * weight + (1-weight))
          prev_ranks_new = list(prev_ranks.collect())
          sorted_prev = sorted(prev_ranks_new, key = lambda x: x[0])
       	  ranks_new = list(ranks.collect())
          sorted_pres = sorted(ranks_new, key = lambda x: x[0])    
          t1 = sorted_prev[0][1]
          t2 = sorted_pres[0][1]
    # Collects all URL ranks and dump them to console.
    ranks = ranks.sortBy(lambda x: x[1], False)
    for (link, rank) in ranks.collect():
        print("%s,%s" % (link, rank))

    spark.stop()