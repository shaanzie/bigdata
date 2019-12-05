# PageRank using Apache Spark

The objective of the codes here are to perform an exploratory analysis on the dataset given using Apache Spark with Python.
The dataset contains ball-by-ball data about each Indian Premier League cricket match played upto 2018. 

## Batting Prowess

The objective was to write python code using PySpark library to rank players as batsmen using PageRank on Spark using the bowling rank dataset given and  to print out the players and their ranks in descending order of rank. 

For PageRank it was considered that each player was a page and initial rank of each player was the sum of their bowling average against all players and the links were be from the batsman to the bowler for every pair.

## Bowling Prowess

The objective was to write python code using PySpark library to rank players as bowlers using PageRank on Spark using the bowling rank dataset given and  to print out the players and their ranks in descending order of rank. 

For PageRank it was considered that each player was a page and initial rank of each player was the sum of their bowling average against all players and the links were be from the batsman to the bowler for every pair.

## Replication

Ensure Spark 2.2.0 is installed correctly.
Run `$SPARK_HOME/bin/spark-submit <filename.py>`