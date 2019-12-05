# Tweet Analysis using Spark Streaming

For this task, spark streaming with python was used in two different ways to stream and analyse data. The dataset was a collection of tweets during the FIFA 2018 world cup. 

## Most Common Hashtag

This is an application of structured streaming using Dstreams in Spark. The most common hashtag of each file streamed was found using this. 

## Most Popular User

This is an application of structured streaming using Dstreams in Spark. The most popular user of each file streamed was found using this. 

## Top 5 Hashtag

This is an application of socket streaming in Spark. The top 5 hashtags of each file streamed was found in sorted order for each window. 


## Replication

Ensure Spark 2.2.0 is installed correctly.

For the Top5Hashtags.py file, make sure app.py is running in the background for the socket stream.

Run `$SPARK_HOME/bin/spark-submit <filename.py>`
