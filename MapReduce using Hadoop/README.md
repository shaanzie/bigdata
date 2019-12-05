# MapReduce using Hadoop and Java

The objective of the codes here are to perform an exploratory analysis on the dataset given using Apache Hadoop's MapReduce framework. 
The dataset contains ball-by-ball data about each Indian Premier League cricket match played upto 2018. 

## Batsman Vulnerability

The first task was to find the number of times a batsman got out to a bowler, considering bowler-batsman pairs where the batsman has faced more than 5 balls from the bowler. The output needed to be sorted by number of wickets. 

## Bowler Vulnerability

The second task was to find the number of runs conceded by a bowler to a batsman, where the bowler-batsman pairs are said to have 5 deliveries between each other. The output needed to be sorted by the descending order of the number of runs.

## Most Prolific Batsman

The third task was to find for each venue, the batsman who was the most prolific, with the strike rate as the performance measure, with the batsman having faced a minimum of 10 deliveries at the venue.

## Replication Guidelines

Please ensure Hadoop DFS is running in the backend when these programs are run. These tasks were run on Hadoop 2.7 running in Pseudo-distributed mode. Please ensure Java 8 or above is correctly installed.

To replicate,
1. Ensure the alldata.csv file exists in the HDFS.
2. Run `javac -d . <file-name>.java`
3. Run `sudo gedit Manifest.txt`
4. Add the lines `Main-Class: <ClassName>` and press ENTER.
5. Create a jar file using `jar cfm <NameOfJar>.jar Manifest.txt ./*.class`
6. Run MapReduce Job as `$HADOOP_HOME/bin/hadoop jar <NameOfJar>.jar /inputMapReduce /mapreduce_output`
7. Results can be seen at `$HADOOP_HOME/bin/hdfs dfs -cat /mapreduce_output/part-00000`
