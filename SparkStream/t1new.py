from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

<<<<<<< HEAD
from pyspark.sql import window
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("Most popular hashtag").getOrCreate()


=======
from pyspark.sql.functions import udf
spark = SparkSession.builder.appName("StructuredNetworkWordCount").getOrCreate()

from pyspark.sql import SQLContext
sqlContext = SQLContext(spark)

# this dataframe will contain the data coming in from our socket. we will be getting a csv values
#socketDF = spark \
#    .readStream \
 #   .format("socket") \
#    .option("host", "localhost") \
 #   .option("port", 9999) \
#    .load()


#socketDF.isStreaming()

#socketDF.printSchema()
@udf
def cleanData(x):
  hashtags = x.split(",")
  return hashtags

# define the schema of the csv file we are reading?
# the separation is ';' and lines are separated by '\n'?
# should path to directory be a hdfs file
>>>>>>> 5c915aa7fd62b71aaff9abc7ac07e448d7893dbe
userSchema = StructType().add("1", "string") \
		.add("2", "string") \
		.add("3", "string") \
		.add("4", "string") \
		.add("5", "string") \
		.add("6", "string") \
		.add("7", "string") \
		.add("c8", "string") \
		.add("9", "string") \
		.add("10", "string") \
		.add("11", "string") \
		.add("12", "string") \
		.add("13", "string") \
		.add("14", "string") 
csvDF = spark \
    .readStream \
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/A3/") 

<<<<<<< HEAD

hashtags = csvDF.select("c8")
words = hashtags.select(explode(split(hashtags.c8, ",")))
words = words.withColumnRenamed("col", "hashtag")

words.groupBy("hashtag").count().orderBy("count", ascending = False).writeStream.outputMode("complete").format("console").start().awaitTermination()


=======
# Generate running word count
hashtags = csvDF.select("c8")
words = hashtags.select(explode(split(hashtags.c8, ",")))
#hashtagsCleaned = hashtags.writeStream.format("memory").queryName("hclean").start#

#hashtag1 = hashtags.rdd.map(lambda x: x)

#hashtagsC = hashtags.select(cleanData("c8"))
#words = hashtagsC.withColumn("cleanData(c8)", explode(hashtagsC["cleanData(c8)"]))

#rdd.flatMap(lambda x: cleanData(x))


words.registerTempTable("words")
#words1 = sqlContext.sql("select c8, no_of_tweets from hashtags order by no_of_tweets desc limit 20")
words1 = sqlContext.sql("select col as name, COUNT(*) as counts from words GROUP BY name limit 20")
w = words1.select("name", "counts")
query = w \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination(6)
>>>>>>> 5c915aa7fd62b71aaff9abc7ac07e448d7893dbe
