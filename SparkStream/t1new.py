from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType
from pyspark.sql import functions as F
from pyspark.sql import window
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("Most popular hashtag").getOrCreate()


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


hashtags = csvDF.select("c8")
words = hashtags.select(explode(split(hashtags.c8, ",")))
words = words.withColumnRenamed("col", "hashtag")

#if you dont specify a value for awaittermination, then this will run forever without giving a chance to the subsequent queries
words.groupBy("hashtag").count().orderBy("count", ascending = False).writeStream.outputMode("complete").format("console").start().awaitTermination()

pop = csvDF.select("11", getRatio("13", "14").alias("ratio1"))
pop = pop.groupBy("11").agg(F.max("ratio1"))
pop = pop.orderBy("max(ratio1)", ascending = False).writeStream.outputMode("complete").format("console").start().awaitTermination(20)
