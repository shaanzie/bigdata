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
		.add("c13", "string") \
		.add("c14", "string")

#maxfilestrigger not necessary.
#monitors stream directory
csvDF = spark \
    .readStream \
#    .option("maxFilesPerTrigger", 1)
    .option("sep", ";") \
    .schema(userSchema) \
    .csv("hdfs://localhost:9000/stream/")


hashtags = csvDF.select("c8")
words = hashtags.select(explode(split(hashtags.c8, ",")))
words = words.withColumnRenamed("col", "hashtag")
df1 = csvDF.select("11", "c13", "c14").withColumn("ratio", (csvDF.c13/ csvDF.c14))

#if you dont specify a value for awaittermination, then this will run forever without giving a chance to the subsequent queries
words.groupBy("hashtag").count().orderBy("count", ascending = False).writeStream.outputMode("complete").format("console").start().awaitTermination(20)

df1.groupBy("11").agg(F.max("ratio")).orderBy("max(ratio)", ascending = False).writeStream.outputMode("complete").format("console").start().awaitTermination(20)
