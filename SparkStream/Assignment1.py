from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[2]", "A1")
ssc = StreamingContext(sc, 0.1)

lines = ssc.textFileStream("hdfs://localhost:9000/stream/*")

words = lines.flatMap(lambda line : line.split(";"))

pairs = words.map(lambda word: (word, 1))

wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination(20)  # Wait for the computation to terminate

