__author__ = 'Bella'

import sys
from pyspark import SparkContext
from pyspark.mllib.feature import IDF
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.clustering import KMeans
from numpy import array
from math import sqrt
import itertools
from pyspark.sql import *
from pyspark.sql import types
from pyspark.streaming import StreamingContext


if __name__ == "__main__":

    #P3

    sc = SparkContext("local[2]", "p3_sql")
    ssc = StreamingContext(sc, 3)  # set batch size to be 1 second #number of sec get data before processing the data
    input_top = ssc.socketTextStream("localhost", int(sys.argv[2]))

    sqlContext = SQLContext(sc)
    doc_test = sc.textFile(sys.argv[1])

    clean_dt = []
    for line in doc_test.collect():
        clean_dt.append(line.split(','))

    stream = sc.parallelize(clean_dt)

    # The schema is encoded in a string.
    fields = [types.StructField("topic", types.StringType(), True),
              types.StructField("title", types.StringType(), True)]
    schema = types.StructType(fields)
    # Apply the schema to the RDD.
    schema_clust = sqlContext.createDataFrame(stream, schema)

    # Register the DataFrame as a table.
    schema_clust.registerTempTable("clust")

    # SQL can be run over DataFrames that have been registered as a table.
    results = sqlContext.sql("SELECT title FROM clust")
    topics = results.map(lambda p: "Topic: " + p.topic)
    for topic in topics.collect():
        print topic

    ssc.start()
    ssc.awaitTermination()










