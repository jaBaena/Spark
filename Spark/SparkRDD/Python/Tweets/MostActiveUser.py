"""find the user that has written the largest amount of tweets.
 The program will print in the screen the name of the user and the number of tweets."""

import sys
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit MostActiveUser.py <file>", file=sys.stderr)
        exit(-1)

    # Create the SparkConf and the SparkContext
    spark_conf = SparkConf().setAppName("MostActiveUser").setMaster("local[2]")
    spark_context = SparkContext(conf=spark_conf)

    # Create the logger
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Take the user who wrote more tweets
    output = spark_context \
        .textFile(sys.argv[1]) \
        .flatMap(lambda line: line.split('\t')) \
        .filter(lambda word: "@" in word)\
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(1)

    # Show it
    for (word, count) in output:
        print("%s: %i" % (word, count))

    # Stop the SparkContext
    spark_context.stop()