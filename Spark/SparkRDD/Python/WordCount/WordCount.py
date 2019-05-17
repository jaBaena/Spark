import sys
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit WordCount.py <file>", file=sys.stderr)
        exit(-1)

    # Create the SparkConf and the SparkContext
    spark_conf = SparkConf().setAppName("WordCount").setMaster("local[2]")
    spark_context = SparkContext(conf=spark_conf)

    # Set the warnings
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Read the text file and count the words
    output = spark_context \
        .textFile(sys.argv[1]) \
        .flatMap(lambda line: line.split(' ')) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(20)

    # Show the words and the times they appear
    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark_context.stop()