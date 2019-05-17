import sys
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit TrendingTopic.py <file>", file=sys.stderr)
        exit(-1)

    # Create the SparkConf and the SparkContext
    spark_conf = SparkConf().setAppName("TrendingTopic").setMaster("local[2]")
    spark_context = SparkContext(conf=spark_conf)

    # Create the logger
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Create a list of words we don't want to have
    words_banned = ["you", "all", "with", "have", "The", "This", "has", "will", "http://t.co/t3KHlNvtIz", "http://t.?", "and", "for", "CET", "Mar", "2014", "Thursday's", "When", "the"]

    # Take the top ten words more written
    output = spark_context \
        .textFile(sys.argv[1]) \
        .map(lambda word: word.split(' '))\
        .filter(lambda word: len(word[2]) > 2)\
        .filter(lambda word: word[2] not in words_banned)\
        .map(lambda word: (word[2], 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .sortBy(lambda pair: pair[1], ascending=False) \
        .take(10)

    # Show it
    for (word, count) in output:
        print("%s: %i" % (word, count))

    # Stop the SparkContext
    spark_context.stop()