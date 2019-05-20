"""find the longest tweet emitted by any user.
The program will print in the screen the user name,
the length of the tweet, and the time and date of the tweet."""

import sys
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: spark-submit LongestTweet.py <file>", file=sys.stderr)
        exit(-1)

    # Create the SparkConf and the SparkContext
    spark_conf = SparkConf().setAppName("LongestTweet").setMaster("local[2]")
    spark_context = SparkContext(conf=spark_conf)

    # Create the logger
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # \t is for tabulations
    tweetListSorted = spark_context \
        .textFile(sys.argv[1]) \
        .map(lambda line: line.split('\t')) \
        .map(lambda array: (array[1], len(array[2]), array[3])) \
        .sortBy(lambda array: array[1], False)

    # Take the tweet which has more characters
    tweetMax = tweetListSorted.first()

    # Take all the tweets which have the same characters than the previous tweet
    output = tweetListSorted.filter(lambda tweet: tweet[1] == tweetMax[1]).collect()

    # Show it
    print("Name,      ", "Length of tweet,              ", "Date")
    for word in range(output.__len__()):
        print(output[word][0], ": ", output[word][1], " - " ,output[word][2])

    # Stop the Sparkcontext
    spark_context.stop()