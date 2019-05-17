import sys
from pyspark import SparkConf, SparkContext


def main(file_name: str) -> None:
    spark_conf = SparkConf() # We create the SparkConf
    spark_context = SparkContext(conf=spark_conf) # and the SparkContext.

    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN) # Logger for warnings.

    # Now we create the RDD.
    airports = spark_context \
        .textFile(sys.argv[1])\
        .map(lambda line: line.split(",")) \
        .filter(lambda array: array[8] == "\"ES\"") \
        .map(lambda array: (array[2], 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortBy(lambda pair: pair[1], False) \
        .persist()

    # After that we can show it.
    for line in airports.collect():
        print(line)

    spark_context.stop()


if __name__ == "__main__":
    """
    Python program that uses Apache Spark to find Spanihs airports
    """

    if len(sys.argv) != 2:
        print("Usage: spark-submit Airports.py <file>", file=sys.stderr)
        exit(-1)

    main(sys.argv[1])

