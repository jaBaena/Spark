import sys

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: spark-submit CountCharacters.py <file>", file = sys.stderr)
        exit(-1)

    # Create the SparkContext
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    # Set the warnings
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Load the file
    lines = spark_context.textFile(sys.argv[1]).cache()

    number_of_as = lines.filter(lambda line: "a" in line).count()

    number_of_bs = lines.filter(lambda line: "b" in line).count()

    # We could save the results in a txt format
    #number_of_cs = lines.filter(lambda line: "c" in line).saveAsTextFile("Cs.txt")

    print("Number of 'a's: " + str(number_of_as))
    print("Number of 'b's: " + str(number_of_bs))

    spark_context.stop()