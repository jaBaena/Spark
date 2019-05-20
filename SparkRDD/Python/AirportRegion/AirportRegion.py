from pyspark import SparkContext, SparkConf


def run() -> None:

    # Create the SparkContext
    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    # Set the logger for warnings
    logger = spark_context._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    # Operate with Airports dataset
    airport = spark_context \
        .textFile('/home/master/Descargas/airports.csv')\
        .map(lambda line : line.split(","))\
        .filter(lambda list : list[2] == "\"large_airport\"") \
        .map(lambda list: (list[8], 1))\
        .reduceByKey(lambda x, y: x + y)\

    # Operate with Countries dataset
    countries = spark_context\
        .textFile("/home/master/Descargas/countries.csv")\
        .map(lambda line: line.split(','))\
        .map(lambda list: (list[1], list[2]))\

    # Join the datasets in order to show what we want to show
    result = airport\
        .join(countries)\
        .map(lambda pair: (pair[1][1], pair[1][0]))\
        .sortBy(lambda pair: pair[1], ascending=False)

    for record in result.take(20):
        print(record)


if __name__ == "__main__":
    run()
