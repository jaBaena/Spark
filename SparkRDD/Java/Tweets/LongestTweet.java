package org.masterbigdata3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class LongestTweet {


    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf() // We create the SparkConf
                .setAppName("LogestTweet_Spark")
                .setMaster("local[2]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf); // and the SparkContext

        // Read the file
        JavaRDD<String> lines = sparkContext.textFile("/home/master/Descargas/tweets.txt");

        // Separate the columns
        JavaRDD<String[]> fields = lines.map(line -> line.split("\t"));

        // Take the columns we want
        JavaPairRDD<String, Integer> longestTweet = fields.mapToPair(array -> new Tuple2<>(array[1] + " - " + array[3], array[2].length())); //          .map(lambda array: (array[1], len(array[2]), array[3])) \

        // Sort the RDD
        JavaPairRDD<Integer, String> longesTweetSorted = longestTweet.mapToPair(pair -> new Tuple2<>(pair._2, pair._1)).sortByKey(false);

        // Take the 20 longest tweets
        List<Tuple2<Integer, String>> output = longesTweetSorted.take(20);

        // Now we can show it
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._2() + ", " + tuple._1());
        }
    }
}