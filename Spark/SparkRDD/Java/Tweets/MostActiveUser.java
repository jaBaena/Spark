package org.masterbigdata3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class MostActiveUser {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf() // We create the SparkConf
                .setAppName("MostActiveUser_Spark")
                .setMaster("local[2]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf); // and the SparkContext

        // Read the file
        JavaRDD<String> lines = sparkContext.textFile("/home/master/Descargas/tweets.txt");

        // Separate the columns
        JavaRDD<String[]> fields = lines.map(line -> line.split("\t"));


        // map the airports in order to count the types of airports
        JavaPairRDD<String, Integer> wordcounts = fields.mapToPair(array -> new Tuple2<String, Integer>(array[1], 1));

        // For counting
        JavaPairRDD<String, Integer> wordcountsfinal = wordcounts.reduceByKey((x, y) -> x + y);

        // Sort it. We need to change the kye-value into value-key in order to sort by key
        List<Tuple2<Integer, String>> mostActiveUser = wordcountsfinal.mapToPair(pair -> new Tuple2<>(pair._2, pair._1)).sortByKey(false).take(1);

        // Now we can show it
        for (Tuple2<?, ?> tuple : mostActiveUser) {
            System.out.println(tuple._2() + ", " + tuple._1());
        }
    }
}