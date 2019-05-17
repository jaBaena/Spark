package org.masterbigdata3;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Airports{
public static void main(String[]args){
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf() // We create the SparkConf
                .setAppName("SparkAirport")
                .setMaster("local[2]");

    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf); // and the SparkContext

    // Read the file
    JavaRDD<String> lines = sparkContext.textFile("/home/master/Descargas/airports.csv");

    // For separating the columns
    JavaRDD<String[]> fields = lines.map(line -> line.split(","));

    // For selecting the spanish airports
    JavaRDD<String[]> spanishAirports = fields.filter(array -> array[8].equals("\"ES\""));

    // mapping the airports in order to count the types of airports
    JavaPairRDD<String, Integer> airportTypes = spanishAirports.mapToPair(array -> new Tuple2<String, Integer>(array[2], 1));

    // For counting
    JavaPairRDD<String, Integer> numAirports = airportTypes.reduceByKey((x, y) -> x + y);

    // Sorting it. We need to change the kye-value into value-key in order to sort by key
    List<Tuple2<Integer, String>> numAirportsSorted = numAirports.mapToPair(pair -> new Tuple2<>(pair._2, pair._1)).sortByKey(false).collect();

    // Show it
    for (Tuple2<?, ?> tuple : numAirportsSorted) {
        System.out.println(tuple._2() + ", " + tuple._1());
    }

    sparkContext.stop();

        }
}