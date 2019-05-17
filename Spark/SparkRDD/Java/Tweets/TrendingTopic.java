package org.masterbigdata3;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;

public class TrendingTopic {

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);

        SparkConf sparkConf = new SparkConf() // We create the SparkConf
                .setAppName("TrendingTopic_Spark")
                .setMaster("local[2]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf); // and the SparkContext

        // Read the file
        JavaRDD<String> lines = sparkContext.textFile("/home/master/Descargas/tweets.txt");

        // Separate the columns
        JavaRDD<String[]> fields = lines.map(line -> line.split(" "));

        // First filter for words we don't want to have
        JavaRDD<String[]> filterlength = fields.filter(array -> array[2].length() > 2);

        // Other words we don't want to have
        List<String> specialCharacters = Arrays
                .asList(new String[] {"you", "all", "with", "have", "The", "This", "has", "will", "http://t.co/t3KHlNvtIz", "http://t.?", "and", "for", "CET", "Mar", "2014", "Thursday's", "When", "the"});

        // The second filter
        JavaRDD<String[]> filterword = filterlength.filter(word -> !specialCharacters.contains(word[2]));

        // Map the airports in order to count the types of airports
        JavaPairRDD<String, Integer> wordcounts = filterword.mapToPair(array -> new Tuple2<String, Integer>(array[2], 1));

        // For counting
        JavaPairRDD<String, Integer> wordcountsfinal = wordcounts.reduceByKey((x, y) -> x + y);

        // Sort it. We need to change the kye-value into value-key in order to sort by key
        List<Tuple2<Integer, String>> trendingTopic = wordcountsfinal.mapToPair(pair -> new Tuple2<>(pair._2, pair._1)).sortByKey(false).take(10);

        // Now we can show it
        for (Tuple2<?, ?> tuple : trendingTopic) {
            System.out.println(tuple._2() + ", " + tuple._1());
        }
    }
}
