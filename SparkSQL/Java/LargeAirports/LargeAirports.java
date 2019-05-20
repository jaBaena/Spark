package org.masterbigdata3;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;

public class LargeAirports {
    public static void main(String[] args){

        Logger.getLogger("org").setLevel(Level.OFF);

        // Create the sparkSession
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Java Spark SQL. Large airports")
                .master("local[2]")
                //.config("spark.some.config.option", "some-value")
                .getOrCreate();

        // Configure the schema of the next dataframe
        StructType schema_airports = new StructType()
                .add("id", "String", true)
                .add("ident", "String", true)
                .add("type", "String", true)
                .add("name", "String", true)
                .add("latitude", "Double", true)
                .add("longitude", "Double", true)
                .add("elevation_ft", "Integer", true)
                .add("continent", "String", true)
                .add("iso_country", "String", true)
                .add("iso_region", "String", true)
                .add("municipality", "String", true)
                .add("scheduled_service", "String", true)
                .add("gps_code", "String", true)
                .add("iata_code", "String", true)
                .add("local_code", "String", true)
                .add("home_link", "String", true)
                .add("wikipedia", "String", true)
                .add("keywords", "String", true);

        Dataset<Row> airports = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .schema(schema_airports)
                .load("/home/master/Descargas/airports.csv")
                .cache();

        //airports.printSchema();
        //airports.show();

        Dataset<Row> large_airports = airports
        .filter(col("type").contains("large_airport"))
        .groupBy(col("iso_country"))
        .count();

        large_airports.printSchema();
        large_airports.show();

        // Configure the schema of the next dataframe
        StructType schema_countries = new StructType()
                .add("id", "String", true)
                .add("code", "String", true)
                .add("name", "String", true)
                .add("continent", "String", true)
                .add("wikipedia_link", "String", true)
                .add("keywords", "String", true);


        // Get airport name dataframe
        Dataset<Row> countries = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .schema(schema_countries)
                .load("/home/master/Descargas/countries.csv")
                .cache();

        //countries.printSchema();
        //countries.show();

        // Rename this column
        Dataset<Row> countries1 = countries.withColumnRenamed("code", "iso_country");

        // Join the dataframes
        Dataset<Row> join_dataframe = large_airports
        .join(countries1, large_airports.col("iso_country").equalTo(countries1.col("iso_country")))
        .select("name", "count")
        .sort(desc("count"));

        // Show it
        join_dataframe.printSchema();
        join_dataframe.show();
    }
}