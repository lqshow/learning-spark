package com.example.spark.helpers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.net.URL;

public class Utils implements Serializable {
    private final static String appName = "Simple Application";

    public static SparkSession createSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master("local")
                .getOrCreate();

        return spark;
    }

    public static JavaSparkContext createJavaSparkContext(SparkSession spark) {
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        return jsc;
    }

    public static JavaSparkContext createJavaSparkContext() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        return jsc;
    }

    public static Dataset<String> datasetFromTextFile(SparkSession spark) {
        URL path = Utils.class.getResource("/people.txt");
        Dataset<String> peopleData = spark.read().textFile(path.toString());

        return peopleData;
    }

    public static JavaRDD<String> javaRDDFromTextFile(SparkSession spark) {
        URL path = Utils.class.getResource("/people.txt");
        JavaRDD<String> peopleData = spark.read().textFile(path.toString()).javaRDD();

        return peopleData;
    }
}
