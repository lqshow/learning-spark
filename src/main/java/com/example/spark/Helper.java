package com.example.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.net.URL;

public class Helper implements Serializable {
    private final static String appName = "Simple Application";

    public static SparkSession createSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master("local")
                .getOrCreate();

        return spark;
    }

    public static JavaSparkContext createJavaSparkContext() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        return sc;
    }

    public static Dataset<String> loadTextFile(SparkSession spark) {
        URL path = Helper.class.getResource("/people.txt");
        Dataset<String> peopleData = spark.read().textFile(path.toString());

        return peopleData;
    }
}
