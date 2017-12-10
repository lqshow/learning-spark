package com.example.spark.rdd.javaRdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.URL;

public class ReduceRdd {

    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/README.md");
        JavaRDD<String> lines = jsc.textFile(path.toString());
        JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);

        System.out.println("totalLength: " + totalLength);
    }
}
