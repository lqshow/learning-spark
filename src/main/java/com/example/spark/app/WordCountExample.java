package com.example.spark.app;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.net.URL;
import java.util.Arrays;

public class WordCountExample {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        // load rdd
        URL path = WordCountExample.class.getResource("/README.md");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        JavaPairRDD<String, Integer> counts = lines
                .flatMap(line -> {
                    String[] split = line.split(" ");
                    return Arrays.asList(split).iterator();
                })  // words
                .mapToPair(word -> new Tuple2<>(word, 1)) // ones
                .reduceByKey((a1, a2) -> a1 + a2);  // counts

        counts.foreach(tuple -> {
            System.out.println(tuple._1() + " = " + tuple._2());
        });

        /**
         * output
         *
         * package = 1
         * For = 3
         * Programs = 1
         * processing. = 1
         * Because = 1
         * The = 1
         * .....
         */
    }
}
