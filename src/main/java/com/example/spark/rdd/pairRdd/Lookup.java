package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 返回给定键对应的所有值List<T>
 */
public class Lookup {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());
        JavaPairRDD<String, Integer> pairRDD = Utils.createJavaPairRddDemo(jsc);

        pairRDD.lookup("coffee").forEach(System.out::println);

        /**
         * output
         *
         * 1
         * 2
         */
    }
}
