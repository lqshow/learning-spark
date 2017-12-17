package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 返回一个仅包含键的RDD
 */
public class keysRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        JavaPairRDD<String, Integer> pairRDD = Utils.createJavaPairRddDemo(jsc);

        pairRDD.keys().collect().stream().forEach(System.out::println);

        /**
         * output
         *
         * coffee
         * coffee
         * pandas
         */
    }
}
