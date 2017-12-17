package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 返回一个仅包含值的RDD
 */
public class ValuesRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        JavaPairRDD<String, Integer> pairRDD = Utils.createJavaPairRddDemo(jsc);

        pairRDD.values().collect().stream().forEach(System.out::println);

        /**
         * output
         *
         * 1
         * 2
         * 3
         */
    }
}
