package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.ParseLine;
import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.net.URL;
import java.util.Map;

/**
 * 对每个键对应的元素分别计数
 */
public class CountByKeyRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/daily_show_guests");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        JavaPairRDD<String, Integer> yearRDD = lines.mapToPair(new ParseLine());

        Map<String, Long> countByKeyRDD = yearRDD.countByKey();
        for (String year: countByKeyRDD.keySet()) {
            System.out.println("year = " + year +  ", count=" + countByKeyRDD.get(year));
        }

        /**
         * output
         *
         * year = 2014, count=163
         * year = 2003, count=166
         * year = 2013, count=166
         * year = 2002, count=159
         * year = 2007, count=141
         * year = 2004, count=164
         * ....
         */
    }
}
