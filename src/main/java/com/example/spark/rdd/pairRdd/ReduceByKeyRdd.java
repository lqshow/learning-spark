package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.ParseLine;
import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.net.URL;

public class ReduceByKeyRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/daily_show_guests");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        JavaPairRDD<String, Integer> yearRDD = lines.mapToPair(new ParseLine());

        JavaPairRDD<String, Integer> yearStatisticsRDD = yearRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        yearStatisticsRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
            System.out.println("year: " + tuple._1 + ", total: " + tuple._2);
        });


        JavaPairRDD<String, Integer> ascRdd = yearStatisticsRDD.sortByKey();
        ascRdd.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
            System.out.println("year asc: " + tuple._1 + ", value: " + tuple._2);
        });

        JavaPairRDD<String, Integer> descRdd = yearStatisticsRDD.sortByKey(false);
        descRdd.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
            System.out.println("year desc: " + tuple._1 + ", value: " + tuple._2);
        });
    }
}
