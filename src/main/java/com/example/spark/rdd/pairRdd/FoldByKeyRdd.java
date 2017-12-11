package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.ParseLine;
import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.net.URL;

public class FoldByKeyRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/daily_show_guests");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        JavaPairRDD<String, Integer> yearRDD = lines.mapToPair(new ParseLine());

        JavaPairRDD<String, Integer> yearStatisticsRDD = yearRDD.foldByKey(10,
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        yearStatisticsRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
            System.out.println("year: " + tuple._1 + ", total: " + tuple._2);
        });
    }
}
