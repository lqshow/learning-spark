package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.ParseLine;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.commons.lang3.StringUtils;
import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.net.URL;

/**
 * 创建JavaPairRDD
 */
public class MapToPairRdd {

    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/people.txt");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        // input: String
        // output: (String, Integer)
        JavaPairRDD<String, Integer> pairRDD = lines.mapToPair(new ParseLine());

        pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
            System.out.println("key: " + tuple._1 + ", value: " + tuple._2);
        });

        /**
         * output
         *
         * name: Michael, value: 1
         * name: Andy, value: 1
         * name: Justin, value: 1
         */
    }
}
