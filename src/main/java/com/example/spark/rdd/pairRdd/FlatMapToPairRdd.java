package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class FlatMapToPairRdd {

    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/people.txt");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        // input: String
        // output: (String, Integer)
        JavaPairRDD<String, Integer> pairRDD = lines.flatMapToPair((PairFlatMapFunction<String, String, Integer>) s -> {
            List<Tuple2<String, Integer>> tupleList = new ArrayList<>();
            String[] arrData = StringUtils.split(s, ",");
            for (String data : arrData) {
                tupleList.add(new Tuple2(data, 1));
            }

            return tupleList.iterator();
        });

        pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
            System.out.println("key: " + tuple._1 + ", value: " + tuple._2);
        });

        /**
         * output
         *
         * key: Michael, value: 1
         * key:  29, value: 1
         * key: Andy, value: 1
         * key:  30, value: 1
         * key: Justin, value: 1
         * key:  19, value: 1
         */

    }
}
