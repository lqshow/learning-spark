package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.net.URL;

public class CreateJavaPairRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        parallelizePairsRdd(jsc);
        viaMapToPair(jsc);
    }

    static void parallelizePairsRdd(JavaSparkContext jsc) {
        JavaPairRDD<String, Integer> pairRDD = Utils.createJavaPairRddDemo(jsc);

        pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
            System.out.println("key: " + tuple._1 + ", value: " + tuple._2);
        });

        /**
         * output
         *
         * key: coffee, value: 1
         * key: coffee, value: 2
         * key: pandas, value: 3
         */
    }

    static void viaMapToPair(JavaSparkContext jsc) {
        URL path = ReduceRdd.class.getResource("/people.txt");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        JavaPairRDD<String, String> pairRDD = lines.mapToPair((PairFunction<String, String, String>) (line) -> {
            String key = StringUtils.split(line, ",")[0].trim();
            return new Tuple2(key, line);
        });

        pairRDD.foreach((VoidFunction<Tuple2<String, String>>) tuple -> {
            System.out.println("key: " + tuple._1 + ", value: " + tuple._2);
        });

        /**
         * output
         *
         * key: Michael, value: Michael, 29
         * key: Andy, value: Andy, 30
         * key: Justin, value: Justin, 19
         */
    }
}
