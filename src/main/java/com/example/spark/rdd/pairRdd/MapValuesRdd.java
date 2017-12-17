package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.Utils;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;


/**
 * 对pairRdd中的每个值应用一个函数而不改变键值
 */
public class MapValuesRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        JavaPairRDD<String, Integer> pairRDD = Utils.createJavaPairRddDemo(jsc);

        pairRDD.mapValues((Function<Integer, Integer>) (ele) -> ele * 10)
                .foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
                    String output = StringUtils.format("key: %s, value: %d", tuple._1, tuple._2);
                    System.out.println(output);
                });

        /**
         * output
         *
         * key: coffee, value: 10
         * key: coffee, value: 20
         * key: pandas, value: 30
         */
    }
}
