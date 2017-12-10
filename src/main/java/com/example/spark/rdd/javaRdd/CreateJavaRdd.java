package com.example.spark.rdd.javaRdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class CreateJavaRdd {

    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        parallelizeRdd(jsc);
    }

    static void parallelizeRdd(JavaSparkContext jsc) {
        List<String> data = Arrays.asList("pandas", "i like pandas");
        JavaRDD<String> lines = jsc.parallelize(data);

        List<String> list = lines.collect();
        list.stream().forEach(System.out::println);


        /**
         * output
         *
         * pandas
         * i like pandas
         */
    }
}
