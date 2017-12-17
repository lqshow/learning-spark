package com.example.spark.rdd.javaRdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TransformationRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        List<List<?>> data1 = Arrays.asList(
                Arrays.asList(1,"jan",2016),
                Arrays.asList(3,"nov",2014),
                Arrays.asList(1,"jan",2016),
                Arrays.asList(16,"feb",2014)
        );

        List<List<?>> data2 = Arrays.asList(
                Arrays.asList(3,"nov",2014),
                Arrays.asList(17,"sep",2015)
        );

        List<List<?>> data3 = Arrays.asList(
                Arrays.asList(16,"may",2017)
        );

        JavaRDD<List<?>> rdd1 = jsc.parallelize(data1);
        JavaRDD<List<?>> rdd2 = jsc.parallelize(data2);
        JavaRDD<List<?>> rdd3 = jsc.parallelize(data3);

        union(rdd1, rdd2, rdd3);
        intersection(rdd1, rdd2);//开销大（通过网络混洗数据来发现共有元素）
        distinct(rdd1);//开销大（将所有数据通过网络进行混洗）
        subtract(rdd1, rdd2);
    }

    /**
     * 合并RDD
     * @param rdd1
     * @param rdd2
     * @param rdd3
     */
    static void union(JavaRDD<List<?>> rdd1, JavaRDD<List<?>> rdd2, JavaRDD<List<?>> rdd3) {
        JavaRDD<List<?>> unionRDD = rdd1.union(rdd2).union(rdd3);
        unionRDD.collect().stream().forEach(System.out::println);

        /**
         * output
         *
         * [1, jan, 2016]
         * [3, nov, 2014]
         * [1, jan, 2016]
         * [16, feb, 2014]
         * [3, nov, 2014]
         * [17, sep, 2015]
         * [16, may, 2017]
         */
    }

    /**
     * RDD交集
     * @param rdd1
     * @param rdd2
     */
    static void intersection(JavaRDD<List<?>> rdd1, JavaRDD<List<?>> rdd2) {
        JavaRDD<List<?>> intersectionRDD = rdd1.intersection(rdd2);
        intersectionRDD.collect().stream().forEach(System.out::println);

        /**
         * outout
         *
         * [3, nov, 2014]
         */
    }

    /**
     * 去重
     * @param rdd
     */
    static void distinct(JavaRDD<List<?>> rdd) {

        rdd.collect().stream().forEach(System.out::println);
        /**
         * output
         *
         * [1, jan, 2016]
         * [3, nov, 2014]
         * [1, jan, 2016]
         * [16, feb, 2014]
         */


        rdd.distinct().collect().stream().forEach(System.out::println);

        /**
         * output
         *
         * [3, nov, 2014]
         * [1, jan, 2016]
         * [16, feb, 2014]
         */
    }

    /**
     * Return an RDD with the elements from `this` that are not in `other`.
     * @param rdd1
     * @param rdd2
     */
    static void subtract(JavaRDD<List<?>> rdd1, JavaRDD<List<?>> rdd2) {
        rdd1.subtract(rdd2).collect().stream().forEach(System.out::println);

        /**
         * output
         *
         * [16, feb, 2014]
         * [1, jan, 2016]
         * [1, jan, 2016]
         */
    }
}
