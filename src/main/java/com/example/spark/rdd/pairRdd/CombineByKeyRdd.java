package com.example.spark.rdd.pairRdd;

import com.example.spark.beans.AvgCount;
import com.example.spark.helpers.Utils;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Map;

/**
 * 使用不同的返回类型合并具有相同键的值(可以返回与输入数据的类型不同的返回值)
 * <p>
 * combineByKey遍历分区中的所有元素
 */
public class CombineByKeyRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());
        run(jsc);
    }

    static void run(JavaSparkContext jsc) {
        JavaPairRDD<String, Integer> pairRDD = Utils.createJavaPairRddDemo(jsc);

        /**
         * createCombiner
         * 创建键对应的累加器的初始值
         */
        Function<Integer, AvgCount> createAcc = (value) -> new AvgCount(value, 1);


        /**
         * mergerValue
         *
         * 聚合各分区中的元素
         *
         * 将该键的累加器对应的当前值与这个新的值进行合并
         */
        Function2<AvgCount, Integer, AvgCount> addAndCount = (acc, value) -> {
            acc.total += value;
            acc.num += 1;
            return acc;
        };

        /**
         * mergeCombiners
         *
         * 将所有分区的聚合结果再次聚合
         */
        Function2<AvgCount, AvgCount, AvgCount> combine = (acc1, acc2) -> {
            acc1.total += acc2.total;
            acc1.num += acc2.num;
            return acc1;
        };

        JavaPairRDD<String, AvgCount> avgCounts = pairRDD.combineByKey(createAcc, addAndCount, combine);

        Map<String, AvgCount> countMap = avgCounts.collectAsMap();
        for (Map.Entry<String, AvgCount> entry : countMap.entrySet()) {
            String output = StringUtils.format("key: %s, avg: %f", entry.getKey(), entry.getValue().avg());

            System.out.println(output);
        }

        /**
         * output
         *
         * key: pandas, avg: 3.000000
         * key: coffee, avg: 1.500000
         */
    }
}
