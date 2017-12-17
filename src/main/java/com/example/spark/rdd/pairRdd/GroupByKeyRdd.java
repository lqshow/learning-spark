package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.net.URL;
import java.util.Map;
import java.util.List;

/**
 * 对具有相同键的值进行分组
 */
public class GroupByKeyRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/daily_show_guests");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        JavaPairRDD<String, String> pairRDD = lines.mapToPair((PairFunction<String, String, String>) s -> {
            String year = StringUtils.split(s, ",")[0].trim();
            return new Tuple2(year, s);
        });

        // output: (key,Iterable[value])
        JavaPairRDD<String, List<String>> groupByKeyRDD = pairRDD
                .groupByKey()
                .mapValues((Function<Iterable<String>, List<String>>) ite -> Lists.newArrayList(ite));

        Map<String, List<String>> res = groupByKeyRDD.collectAsMap();
        for (Map.Entry<String, List<String>> entry : res.entrySet()) {
            JavaRDD<String> curRdd = jsc.parallelize(entry.getValue());

            curRdd.saveAsTextFile("output/daily_show_guests/" + entry.getKey());
        }
    }
}
