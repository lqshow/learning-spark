package com.example.spark.rdd.javaRdd;

import com.example.spark.helpers.Utils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class ActionRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/daily_show_guests");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        run(lines);
    }

    static void run(JavaRDD<String> lines) {
        // first（返回第一个元素）
        System.out.println(lines.first());
        /**
         * output
         *
         * 1999,actor,1/11/99,Acting,Michael J. Fox
         */


        // take（用数据集的前n个元素返回一个List）
        List<String> take = lines.take(5);
        take.stream().forEach(System.out::println);
        /**
         * output
         *
         * 1999,actor,1/11/99,Acting,Michael J. Fox
         * 1999,Comedian,1/12/99,Comedy,Sandra Bernhard
         * 1999,television actress,1/13/99,Acting,Tracey Ullman
         * 1999,film actress,1/14/99,Acting,Gillian Anderson
         * 1999,actor,1/18/99,Acting,David Alan Grier
         */



        // collect（返回RDD中的所有元素）
        lines.collect();


        // count(返回RDD中的元素个数）
        long count = lines.count();
        System.out.println("count = " + count);
        /**
         * output
         *
         * count = 2693
         */


        // foreach（loop rdd）
        lines.foreach((VoidFunction) line -> {
//            System.out.println(line);
        });


        // countByValue（各元素在 RDD 中出现的次数 返回{(key1,次数),(key2,次数),…(keyn,次数)} ）
        Map<String, Long> countByValueRes = lines.map((Function<String, String>) line-> {
            String year = StringUtils.split(line, ",")[0].trim();
            return year;
        }).countByValue();

        for (Map.Entry<String, Long> entry : countByValueRes.entrySet()) {
            System.out.println("year = " + entry.getKey() +  ", count=" + entry.getValue());
        }
        /**
         * output
         *
         * year = 2014, count=163
         * year = 2003, count=166
         * year = 2013, count=166
         * year = 2002, count=159
         * year = 2007, count=141
         * year = 2004, count=164
         * ....
         */
    }
}
