package com.example.spark.app;

import com.example.spark.helpers.Pagination;
import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


import java.net.URL;
import java.util.Iterator;

public class RddPaginationExample {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: RddPaginationExample <offset> <limit>");
            System.exit(1);
        }
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        // load rdd
        URL path = ReduceRdd.class.getResource("/README.md");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        long offset = Long.valueOf(args[0].trim());
        long limit = Long.valueOf(args[1].trim());
        Pagination pagination = new Pagination(offset, limit);

        JavaPairRDD<String, Long> pairRDD = lines.zipWithIndex();
        JavaPairRDD<String, Long> currentRDD = pagination.rddPagination(pairRDD);

        for (Iterator<Tuple2<String, Long>> iter = currentRDD.collect().iterator(); iter.hasNext(); ) {
            Tuple2<String, Long> curData = iter.next();
            String line = curData._1();
            System.out.println(line);
        }
    }
}
