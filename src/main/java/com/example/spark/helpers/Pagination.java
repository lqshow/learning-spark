package com.example.spark.helpers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class Pagination {
    private static long offset;
    private static long limit;

    public Pagination(long offset, long limit) {
        this.offset = offset;
        this.limit = limit;
    }

    public static JavaPairRDD<String, Long> rddPagination(JavaPairRDD<String, Long> pairRdd) {
        JavaPairRDD<String, Long> currentRDD = pairRdd.filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> rowLongTuple2) throws Exception {
                return rowLongTuple2._2() >= offset && rowLongTuple2._2() < (limit + offset);
            }
        });

        return currentRDD;
    }
}
