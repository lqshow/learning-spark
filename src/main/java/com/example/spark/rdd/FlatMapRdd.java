package com.example.spark.rdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Iterator;

public class FlatMapRdd {

    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();
        Dataset<String> peopleData = Utils.datasetFromTextFile(spark);

        flatMappRdd(peopleData);
    }


    static void flatMappRdd(Dataset<String> peopleData) {
        Dataset<String> flatMapRdd = peopleData.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split(",");
                return Arrays.asList(split).iterator();
            }
        }, Encoders.STRING());

        flatMapRdd.show();

        /**
         * output
         *
         +-------+
         |  value|
         +-------+
         |Michael|
         |     29|
         |   Andy|
         |     30|
         | Justin|
         |     19|
         +-------+
         */

        flatMapRdd.foreach(new ForeachFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
