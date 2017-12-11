package com.example.spark.rdd.javaRdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapPartitionsWithIndexRdd {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();
        JavaRDD<String> peopleData =  Utils.javaRDDFromTextFile(spark);


        JavaRDD<Tuple2<String, Integer>> mapPartitionsRDD = peopleData.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<Tuple2<String, Integer>>>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(Integer partIndex, Iterator<String> lines) throws Exception {
                List<Tuple2<String, Integer>> lowerCaseLines = new ArrayList<>();
                while (lines.hasNext()) {
                    String line = lines.next();
                    lowerCaseLines.add(new Tuple2<>(line.toUpperCase(), partIndex));
                }

                return lowerCaseLines.iterator();
            }
        }, false);

        mapPartitionsRDD.collect().stream().forEach(System.out::println);

    }
}
