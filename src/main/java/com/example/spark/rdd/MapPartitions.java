package com.example.spark.rdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapPartitions {

    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();
        Dataset<String> peopleData = Utils.datasetFromTextFile(spark);

        mapPartitionsRdd(peopleData);
    }

    static void mapPartitionsRdd(Dataset<String> peopleData) {
        /**
         * 先partition，再把每个partition进行map函数
         */
        Dataset<String> mapPartitionsRDD = peopleData.mapPartitions(new MapPartitionsFunction<String, String>() {
            @Override
            public Iterator<String> call(Iterator<String> lines) throws Exception {
                List<String> lowerCaseLines = new ArrayList<String>();
                while (lines.hasNext()) {
                    String line = lines.next();
                    lowerCaseLines.add(line.toUpperCase());
                }
                return lowerCaseLines.iterator();
            }
        }, Encoders.STRING());

        mapPartitionsRDD.show();

        /**
         * output
         *
         * |      value|
         *  +-----------+
         * |MICHAEL, 29|
         * |   ANDY, 30|
         * | JUSTIN, 19|
         * +-----------+
         */
    }

}
