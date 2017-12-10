package com.example.spark.rdd;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterRdd {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterRdd.class);


    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Dataset<String> peopleData = Utils.datasetFromTextFile(spark);

        filterYouth(peopleData);
        filterMiddleAge(peopleData);
    }

    static void filterYouth(Dataset<String> peopleData) {
        Dataset<String> youthRdd = peopleData.filter(new FilterFunction<String>() {
            @Override
            public boolean call(String s) throws Exception {
                String[] data = s.split(",");
                int age = Integer.valueOf(data[1].trim());
                return age < 30;
            }
        });
        youthRdd.show();

        /**
         * output
         *
         +-----------+
         |      value|
         +-----------+
         |Michael, 29|
         | Justin, 19|
         +-----------+
         */
    }

    static void filterMiddleAge(Dataset<String> peopleData) {
        Dataset<String> middleAgeRdd = peopleData.filter((FilterFunction<String>) s -> {
            String[] data = s.split(",");
            int age = Integer.valueOf(data[1].trim());
            return age >= 30;
        });

        middleAgeRdd.show();

        /**
         * output
         *
         +--------+
         |   value|
         +--------+
         |Andy, 30|
         +--------+
         */
    }
}
