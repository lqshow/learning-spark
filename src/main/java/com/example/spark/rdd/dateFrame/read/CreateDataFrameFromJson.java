package com.example.spark.rdd.dateFrame.read;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URL;

public class CreateDataFrameFromJson {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        URL josnPath = CreateDataFrameFromExcel.class.getResource("/people.json");

        // Dataset<Row> df = spark.read().json(josnPath.toString());
        Dataset<Row> df = spark
                .read()
                .format("json")
                .load(josnPath.toString());


        df.printSchema();
        /**
         * output
         *
         * root
         * |-- age: string (nullable = true)
         * |-- name: string (nullable = true)
         */



        df.show();
        /**
         * output
         *
         * +---+-------+
         * |age|   name|
         * +---+-------+
         * | 30|Michael|
         * | 30|   Andy|
         * | 19| Justin|
         * +---+-------+
         */
    }
}
