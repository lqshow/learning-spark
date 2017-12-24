package com.example.spark.sql;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// $example on:untyped_ops$
// col("...") is preferable to df.col("...")
import static org.apache.spark.sql.functions.col;
// $example off:untyped_ops$


public class BasicDataFrame {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Dataset<Row> df = spark
                .read()
                .format("json")
                .load("src/main/resources/people.json");


        df.select(
                col("name").alias("bb_name"),
                col("age").plus(1).name("age")
        ).show();

        /**
         * output
         *
         * +-------+----+
         * |bb_name| age|
         * +-------+----+
         * |Michael|null|
         * |   Andy|  31|
         * | Justin|  20|
         * +-------+----+
         */

        df.filter(col("age").gt(20)).show();

        /**
         * output
         *
         * +---+----+
         * |age|name|
         * +---+----+
         * | 30|Andy|
         * +---+----+
         */


        Dataset<String> nameByField = df.map(
                (MapFunction<Row, String>) row -> row.getAs("name"),
                Encoders.STRING()
        );
        Dataset<String> nameByIndex = df.map(
                (MapFunction<Row, String>) row -> row.getString(1),
                Encoders.STRING()
        );
        nameByField.show();
        /**
         * output
         *
         * +-------+
         * |  value|
         * +-------+
         * |Michael|
         * |   Andy|
         * | Justin|
         * +-------+
         */
    }
}
