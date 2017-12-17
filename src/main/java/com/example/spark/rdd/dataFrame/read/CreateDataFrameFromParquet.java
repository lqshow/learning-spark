package com.example.spark.rdd.dataFrame.read;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

import java.net.URL;
import java.util.Arrays;

/**
 * parquet列式存储
 */
public class CreateDataFrameFromParquet {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        URL parquetPath = CreateDataFrameFromExcel.class.getResource("/users.parquet");

        // Dataset<Row> df = spark.read().parquet(parquetPath.toString());
        Dataset<Row> df = spark
                .read()
                .format("parquet")
                .load(parquetPath.toString());

        Arrays.asList(df.columns()).stream().forEach(System.out::println);
        /**
         * columns output
         *
         * name
         * favorite_color
         * favorite_numbers
         */

        df.show();
        /**
         * output
         *
         * +------+--------------+----------------+
         * |  name|favorite_color|favorite_numbers|
         * +------+--------------+----------------+
         * |Alyssa|          null|  [3, 9, 15, 20]|
         * |   Ben|           red|              []|
         * +------+--------------+----------------+
         */



        df.select(col("name"), col("favorite_color")).show();
        /**
         * output
         * +------+--------------+
         * |  name|favorite_color|
         * +------+--------------+
         * |Alyssa|          null|
         * |   Ben|           red|
         * +------+--------------+
         */
    }
}
