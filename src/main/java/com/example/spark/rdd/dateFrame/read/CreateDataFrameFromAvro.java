package com.example.spark.rdd.dateFrame.read;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URL;

/**
 * https://github.com/databricks/spark-avro
 */
public class CreateDataFrameFromAvro {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        URL avroPath = CreateDataFrameFromExcel.class.getResource("/users.avro");

        Dataset<Row> df = spark
                .read()
                .format("com.databricks.spark.avro")
                .load(avroPath.toString());

        df.printSchema();

        /**
         * schema output
         *
         * root
         * |-- name: string (nullable = true)
         * |-- favorite_color: string (nullable = true)
         * |-- favorite_numbers: array (nullable = true)
         * |    |-- element: integer (containsNull = true)
         */

        //TODO: print
    }
}
