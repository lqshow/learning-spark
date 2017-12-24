package com.example.spark.rdd.dataFrame.read;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RunSqlOnFile {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`");
        sqlDF.printSchema();
        /**
         * output
         *
         * root
         * |-- name: string (nullable = true)
         * |-- favorite_color: string (nullable = true)
         * |-- favorite_numbers: array (nullable = true)
         * |    |-- element: integer (containsNull = true)
         */

        sqlDF.show();
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
    }
}
