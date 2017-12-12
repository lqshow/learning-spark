package com.example.spark.rdd.dateFrame.read;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URL;

/**
 * https://github.com/databricks/spark-csv
 */
public class CreateDataFrameFromCsv {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        URL csvPath = CreateDataFrameFromExcel.class.getResource("/csv.csv");

        Dataset<Row> df = spark
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(csvPath.toString());

        df.printSchema();
        /**
         * schema output
         *
         * root
         * |-- A1: string (nullable = true)
         * |-- B1: string (nullable = true)
         * |-- C1: string (nullable = true)
         * |-- D1: string (nullable = true)
         */


        df.show();
        /**
         * output
         * +---+----+----+----+
         * | A1|  B1|  C1|  D1|
         * +---+----+----+----+
         * | A1|  B1|  C1|  D1|
         * | AA|null|  MN| DFS|
         * | BB|   D| FSD|null|
         * | CC|null| SDF| SFD|
         * | DD|  FS|null| FDS|
         * +---+----+----+----+
         */

        df.select("A1").show();
        /**
         *
         * +---+
         * | A1|
         * +---+
         * | A1|
         * | AA|
         * | BB|
         * | CC|
         * | DD|
         * +---+
         */
    }
}
