package com.example.spark.rdd.dateFrame.read;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URL;

/**
 * https://github.com/crealytics/spark-excel
 */
public class CreateDataFrameFromExcel {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        URL xlsPath = CreateDataFrameFromExcel.class.getResource("/xls.xls");

        DataFrameReader dsReader = spark
                .read()
                .format("com.crealytics.spark.excel")
                .option("useHeader", "true")
                .option("treatEmptyValuesAsNulls", "true")
                .option("inferSchema", "false")
                .option("addColorColumns", "false");

        Dataset<Row> df = dsReader.load(xlsPath.toString());
        df.show();

        /**
         * output
         *
         * +---+---+---+---+
         * | A1| B1| C1| D1|
         * +---+---+---+---+
         * | A1| B1| C1| D1|
         * | AA|   | MN|DFS|
         * | BB|  D|FSD|   |
         * | CC|   |SDF|SFD|
         * | DD| FS|   |FDS|
         * +---+---+---+---+
         */
    }
}
