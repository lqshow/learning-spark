package com.example.spark.sql;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Running SQL Queries Programmatically
 */
public class RegisterTemporaryView {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = Utils.createSparkSession();

        Dataset<Row> df = spark
                .read()
                .format("json")
                .load("src/main/resources/people.json");

        createOrReplaceTempView(spark, df);
        createGlobalTempView(spark, df);
    }

    static void createOrReplaceTempView(SparkSession spark, Dataset<Row> df) {
        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("people");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");

        sqlDF.show();

        /**
         * output
         *
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|Michael|
         * |  30|   Andy|
         * |  19| Justin|
         * +----+-------+
         */
    }

    static void createGlobalTempView(SparkSession spark, Dataset<Row> df) throws AnalysisException {
        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM global_temp.people");
        sqlDF.show();

        /**
         * output
         *
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|Michael|
         * |  30|   Andy|
         * |  19| Justin|
         * +----+-------+
         */

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();

        /**
         * output
         *
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|Michael|
         * |  30|   Andy|
         * |  19| Justin|
         * +----+-------+
         */
    }
}
