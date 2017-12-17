package com.example.spark.rdd.dataFrame.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.SparkSession;

/**
 *
 * dependency: https://mvnrepository.com/artifact/mysql/mysql-connector-java
 */
public class CreateDataFrameFromJdbc {
    public static void main(String[] args) throws ClassNotFoundException{
        SparkSession spark = Utils.createSparkSession();

        Dataset<Row> df = spark
                .read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/sys")
                .option("driver", "com.mysql.jdbc.Driver")
                .option("dbtable", "host_summary_by_file_io_type")
                .option("user", "root")
                .option("password", "")
                .load();

        df.printSchema();
        /**
         *
         * root
         * |-- host: string (nullable = true)
         * |-- event_name: string (nullable = false)
         * |-- total: decimal(20,0) (nullable = false)
         * |-- total_latency: string (nullable = true)
         * |-- max_latency: string (nullable = true)
         */

        df.show();
        /**
         *
         * +----------+--------------------+-----+-------------+-----------+
         * |      host|          event_name|total|total_latency|max_latency|
         * +----------+--------------------+-----+-------------+-----------+
         * |background|wait/io/file/sql/FRM| 3802|    249.50 ms|    4.26 ms|
         * |background|wait/io/file/inno...| 1091|    192.58 ms|   31.55 ms|
         * |background|wait/io/file/sql/...|    5|     12.02 ms|   10.38 ms|
         * |background|wait/io/file/sql/...|  200|      8.49 ms|  362.51 us|
         * ....
         */
        df.write().parquet("output/parquet");
    }
}
