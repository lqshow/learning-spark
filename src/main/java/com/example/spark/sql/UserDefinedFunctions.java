package com.example.spark.sql;

import com.example.spark.helpers.Utils;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.lit;

/**
 * Reference:
 * - https://sparkour.urizone.net/recipes/using-sql-udf/
 * - https://www.jianshu.com/p/833b72adb2b6
 */
public class UserDefinedFunctions {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Dataset<Row> df = spark
                .read()
                .format("json")
                .load("src/main/resources/people.json");

        udfUpperCase(spark, df);
        udfStrLen(spark, df);
        udfGetJsonObject(spark);
    }

    static void udfUpperCase(SparkSession spark, Dataset<Row> df) {
        // Define the UDF
        spark.udf().register(
                "udfUppercase",
                (String string) -> string.toUpperCase(),
                DataTypes.StringType
        );

        // Convert a whole column to uppercase with a UDF.
        Dataset<Row> newDf = df.withColumn("name", callUDF("udfUppercase", df.col("name")));
        newDf.show();
        /**
         * output
         *
         * +----+-------+
         * | age|   name|
         * +----+-------+
         * |null|MICHAEL|
         * |  30|   ANDY|
         * |  19| JUSTIN|
         * +----+-------+
         */


        df.select(callUDF("udfUppercase", col("name"))).show();
        df.selectExpr("udfUppercase(name)").show();
        /**
         * output
         *
         * +---------+
         * |UDF(name)|
         * +---------+
         * |  MICHAEL|
         * |     ANDY|
         * |   JUSTIN|
         * +---------+
         */

        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT age, udfUppercase(name) as upperCaseName FROM people");
        sqlDF.show();
        /**
         * output
         *
         * +----+-------------+
         * | age|upperCaseName|
         * +----+-------------+
         * |null|      MICHAEL|
         * |  30|         ANDY|
         * |  19|       JUSTIN|
         * +----+-------------+
         */

    }

    static void udfStrLen(SparkSession spark, Dataset<Row> df) {
        // Define the UDF
        spark.udf().register("strLen", (String string) -> string.length(), DataTypes.IntegerType);

        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT strLen(name), length(name) FROM people");
        sqlDF.show();
        /**
         * output
         *
         * +---------+------------+
         * |UDF(name)|length(name)|
         * +---------+------------+
         * |        7|           7|
         * |        4|           4|
         * |        6|           6|
         * +---------+------------+
         */
    }

    static void udfGetJsonObject(SparkSession spark) {
        Dataset<String> peopleData = spark.read().textFile("src/main/resources/people.json");
        JavaRDD<Row> rowRDD = peopleData
                .toJavaRDD()
                .map(line -> RowFactory.create(line));

//        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD,
//                new StructType(
//                        new StructField[]{
//                                DataTypes.createStructField("data", DataTypes.StringType, true)
//                        })
//        );

        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD,
                DataTypes.createStructType(
                        Arrays.asList(
                                DataTypes.createStructField("data", DataTypes.StringType, true)
                        )
                )
        );

        // Define the UDF
        spark.udf().register("getJsonObject", (String string, String field) -> {
            String data = "";
            try {
                JsonParser parser = new JsonParser();
                JsonObject object = parser.parse(string).getAsJsonObject();
                if (object.has(field)) {
                    data = object.get(field).getAsString();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return data;
        }, DataTypes.StringType);


        peopleDataFrame.select(callUDF("getJsonObject", col("data"), lit("name"))).show();
        /**
         * output
         *
         * +---------------+
         * |UDF(data, name)|
         * +---------------+
         * |        Michael|
         * |           Andy|
         * |         Justin|
         * +---------------+
         */


        peopleDataFrame.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("SELECT data, getJsonObject(data, 'name') FROM people");
        sqlDF.show();
        /**
         * output
         *
         * +--------------------+---------------+
         * |                data|UDF(data, name)|
         * +--------------------+---------------+
         * |{"name":"Michael"...|        Michael|
         * |{"name":"Andy", "...|           Andy|
         * |{"name":"Justin",...|         Justin|
         * +--------------------+---------------+
         */
    }
}
