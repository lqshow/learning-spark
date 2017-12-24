package com.example.spark.rdd.dataFrame;

import com.example.spark.beans.Person;
import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateDataFrame {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Dataset<String> peopleData = Utils.datasetFromTextFile(spark);

        viaRDD(spark, peopleData);
        viaRowRDD(spark, peopleData);
    }

    static void viaRDD(SparkSession spark, Dataset<String> peopleData) {
        Dataset<Person> peopleDS = peopleData
                .map((MapFunction<String, Person>) line -> {
                    String[] parts = line.split(",");
                    Person person = new Person(parts[0], Integer.parseInt(parts[1].trim()));
                    return person;
                }, Encoders.bean(Person.class));

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> peopleDF = spark.createDataFrame(peopleDS.toJavaRDD(), Person.class);
        peopleDF.printSchema();

        /**
         * output
         *
         * root
         * |-- age: integer (nullable = false)
         * |-- name: string (nullable = true)
         */
    }

    static void viaRowRDD(SparkSession spark, Dataset<String> peopleData) {
        String[] columns = new String[]{"name", "age"};
        int columnsLen = columns.length;

        StructField[] fields = new StructField[columnsLen];
        for (int i = 0; i < columnsLen; i++) {
            fields[i] = DataTypes.createStructField(columns[i], DataTypes.StringType, true);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleData.toJavaRDD()
                .map(line -> {
                    String[] attributes = line.split(",");
                    return RowFactory.create(attributes);
                });

        // Apply the schema to the RDD
        Dataset<Row> peopleDataFrame = spark.createDataFrame(rowRDD, schema);
        peopleDataFrame.printSchema();

        /**
         * output
         *
         * root
         * |-- name: string (nullable = true)
         * |-- age: string (nullable = true)
         */
    }
}
