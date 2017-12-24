package com.example.spark.rdd;

import com.example.spark.helpers.Utils;
import com.example.spark.beans.Person;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class CreateDataset {
    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDataset.class);

    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        stringDataset(spark);
        personDataset(spark);
        personDatasetViaTextFile(spark);
        personDatasetViaDataFrame(spark);
        tupleDataset(spark);
    }

    /**
     * Encoders for most common types are provided in class Encoders
     *
     * @param spark
     */
    static void stringDataset(SparkSession spark) {
        List<String> data = Arrays.asList("pandas", "i like pandas");
        Dataset<String> lines = spark.createDataset(data, Encoders.STRING());

        lines.show();

        /**
         *  output
         *
         * +-------------+
         * |        value|
         * +-------------+
         * |       pandas|
         * |i like pandas|
         * +-------------+
         */

        String[] arrLines = (String[]) lines.collect();
        for (String line : arrLines) {
            LOGGER.info(line);
        }

        /**
         *  output
         *
         * pandas
         * i like pandas
         */
    }

    static void personDataset(SparkSession spark) {

        // Create an instance of a Bean class
        List<Person> personList = Arrays.asList(
                new Person("Michael", 29),
                new Person("Andy", 30),
                new Person("Justin", 18)
        );

        // Encoders are created for Java beans
        Dataset<Person> javaBeanDS = spark.createDataset(personList, Encoders.bean(Person.class));

        javaBeanDS.show();

        /**
         * output
         *
         * +---+-------+
         * |age|   name|
         * +---+-------+
         * | 29|Michael|
         * | 30|   Andy|
         * | 18| Justin|
         * +---+-------+
         */
    }

    static void personDatasetViaTextFile(SparkSession spark) {
        Dataset<String> peopleData = Utils.datasetFromTextFile(spark);

        Dataset<Person> peopleDS = peopleData
                .map((MapFunction<String, Person>) line -> {
                    String[] parts = line.split(",");
                    Person person = new Person(parts[0], Integer.parseInt(parts[1].trim()));
                    return person;
                }, Encoders.bean(Person.class));
        peopleDS.show();

        /**
         * output
         *
         * +---+-------+
         * |age|   name|
         * +---+-------+
         * | 29|Michael|
         * | 30|   Andy|
         * | 19| Justin|
         * +---+-------+
         */
    }

    /**
     * DataFrames can be converted to a Dataset by providing a class. Mapping based on name
     *
     * @param spark
     */
    static void personDatasetViaDataFrame(SparkSession spark) {
        Dataset<Row> df = spark
                .read()
                .format("json")
                .load("src/main/resources/people.json");

        Dataset<Person> peopleDS = df.as(Encoders.bean(Person.class));
        peopleDS.show();

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

    static void tupleDataset(SparkSession spark) {
        Encoder<Tuple2<Integer, String>> encoder2 = Encoders.tuple(Encoders.INT(), Encoders.STRING());
        List<Tuple2<Integer, String>> data2 = Arrays.asList(new scala.Tuple2(1, "a"));
        Dataset<Tuple2<Integer, String>> ds2 = spark.createDataset(data2, encoder2);

        ds2.show();

        /***
         * output
         *
         * +---+---+
         * | _1| _2|
         * +---+---+
         * |  1|  a|
         * +---+---+
         */
    }
}
