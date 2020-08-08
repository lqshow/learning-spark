package com.example.spark.helpers;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class Utils implements Serializable {
    private final static String appName = "Simple Application";

    public static SparkSession createSparkSession() {
        SparkSession spark = SparkSession
                .builder()
                .appName(appName)
                .master("local")
                .getOrCreate();

        return spark;
    }

    public static JavaSparkContext createJavaSparkContext(SparkSession spark) {
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        return jsc;
    }

    public static JavaSparkContext createJavaSparkContext() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster("local");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        return jsc;
    }

    public static Dataset<String> datasetFromTextFile(SparkSession spark) {
        URL path = Utils.class.getResource("/people.txt");
        Dataset<String> peopleData = spark.read().textFile(path.toString());

        return peopleData;
    }

    public static JavaRDD<String> javaRDDFromTextFile(SparkSession spark) {
        URL path = Utils.class.getResource("/people.txt");
        JavaRDD<String> peopleData = spark.read().textFile(path.toString()).javaRDD();

        return peopleData;
    }

    public static JavaPairRDD<String, Integer> createJavaPairRddDemo(JavaSparkContext jsc) {
        List<Tuple2<String, Integer>> input = new ArrayList();
        input.add(new Tuple2("coffee", 1));
        input.add(new Tuple2("coffee", 2));
        input.add(new Tuple2("pandas", 3));


        JavaPairRDD<String, Integer> pairRDD = jsc.parallelizePairs(input);

        return pairRDD;
    }

    public static void registerDailyShowGuestsView(SparkSession spark) {
        String[] columns = new String[]{"year", "occupation", "show", "group", "raw_guest"};
        int columnsLen = columns.length;
        StructField[] fields = new StructField[columnsLen];
        for (int i = 0; i < columnsLen; i++) {
            fields[i] = DataTypes.createStructField(columns[i], DataTypes.StringType, true);
        }
        StructType schema = DataTypes.createStructType(fields);

        Dataset<String> data = spark.read().textFile("src/main/resources/daily_show_guests");
        JavaRDD<Row> rowRDD = data.toJavaRDD()
                .map(line -> {
                    String[] attributes = line.split(",");
                    return RowFactory.create(attributes);
                });
        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);

        dataFrame.createOrReplaceTempView("daily_show_guests");
    }

    public static void registerEmployeeView(SparkSession spark) {
        Dataset<Row> df = spark
                .read()
                .format("json")
                .load("src/main/resources/file/window_functions_data.json");

        df.createOrReplaceTempView("empsalary");
    }
}
