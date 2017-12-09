package com.example.spark.rdd;

import com.example.spark.Helper;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;


public class MapRdd {

    public static void main(String[] args) {
        SparkSession spark = Helper.createSparkSession();

        Dataset<String> peopleData = Helper.loadTextFile(spark);

        appendAddress(peopleData);
        getAges(peopleData);
    }

    static void appendAddress(Dataset<String> peopleData) {
        String[] address = new String[]{"beijing", "shanghai", "chongqing"};

        Dataset<String> appendAddressRDD = peopleData.map(new MapFunction<String, String>() {
            int i = 0;

            @Override
            public String call(String s) throws Exception {
                return s + ", " + address[i++];
            }
        }, Encoders.STRING());

        appendAddressRDD.show();

        /**
         * output
         *
         +--------------------+
         |               value|
         +--------------------+
         |Michael, 29, beijing|
         |  Andy, 30, shanghai|
         |Justin, 19, chong...|
         +--------------------+
         */
    }

    static void getAges(Dataset<String> peopleData) {
        Dataset<Integer> ageRDD = peopleData.map((MapFunction<String, Integer>) s -> {
            String[] data = s.split(",");
            int age = Integer.valueOf(data[1].trim());

            return age;
        }, Encoders.INT());

        ageRDD.show();

        /**
         * output
         *
         +-----+
         |value|
         +-----+
         |   29|
         |   30|
         |   19|
         +-----+
         */
    }
}
