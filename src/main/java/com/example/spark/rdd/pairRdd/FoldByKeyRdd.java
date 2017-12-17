package com.example.spark.rdd.pairRdd;

import com.example.spark.beans.Person;
import com.example.spark.helpers.ParseLine;
import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class FoldByKeyRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());


        total(jsc);
        oldestPerson(jsc);
    }

    static void total(JavaSparkContext jsc) {
        URL path = ReduceRdd.class.getResource("/daily_show_guests");
        JavaRDD<String> lines = jsc.textFile(path.toString());

        JavaPairRDD<String, Integer> yearRDD = lines.mapToPair(new ParseLine());

        JavaPairRDD<String, Integer> yearStatisticsRDD = yearRDD.foldByKey(10,
                (Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        yearStatisticsRDD.foreach((VoidFunction<Tuple2<String, Integer>>) tuple -> {
            System.out.println("year: " + tuple._1 + ", total: " + tuple._2);
        });
    }

    static void oldestPerson(JavaSparkContext jsc) {
        List<Tuple2<String, Person>> personList = Arrays.asList(
                new Tuple2("java", new Person("Michael", 29)),
                new Tuple2("java", new Person("Andy", 30)),
                new Tuple2("python", new Person("Justin", 12)),
                new Tuple2("python", new Person("Lisa", 15))
        );

        JavaPairRDD<String, Person> pairRDD = jsc.parallelizePairs(personList);

        JavaPairRDD<String, Person> oldestPersonByOccupation = pairRDD.foldByKey(new Person("dummy", 0),
                (Function2<Person, Person, Person>) (acc, person) -> {
                    if (acc.getAge() < person.getAge()) {
                        return person;
                    } else {
                        return acc;
                    }
                });

        Map<String, Person> res = oldestPersonByOccupation.collectAsMap();
        for (Map.Entry<String, Person> entry : res.entrySet()) {
            String occupation = entry.getKey();
            Person curPerson = entry.getValue();

            String output = StringUtils.format("Oldest person by occupation: %s, name: %s, age: %d",
                    occupation, curPerson.getName(), curPerson.getAge());
            System.out.println(output);
        }

        /**
         * output
         *
         * Oldest person by occupation: java, name: Andy, age: 30
         * Oldest person by occupation: python, name: Lisa, age: 15
         */
    }
}
