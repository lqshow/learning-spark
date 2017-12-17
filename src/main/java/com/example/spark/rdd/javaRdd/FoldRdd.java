package com.example.spark.rdd.javaRdd;

import com.example.spark.beans.Person;
import com.example.spark.helpers.Utils;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * references:
 * - http://blog.madhukaraphatak.com/spark-rdd-fold/
 * <p>
 * fold和reduce()区别就在于一个初始值
 */
public class FoldRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        total(jsc);
        oldestPerson(jsc);
    }

    static void total(JavaSparkContext jsc) {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> rdd = jsc.parallelize(data);

        int totalLength = rdd.fold(0, (Function2<Integer, Integer, Integer>) (a, b) -> a + b);
        System.out.println("total: " + totalLength);
    }

    static void oldestPerson(JavaSparkContext jsc) {
        List<Person> personList = Arrays.asList(
                new Person("Michael", 29),
                new Person("Andy", 30),
                new Person("Justin", 18)
        );
        JavaRDD<Person> rdd = jsc.parallelize(personList);


        Person dummyPerson = new Person("dummy", 0);
        Person oldestPerson = rdd.fold(dummyPerson, (Function2<Person, Person, Person>) (acc, person) -> {
            if (acc.getAge() < person.getAge()) {
                return person;
            } else {
                return acc;
            }
        });

        String output = StringUtils.format("Oldest person name: %s, age: %d", oldestPerson.getName(), oldestPerson.getAge());
        System.out.println(output);
        /**
         * output
         *
         * Oldest person name: Andy, age: 30
         */
    }
}
