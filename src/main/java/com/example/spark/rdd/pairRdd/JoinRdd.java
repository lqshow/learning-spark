package com.example.spark.rdd.pairRdd;

import com.example.spark.helpers.Utils;
import com.example.spark.rdd.javaRdd.ReduceRdd;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.net.URL;

public class JoinRdd {
    public static class ParseLine implements PairFunction<String, String, String> {
        @Override
        public Tuple2<String, String> call(String line) throws Exception {
            String key = StringUtils.split(line, ",")[0].trim();
            return new Tuple2(key, line);
        }
    }

    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        URL path = ReduceRdd.class.getResource("/people.txt");
        JavaRDD<String> people = jsc.textFile(path.toString());
        JavaPairRDD<String, String> pairPeopleRDD = people.mapToPair(new ParseLine());

        URL addressPath = ReduceRdd.class.getResource("/people_address.txt");
        JavaRDD<String> peopleAddress = jsc.textFile(addressPath.toString());
        JavaPairRDD<String, String> pairPeopleAddress = peopleAddress.mapToPair(new ParseLine());


        subtractByKey(pairPeopleRDD, pairPeopleAddress);
    }

    /**
     * 内链接
     * @param pairPeopleRDD
     * @param pairPeopleAddress
     */
    static void join(JavaPairRDD<String, String> pairPeopleRDD, JavaPairRDD<String, String> pairPeopleAddress) {
        JavaPairRDD<String, Tuple2<String, String>> resJoin = pairPeopleRDD.join(pairPeopleAddress);

        resJoin.saveAsTextFile("output/join");

        /**
         * output
         *
         * (Michael,(Michael, 29,Michael, beijing))
         * (Andy,(Andy, 30,Andy, shanghai))
         * (Justin,(Justin, 19,Justin, chongqing))
         */
    }

    /**
     * 左外链接
     * @param pairPeopleRDD
     * @param pairPeopleAddress
     */
    static void leftOutJoin(JavaPairRDD<String, String> pairPeopleRDD, JavaPairRDD<String, String> pairPeopleAddress) {
        JavaPairRDD<String, Tuple2<String, Optional<String>>> resJoin = pairPeopleRDD.leftOuterJoin(pairPeopleAddress);
        resJoin.saveAsTextFile("output/leftOutJoin");

        /**
         * output
         *
         * (Michael,(Michael, 29,Optional[Michael, beijing]))
         * (Andy,(Andy, 30,Optional[Andy, shanghai]))
         * (Justin,(Justin, 19,Optional[Justin, chongqing]))
         */
    }

    /**
     * 右外连接
     * @param pairPeopleRDD
     * @param pairPeopleAddress
     */
    static void rightOutJoin(JavaPairRDD<String, String> pairPeopleRDD, JavaPairRDD<String, String> pairPeopleAddress) {
        JavaPairRDD<String, Tuple2<Optional<String>, String>> resJoin = pairPeopleRDD.rightOuterJoin(pairPeopleAddress);
        resJoin.saveAsTextFile("output/rightOutJoin");

        /**
         * output
         *
         * (Michael,(Optional[Michael, 29],Michael, beijing))
         * (Andy,(Optional[Andy, 30],Andy, shanghai))
         * (jon,(Optional.empty,jon, wuhan))
         * (Justin,(Optional[Justin, 19],Justin, chongqing))
         */
    }

    /**
     * 返回在主RDD中出现，并且不在otherRDD中出现的元素
     * @param pairPeopleRDD
     * @param pairPeopleAddress
     */
    static void subtractByKey(JavaPairRDD<String, String> pairPeopleRDD, JavaPairRDD<String, String> pairPeopleAddress) {
        JavaPairRDD<String, String> resPairRDD = pairPeopleAddress.subtractByKey(pairPeopleRDD);

        resPairRDD.foreach((VoidFunction<Tuple2<String, String>>) tuple -> {
            System.out.println("key: " + tuple._1 + ", value: " + tuple._2);
        });
    }
}
