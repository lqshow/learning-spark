package com.example.spark;

import com.example.spark.helpers.Utils;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;


/**
 * 共享变量
 */
public class SharedVariables {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        accumulator(jsc);
    }

    /**
     * 累加器
     * @param jsc
     */
    static void accumulator(JavaSparkContext jsc) {
        LongAccumulator accum =  jsc.sc().longAccumulator("counter");

        jsc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(1));

        String output = StringUtils.format("variable: %s, value: %d", accum.name().toString(), accum.value());
        System.out.println(output);

        /**
         * output
         *
         * variable: Some(counter), value: 4
         */

    }

    /**
     * 广播变量
     * @param jsc
     */
    static void BroadcastVariable(JavaSparkContext jsc) {

    }
}

