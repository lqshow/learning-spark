package com.example.spark.rdd.javaRdd;

import com.example.spark.beans.AvgCount;
import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * references:
 * - https://sourcegraph.com/github.com/databricks/learning-spark/-/blob/src/main/java/com/oreilly/learningsparkexamples/java/BasicAvg.java#L27
 * - http://apachesparkbook.blogspot.com/2015/11/aggregate-examples.html
 * - http://www.jianshu.com/p/15739e95a46e
 *
 * 与reduce（）和fold（）相比，aggregate（）函数具有优势，它可以返回与RDD元素类型（即输入元素类型）不同的类型
 *
 *
 * 将每个分区里面的元素进行聚合，然后用combine函数将每个分区的结果和初始值(zeroValue)进行combine操作。这个函数最终返回的类型不需要和RDD中元素类型一致。
 *
 */
public class AggregateRdd {
    public static void main(String[] args) {
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> rdd = jsc.parallelize(data);

        AvgCount initial = new AvgCount(0, 0);

        // 两个操作的初始值都是zeroValue

        /**
         * addAndCount
         * seqOp操作会聚合各分区中的元素
         *
         * seqOp的操作是遍历分区中的所有元素，将初始值和第一个分区中的第一个元素传递给seq函数进行计算，然后将计算结果和第二个元素传递给seq函数，直到计算到最后一个值。第二个分区中也是同理操作。直到遍历完整个分区。
         *
         */
        Function2<AvgCount, Integer, AvgCount> seqOp = new Function2<AvgCount, Integer, AvgCount>() {
            /**
             *
             * @param acc Reprsents the accumulated result
             * @param value Represents the element in 'inputrdd' In our case this of type (Integer)
             * @return
             */
            @Override
            public AvgCount call(AvgCount acc, Integer value) {
                acc.total += value;
                acc.num += 1;
                return acc;
            }
        };

        /**
         * combine
         * combOp操作将初始值和所有分区的聚合结果再次聚合
         *
         */
        Function2<AvgCount, AvgCount, AvgCount> combOp = new Function2<AvgCount, AvgCount, AvgCount>() {
            @Override
            public AvgCount call(AvgCount acc1, AvgCount acc2) {
                acc1.total += acc2.total;
                acc1.num += acc2.num;
                return acc1;
            }
        };

        // aggregate函数返回一个跟RDD不同类型的值
        AvgCount result = rdd.aggregate(initial, seqOp, combOp);
        System.out.println("result.avg() = " + result.avg());
    }
}
