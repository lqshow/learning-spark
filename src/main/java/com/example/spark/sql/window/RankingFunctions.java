package com.example.spark.sql.window;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RankingFunctions {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Utils.registerEmployeeView(spark);

        denseRnak(spark);
        rank(spark);
        rowNumber(spark);
        combine(spark);
    }

    /**
     * 连续排序， 两个第二名仍然跟着第三名，排名相等不会留下空位
     *
     * @param spark
     */
    static void denseRnak(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                "dense_rank() OVER (PARTITION BY depname ORDER BY salary DESC) AS dense_rank " +
                                "FROM empsalary"
                );
        sqlDF.show();

        /**
         * output
         *
         * +---------+------+----------+
         * |  depname|salary|dense_rank|
         * +---------+------+----------+
         * |  develop|  6000|         1|
         * |  develop|  5200|         2|
         * |  develop|  5200|         2|
         * |  develop|  4500|         3|
         * |  develop|  4200|         4|
         * |    sales|  5000|         1|
         * |    sales|  4800|         2|
         * |    sales|  4800|         2|
         * |personnel|  3900|         1|
         * |personnel|  3500|         2|
         * +---------+------+----------+
         */
    }

    /**
     * 跳跃排序，两个第二名下来就是第四名， 排名相等会在名次中留下空位
     *
     * @param spark
     */
    static void rank(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                "rank() OVER (PARTITION BY depname ORDER BY salary DESC) as rank " +
                                "FROM empsalary"
                );
        sqlDF.show();

        /**
         * output
         *
         * +---------+------+----+
         * |  depname|salary|rank|
         * +---------+------+----+
         * |  develop|  6000|   1|
         * |  develop|  5200|   2|
         * |  develop|  5200|   2|
         * |  develop|  4500|   4|
         * |  develop|  4200|   5|
         * |    sales|  5000|   1|
         * |    sales|  4800|   2|
         * |    sales|  4800|   2|
         * |personnel|  3900|   1|
         * |personnel|  3500|   2|
         * +---------+------+----+
         */
    }

    static void rowNumber(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                "row_number() OVER (order by 1) as rowNo " +
                                "FROM empsalary"
                );
        sqlDF.show();

        /**
         * output
         *
         * +---------+------+-----+
         * |  depname|salary|rowNo|
         * +---------+------+-----+
         * |  develop|  5200|    1|
         * |  develop|  4200|    2|
         * |  develop|  4500|    3|
         * |  develop|  6000|    4|
         * |  develop|  5200|    5|
         * |personnel|  3500|    6|
         * |personnel|  3900|    7|
         * |    sales|  4800|    8|
         * |    sales|  5000|    9|
         * |    sales|  4800|   10|
         * +---------+------+-----+
         *
         */
    }

    /**
     * 分组内当前行(RANK值-1) / (分组内总行数-1)
     *
     * @param spark
     */
    static void percentRank(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                "rank() OVER (PARTITION BY depname ORDER BY salary DESC) as rank, " +
                                "percent_rank() OVER (PARTITION BY depname ORDER BY salary DESC) as percent_rank " +
                                "FROM empsalary"
                );
        sqlDF.show();
    }

    static void combine(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                "rank() OVER (PARTITION BY depname ORDER BY salary DESC) as rank, " +
                                "dense_rank() OVER (PARTITION BY depname ORDER BY salary DESC) as dense_rank, " +
                                "percent_rank() OVER (PARTITION BY depname ORDER BY salary DESC) as percent_rank, " +
                                "row_number() OVER (PARTITION BY depname ORDER BY salary DESC) as rowNo " +
                                "FROM empsalary"
                );
        sqlDF.show();

        /**
         *
         * +---------+------+----+----------+------------+-----+
         * |  depname|salary|rank|dense_rank|percent_rank|rowNo|
         * +---------+------+----+----------+------------+-----+
         * |  develop|  6000|   1|         1|         0.0|    1|
         * |  develop|  5200|   2|         2|        0.25|    2|
         * |  develop|  5200|   2|         2|        0.25|    3|
         * |  develop|  4500|   4|         3|        0.75|    4|
         * |  develop|  4200|   5|         4|         1.0|    5|
         * |    sales|  5000|   1|         1|         0.0|    1|
         * |    sales|  4800|   2|         2|         0.5|    2|
         * |    sales|  4800|   2|         2|         0.5|    3|
         * |personnel|  3900|   1|         1|         0.0|    1|
         * |personnel|  3500|   2|         2|         1.0|    2|
         * +---------+------+----+----------+------------+-----+
         */
    }
}
