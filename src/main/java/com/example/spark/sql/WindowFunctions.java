package com.example.spark.sql;

import com.example.spark.helpers.Utils;
import com.example.spark.rdd.dataFrame.read.CreateDataFrameFromExcel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import java.net.URL;

public class WindowFunctions {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Utils.registerDailyShowGuestsView(spark);
        Utils.registerEmployeeView(spark);

//        rowNumber(spark);
//        avg(spark);
        sum(spark);

    }

    /**
     * 对分组进行排序（再分组的基础上再进行排序）
     * @param spark
     */
    static void rowNumber(SparkSession spark) {
        /**
         * PARTITION BY：指定key分组
         * order by：分组后排序
         */
        Dataset<Row> sqlDF = spark.sql("select year, occupation, rowNo " +
                "from (" +
                "select year, occupation, show, group, raw_guest, " +
                "row_number() over (partition by year order by occupation desc) rowNo FROM daily_show_guests" +
                ") " +
                "where rowNo <= 5");

        sqlDF.show();

        /**
         * output
         *
         * +----+--------------------+-----+
         * |year|          occupation|rowNo|
         * +----+--------------------+-----+
         * |2012|          us senator|    1|
         * |2012|          us senator|    2|
         * |2012|          us senator|    3|
         * |2012|us secetary of ed...|    4|
         * |2012|   us representative|    5|
         * |2014|              writer|    1|
         * |2014|white house official|    2|
         * |2014|          us senator|    3|
         * |2014|          us senator|    4|
         * |2014|   us representative|    5|
         * |2013|              writer|    1|
         * |2013|white house sommu...|    2|
         * |2013|   us representative|    3|
         * |2013|united nations of...|    4|
         * |2013|     television host|    5|
         * |2005|              writer|    1|
         * |2005|              writer|    2|
         * |2005|              writer|    3|
         * |2005|              writer|    4|
         * |2005|              writer|    5|
         * +----+--------------------+-----+
         */

        /**
         * 不分组
         */
        Dataset<Row> sqlDF2 = spark.sql("select year, occupation, rowNo " +
                "from (" +
                "select year, occupation, show, group, raw_guest, " +
                "row_number() over (order by 1) rowNo FROM daily_show_guests" +
                ") " +
                "where rowNo <= 5");

        sqlDF2.show();
        /**
         * output
         *
         * +----+------------------+-----+
         * |year|        occupation|rowNo|
         * +----+------------------+-----+
         * |1999|             actor|    1|
         * |1999|          Comedian|    2|
         * |1999|television actress|    3|
         * |1999|      film actress|    4|
         * |1999|             actor|    5|
         * +----+------------------+-----+
         */

    }

    /**
     * uses the AVG() window function to calculate the average sales for employees
     * @param spark
     */
    static void avg(SparkSession spark) {
        Dataset<Row> sqlDF = spark.sql("SELECT depname, salary, avg(salary) OVER (PARTITION BY depname) FROM empsalary");
        sqlDF.show();
        /**
         *
         * +---------+------+------------------------------------------------------------------------------------------------+
         |  depname|salary|avg(salary) OVER (PARTITION BY depname ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)|
         +---------+------+------------------------------------------------------------------------------------------------+
         |  develop|  5200|                                                                                          5020.0|
         |  develop|  4200|                                                                                          5020.0|
         |  develop|  4500|                                                                                          5020.0|
         |  develop|  6000|                                                                                          5020.0|
         |  develop|  5200|                                                                                          5020.0|
         |    sales|  4800|                                                                               4866.666666666667|
         |    sales|  5000|                                                                               4866.666666666667|
         |    sales|  4800|                                                                               4866.666666666667|
         |personnel|  3500|                                                                                          3700.0|
         |personnel|  3900|                                                                                          3700.0|
         +---------+------+------------------------------------------------------------------------------------------------+
         */

        Dataset<Row> sqlDF3 = spark.sql("SELECT depname, salary, AVG(salary) OVER (order by salary) FROM empsalary");
        sqlDF3.show();
        /**
         *
         * +---------+------+----------------------------------------------------------------------------------------------------+
         |  depname|salary|avg(salary) OVER (ORDER BY salary ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)|
         +---------+------+----------------------------------------------------------------------------------------------------+
         |personnel|  3500|                                                                                              3500.0|
         |personnel|  3900|                                                                                              3700.0|
         |  develop|  4200|                                                                                  3866.6666666666665|
         |  develop|  4500|                                                                                              4025.0|
         |    sales|  4800|                                                                                   4283.333333333333|
         |    sales|  4800|                                                                                   4283.333333333333|
         |    sales|  5000|                                                                                   4385.714285714285|
         |  develop|  5200|                                                                                   4566.666666666667|
         |  develop|  5200|                                                                                   4566.666666666667|
         |  develop|  6000|                                                                                              4710.0|
         +---------+------+----------------------------------------------------------------------------------------------------+
         */

        Dataset<Row> sqlDF2 = spark.sql("SELECT depname, salary, avg(salary) OVER () FROM empsalary");
        sqlDF2.show();
        /**
         * output
         *
         * +---------+------+---------------------------------------------------------------------------+
         * |  depname|salary|avg(salary) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)|
         * +---------+------+---------------------------------------------------------------------------+
         * |  develop|  5200|                                                                     4710.0|
         * |  develop|  4200|                                                                     4710.0|
         * |  develop|  4500|                                                                     4710.0|
         * |  develop|  6000|                                                                     4710.0|
         * |  develop|  5200|                                                                     4710.0|
         * |personnel|  3500|                                                                     4710.0|
         * |personnel|  3900|                                                                     4710.0|
         * |    sales|  4800|                                                                     4710.0|
         * |    sales|  5000|                                                                     4710.0|
         * |    sales|  4800|                                                                     4710.0|
         * +---------+------+---------------------------------------------------------------------------+
         */


    }


    static void sum(SparkSession spark) {
        Dataset<Row> sqlDF = spark.sql("SELECT depname, salary, " +
                "sum(salary) OVER () " +
                "FROM empsalary");
        sqlDF.show();

//        WindowSpec spec = Window.partitionBy("depanme").orderBy("salary");
    }
}
