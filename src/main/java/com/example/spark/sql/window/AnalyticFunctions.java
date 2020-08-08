package com.example.spark.sql.window;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AnalyticFunctions {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Utils.registerEmployeeView(spark);

        firstValue(spark);
        lastValue(spark);
    }

    /**
     * 取出分组内排序后，截止到当前行，第一个值
     * @param spark
     */
    static void firstValue(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                "first_value(salary) OVER (PARTITION BY depname ORDER BY salary DESC) as maxSalary, " +
                                "first_value(salary) OVER (PARTITION BY depname ORDER BY salary ASC) as minSalary " +
                                "FROM empsalary"
                );
        sqlDF.show();
        /**
         *+---------+------+---------+---------+
         |  depname|salary|maxSalary|minSalary|
         +---------+------+---------+---------+
         |  develop|  4200|     6000|     4200|
         |  develop|  4500|     6000|     4200|
         |  develop|  5200|     6000|     4200|
         |  develop|  5200|     6000|     4200|
         |  develop|  6000|     6000|     4200|
         |    sales|  4800|     5000|     4800|
         |    sales|  4800|     5000|     4800|
         |    sales|  5000|     5000|     4800|
         |personnel|  3500|     3900|     3500|
         |personnel|  3900|     3900|     3500|
         +---------+------+---------+---------+
         */
    }

    /**
     * 取出分组内排序后，截止到当前行，最后一个值
     * @param spark
     */
    static void lastValue(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                "last_value(salary) OVER (PARTITION BY depname ORDER BY salary DESC),  " +
                                "last_value(salary) OVER (PARTITION BY depname ORDER BY salary ASC)  " +
                                "FROM empsalary"
                );
        sqlDF.show();
    }
}
