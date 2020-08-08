package com.example.spark.sql.window;

import com.example.spark.helpers.Utils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AggregateFunctions {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        Utils.registerEmployeeView(spark);

        avg(spark);
    }

    static void aggreagte(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                // 默认从起点到当前所有重复行： sum(salary) OVER (PARTITION BY depname ORDER BY salary ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                "sum(salary) OVER (PARTITION BY depname ORDER BY salary ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) salary_1, " +
                                // 默认从起点到当前行
                                "sum(salary) OVER (PARTITION BY depname ORDER BY salary ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) salary_2," +
                                // 不指定ORDER BY，则将分组内所有值累加: sum(salary) OVER (PARTITION BY depname ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                                "sum(salary) OVER (PARTITION BY depname)  as salary_3 " +

//
//                                "sum(salary) OVER (PARTITION by depname order by salary asc ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as salary_4," +
//                                "count(salary) OVER (PARTITION BY depname ORDER BY salary DESC) as count " +
                                "FROM empsalary"
                );
        sqlDF.show();
    }

    static void avg(SparkSession spark) {
        Dataset<Row> sqlDF = spark
                .sql(
                        "SELECT " +
                                "depname, " +
                                "salary, " +
                                // 默认从起点到当前所有重复行: avg(salary) OVER (PARTITION BY depname ORDER BY salary ASC NULLS FIRST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                "avg(salary) OVER (PARTITION BY depname ORDER BY salary ASC) avg_0, " +
                                "avg(salary) OVER (PARTITION BY depname ORDER BY salary ASC RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) avg_1, " +

                                // 默认从起点到当前行
                                "avg(salary) OVER (PARTITION BY depname ORDER BY salary ASC NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) avg_2," +

                                // 不指定ORDER BY，则覆盖分组内所有行: avg(salary) OVER (PARTITION BY depname ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                                "avg(salary) OVER (PARTITION BY depname) avg_3, " +

                                // 不分区且无order by, 覆盖所有行： avg(salary) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
                                "avg(salary) OVER () " +

                                "FROM empsalary"
                );
        sqlDF.show();
    }
}
