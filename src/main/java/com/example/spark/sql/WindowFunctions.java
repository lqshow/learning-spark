package com.example.spark.sql;

import com.example.spark.helpers.Utils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class WindowFunctions {
    public static void main(String[] args) {
        SparkSession spark = Utils.createSparkSession();

        String[] columns = new String[]{"year", "occupation", "show", "group", "raw_guest"};
        int columnsLen = columns.length;
        StructField[] fields = new StructField[columnsLen];
        for (int i = 0; i < columnsLen; i++) {
            fields[i] = DataTypes.createStructField(columns[i], DataTypes.StringType, true);
        }
        StructType schema = DataTypes.createStructType(fields);

        Dataset<String> data = spark.read().textFile("src/main/resources/daily_show_guests");
        JavaRDD<Row> rowRDD = data.toJavaRDD()
                .map(line -> {
                    String[] attributes = line.split(",");
                    return RowFactory.create(attributes);
                });
        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);

        dataFrame.createOrReplaceTempView("daily_show_guests");

        rowNumber(spark);
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
}
