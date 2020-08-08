package com.example.spark.app.multiline_csv_file;

import com.example.spark.helpers.Utils;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.crunch.io.text.csv.CSVInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;


public class parseMultilineCsv {
    private final static String GB18030 = "gb18030";

    public static void main(String[] args) throws Exception {
        SparkSession spark = Utils.createSparkSession();
        JavaSparkContext jsc = Utils.createJavaSparkContext(Utils.createSparkSession());


//        String localPath = "src/main/resources/file/multiline_gbk.csv";
//        String localPath = "/Users/linqiong/Downloads/INPATIENT_FEE.csv";
//        String localPath = "/Users/linqiong/Downloads/upload_file2_app_upload";//app上传由乱码
//        String localPath = "/xdp_test_105/xfs/e4f/c24/e4fc242f232b4bbeba71e1bfc83f1481/final";//hdfs
        String localPath = "/Users/linqiong/Downloads/test_file/destination";
//        String localPath = "/xdp_test_105/xfs/55a/18c/55a18ceabb5b421db8ce9d8e52f65080/final";//本省数据存在乱码
//        String localPath = "/xdp_test_105/xfs/9aa/f4c/9aaf4c14c6b04744a3cf57b2f2890ea9/data";

        try {
            usedNewAPIHadoopFile(jsc, localPath);
//            for (int i = 0; i <= 11; i++) {
//                String localPath1 = "/xdp_test_105/xfs/e4f/c24/e4fc242f232b4bbeba71e1bfc83f1481/_parts/" + i;
//                createDataFrameViaMultilineCsvFile(spark, jsc, localPath1, "ttt" + i);
//            }
//            createDataFrameViaMultilineCsvFile(spark, jsc, localPath);
//            createDataFrameViaReadBinaryFiles(spark, jsc, localPath);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    static void usedNewAPIHadoopFile(JavaSparkContext jsc, String localPath) {
        Configuration conf = new Configuration();
        conf.set("csv.inputfileencoding", GB18030);
        conf.set("csv.openquotechar", "\"");
        conf.set("csv.escapechar", "\\");

        JavaRDD<String> lines = jsc
                .newAPIHadoopFile(localPath, CSVInputFormat.class, null, null, conf)
                .map(s -> s._2().toString());

        LongAccumulator accum = jsc.sc().longAccumulator("counter");
        lines.foreach((VoidFunction<String>) line-> {
            accum.add(1);

            CSVParser xx = CSVParser.parse(line, CSVFormat.DEFAULT.withDelimiter(',').withQuote('"').withEscape('\\'));
//            System.out.println("#: " + accum.value() + "  "  + line);
            List<CSVRecord> csvRecords = xx.getRecords();
            Object[] attributes = new String[28];

            if (csvRecords.get(0).size() != 28) {
                System.out.println("#: " + accum.value() + "  " + csvRecords.get(0).size() + line);
            }

//            if (csvRecords.get(0).size() == 28) {
//                int i = 0;
//                for (Iterator iter = csvRecords.get(0).iterator(); iter.hasNext(); ) {
//                    attributes[i] = (Object) iter.next();
//                    i++;
//                }
//            }
        });
        System.out.println("####: " + accum.value());
//
//        lines.collect().stream().forEach(line -> {
//            accum.add(1);
//            String output = StringUtils.format("#%d:  %s", accum.value(), line);
//            System.out.println(output);
//        });
        /**
         * output
         *
         * #1:  c1,c2,c3,c4,c5,c6,c7
         * #2:  A," bbbb
         * bbaa
         * dd",CC,中文,xx,ff,"ss
         * fck
         * ss33"
         * #3:  11,22,33,44,55,66,66
         */
    }

    static StructType getCustomSchema() {
//        String[] columns = new String[]{"c1", "c2", "c3", "c4", "c5", "c6", "c7"};
        String[] columns = new String[28];
        for (int i = 0; i < 28; i++) {
            columns[i] = "c" + i;
        }
        int columnsLen = columns.length;
        System.out.println("bbb: " + columnsLen);

        StructField[] fields = new StructField[columnsLen];
        for (int i = 0; i < columnsLen; i++) {
            fields[i] = DataTypes.createStructField(columns[i], DataTypes.StringType, true);
        }
        StructType schema = DataTypes.createStructType(fields);

        return schema;
    }


    static void createDataFrameViaMultilineCsvFile(SparkSession spark, JavaSparkContext jsc, String localPath, String pa) throws Exception {
        StructType schema = getCustomSchema();

        Configuration conf = new Configuration();
        conf.set("csv.inputfileencoding", GB18030);
        conf.set("csv.openquotechar", "\"");
        conf.set("csv.escapechar", "\\");

        // Convert records to Rows
        LongAccumulator accum = jsc.sc().longAccumulator("counter");
        JavaRDD<Row> rowRDD = jsc
                .newAPIHadoopFile(localPath, CSVInputFormat.class, null, null, conf)
                .map(s -> {
                    accum.add(1);
//                    System.out.println("#: " + accum.value() + " " + s._2().toString());

                    CSVParser xx = CSVParser.parse(s._2().toString(),
                            CSVFormat.DEFAULT.withDelimiter(',').withQuote('"').withEscape('\\'));
                    List<CSVRecord> csvRecords = xx.getRecords();
                    Object[] attributes = new String[28];
                    if (csvRecords.get(0).size() == 28) {
                        int i = 0;
                        for (Iterator iter = csvRecords.get(0).iterator(); iter.hasNext(); ) {
                            attributes[i] = (Object) iter.next();
                            i++;
                        }
                    }

                    return RowFactory.create((Object[]) attributes);
                });

        // Apply the schema to the RDD
        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);
        System.out.println("count#: " + accum.value());
        System.out.println("tip#: " + pa);
        df.write().csv("output/" + pa);

//        df.show();
//        System.out.println(df.count());
        /**
         * output
         *
         * +---+---------------+---+---+---+---+-------------+
         * | c1|             c2| c3| c4| c5| c6|           c7|
         * +---+---------------+---+---+---+---+-------------+
         * | c1|             c2| c3| c4| c5| c6|           c7|
         * |  A|" bbbb
         * bbaa
         * dd"| CC| 中文| xx| ff|"ss
         * fck
         * ss33"|
         * | 11|             22| 33| 44| 55| 66|           66|
         * +---+---------------+---+---+---+---+-------------+
         */
    }

    /**
     * 读取整个文件去处理。效率低
     * @param spark
     * @param jsc
     * @param localPath
     */
    static void createDataFrameViaReadBinaryFiles(SparkSession spark, JavaSparkContext jsc, String localPath) {
        JavaRDD<Row> rowRDD = jsc.binaryFiles(localPath)
                .flatMap(line -> {
                    PortableDataStream ds = line._2();
                    DataInputStream dis = ds.open();
                    List<String[]> data = new ArrayList<>();

                    CSVReaderBuilder builder = new CSVReaderBuilder(new BufferedReader(new InputStreamReader(dis, GB18030)));
                    CSVParserBuilder parser = new CSVParserBuilder()
                            .withSeparator(',')
                            .withEscapeChar('\\')
                            .withQuoteChar('"');
                    try (CSVReader reader = builder.withCSVParser(parser.build()).build()) {
                        String[] nextLine;
                        while ((nextLine = reader.readNext()) != null) {
                            // nextLine[] is an array of values from the line
                            System.out.println(nextLine);
                            data.add(nextLine);
                        }
                    }
                    return data.iterator();
                }).map(line -> RowFactory.create(line));

        System.out.println("####:" + jsc.binaryFiles(localPath).collect().size());
        StructType schema = getCustomSchema();

        Dataset<Row> df = spark.createDataFrame(rowRDD, schema);

        Row first = df.first();
        df = df.filter((FilterFunction<Row>) row -> !row.equals(first));

        df.show();

        /**
         * output
         *
         * +---+-------------+---+---+---+---+-----------+
         * | c1|           c2| c3| c4| c5| c6|         c7|
         * +---+-------------+---+---+---+---+-----------+
         * | c1|           c2| c3| c4| c5| c6|         c7|
         * |  A| bbbb
         * bbaa
         * dd| CC| 中文| xx| ff|ss
         * fck
         * ss33|
         * | 11|           22| 33| 44| 55| 66|         66|
         * +---+-------------+---+---+---+---+-----------+
         */
    }
}
