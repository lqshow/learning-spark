### Overview
Spark 2.2以下版本对于读取csv单元格内存在多行值(LF)是存在问题的，spark 2.2.0版本虽然修复了该问题，添加了multiLine参数，
但是加上该参数后，encoding参数会失效，对于中文非utf8编码来说读取出来是一堆乱码。

### Issue

- [spark2.2.0 reading CSV with multiLine invalidates encoding option](https://github.com/databricks/spark-csv/issues/448)
- [Rename `wholeFile` to `multiLine` for both CSV and JSON](https://github.com/apache/spark/pull/18202)
- [Support parsing multiline CSV files](https://github.com/apache/spark/pull/16976)

### Solution

- 使用 newAPIHadoopFile && CSVInputFormat

#### Apache crunch maven dependency
```mxml
<dependency>
  <groupId>org.apache.crunch</groupId>
  <artifactId>crunch-core</artifactId>
  <version>0.15.0</version>
</dependency>
```

```java
Configuration conf = new Configuration();
conf.set("csv.inputfileencoding", "gb18030");

jsc.newAPIHadoopFile("/Users/linqiong/Downloads/bb_gbk.csv",
  CSVInputFormat.class, null, null, conf)
  .map(s -> s._2().toString());
```

- 通过读取二进制文件来解决

#### opencsv maven dependency
```mxml
<dependency>
  <groupId>com.opencsv</groupId>
  <artifactId>opencsv</artifactId>
  <version>4.1</version>
</dependency>
```

```java
JavaRDD<Row> rowRDD = jsc.binaryFiles("/Users/linqiong/Downloads/bb_gbk.csv")
    .flatMap(line -> {
        PortableDataStream ds = line._2();
        DataInputStream dis = ds.open();
        List<String[]> data = new ArrayList<>();
        try (CSVReader reader = new CSVReader(new BufferedReader(new InputStreamReader(dis, GB18030)))) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                // nextLine[] is an array of values from the line
                data.add(nextLine);
            }
        }
        return data.iterator();
    }).map(line -> RowFactory.create(line));
```

## Reference
- [Replace new line (\n) character in csv file - spark scala](https://stackoverflow.com/questions/36990177/replace-new-line-n-character-in-csv-file-spark-scala/45087356#45087356)
- [如何拓展Hadoop的InputFormat为其他分隔符](https://www.coder4.com/archives/4313)
- [opencsv](http://opencsv.sourceforge.net/)