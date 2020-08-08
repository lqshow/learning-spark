package com.example.spark.rdd.dataFrame.read;

public class CreateDateFrameFromFiles {
    public static void main(String[] args) {
        //        JavaPairRDD<String, String> xx = jsc.wholeTextFiles("/Users/linqiong/Downloads/bb_gbk.csv");

/*

        JavaRDD<Row> rowRDD = xx.values().flatMap(line -> {
            ByteArrayInputStream stringInputStream = new ByteArrayInputStream(line.getBytes());
            BufferedReader in = new BufferedReader(new InputStreamReader(stringInputStream, "UTF-8"));

            CSVParser parser = new CSVParser(in, csvFileFormat);
            List<List> data = new ArrayList<>();
            for (CSVRecord record : parser) {
                List<String> lineData = new ArrayList<>();
                for (Iterator iter = record.iterator(); iter.hasNext(); ) {
                    String str = (String) iter.next();
                    lineData.add(str);
                }
//                System.out.println(lineData);
//                System.out.println("####");
                data.add(lineData);
            }
            return data.iterator();
        }).map(line -> {
            System.out.println(line);
            System.out.println("\n");
            return RowFactory.create(line);
        });

        System.out.println("#### count " + rowRDD.count());
         */
    }

}
