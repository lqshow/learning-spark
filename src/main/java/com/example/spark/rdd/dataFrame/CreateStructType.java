package com.example.spark.rdd.dataFrame;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CreateStructType {
    public static void main(String[] args) {
        String[] columns = new String[]{"id", "name"};

        buildSparkSchema1(columns);
        buildSparkSchema2(columns);
        buildSparkSchema3(columns);
    }

    public static StructType buildSparkSchema1(String[] columns) {
        int columnsLen = columns.length;

        StructField[] fields = new StructField[columnsLen];
        for (int i = 0; i < columnsLen; i++) {
            fields[i] = DataTypes.createStructField(columns[i], DataTypes.StringType, true);
        }
        StructType schema = DataTypes.createStructType(fields);
        System.out.println(schema);

        return schema;
    }

    public static StructType buildSparkSchema2(String[] columns) {
        int columnsLen = columns.length;

        StructField[] fields = new StructField[columnsLen];
        for (int i = 0; i < columnsLen; i++) {
            fields[i] = new StructField(columns[i], DataTypes.StringType, true, Metadata.empty());
        }
        StructType schema = new StructType(fields);

        System.out.println(schema);

        return schema;
    }

    public static StructType buildSparkSchema3(String[] columns) {
        int columnsLen = columns.length;

        StructType schema = new StructType();
        for (int i = 0; i < columnsLen; i++) {
            schema = schema.add(columns[i], DataTypes.StringType, true);
        }

        System.out.println(schema);

        return schema;
    }
}