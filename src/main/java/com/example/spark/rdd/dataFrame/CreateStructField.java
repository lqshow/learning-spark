package com.example.spark.rdd.dataFrame;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.List;

public class CreateStructField {
    public static void main(String[] args) {
        String[] columns = new String[]{"id", "name"};

        buildStructFieldList(columns);
        buildStructFieldArray(columns);
    }

    static List<StructField> buildStructFieldList(String[] columns) {
        List<StructField> fields = new ArrayList<>();

        for (String fieldName : columns) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }

        return fields;
    }

    static StructField[] buildStructFieldArray(String[] columns) {
        int columnsLen = columns.length;

        StructField[] fields = new StructField[columnsLen];
        for (int i = 0; i < columnsLen; i++) {
            fields[i] = new StructField(columns[i], DataTypes.StringType, true, Metadata.empty());
        }

        return fields;
    }
}
