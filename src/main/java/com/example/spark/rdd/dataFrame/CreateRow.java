package com.example.spark.rdd.dataFrame;

import org.apache.spark.sql.RowFactory;

public class CreateRow {
    public static void main(String[] args) {
        RowFactory.create(5, "c");
    }
}
