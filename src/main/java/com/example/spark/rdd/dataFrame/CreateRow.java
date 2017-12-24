package com.example.spark.rdd.dataFrame;

import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

public class CreateRow {
    public static void main(String[] args) {
        Row row = RowFactory.create(5, "c");
        System.out.println(row);
        /**
         * output
         *
         * [5,c]
         */

        List<String> list = new ArrayList<>();
        list.add("xx");
        list.add("yy");
        list.add("cc");

        Row row2 = RowFactory.create(list.toArray());
        System.out.println(row2);
        /**
         * output
         *
         * [xx,yy,cc]
         */
    }
}
