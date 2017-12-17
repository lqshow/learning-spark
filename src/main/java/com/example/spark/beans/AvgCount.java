package com.example.spark.beans;

import java.io.Serializable;

public class AvgCount implements Serializable {
    public int total;
    public int num;

    public AvgCount(int total, int num) {
        this.total = total;
        this.num = num;
    }

    public float avg() {
        return total / (float) num;
    }
}
