package com.example.spark.helpers;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class ParseLine implements PairFunction<String, String, Integer> {
    @Override
    public Tuple2<String, Integer> call(String line) throws Exception {
        String key = StringUtils.split(line, ",")[0].trim();
        return new Tuple2(key, 1);
    }
}
