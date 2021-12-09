package com.sheep.flink.exercise.bean;

import lombok.Data;
import org.apache.commons.lang3.time.FastDateFormat;

@Data
public class WordTime {


    public WordTime() {
    }

    public WordTime(String word, long time) {
        this.word = word;
        this.time = time;
    }

    private String word;
    private long time;

    public String toString() {
        FastDateFormat dateFormat = FastDateFormat.getInstance("yyyyMMdd HH:mm:ss");
        return word +", "+ dateFormat.format(time);
    }

}
