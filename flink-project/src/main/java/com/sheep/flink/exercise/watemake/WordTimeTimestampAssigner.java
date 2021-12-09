package com.sheep.flink.exercise.watemake;

import com.sheep.flink.exercise.bean.WordTime;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

public class WordTimeTimestampAssigner implements TimestampAssigner<WordTime> {
    @Override
    public long extractTimestamp(WordTime element, long recordTimestamp) {
//        System.out.println("时间戳为: "+Long.valueOf(element.getBirthday()));
        return Long.valueOf(element.getTime());
    }
}
