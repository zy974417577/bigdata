package com.sheep.flink.exercise03.watemake;

import com.sheep.flink.exercise02.bean.Persion;
import org.apache.flink.api.common.eventtime.TimestampAssigner;

public class MyTimestampAssigner implements TimestampAssigner<Persion> {
    @Override
    public long extractTimestamp(Persion element, long recordTimestamp) {
//        System.out.println("时间戳为: "+Long.valueOf(element.getBirthday()));
        return Long.valueOf(element.getBirthday());
    }
}
