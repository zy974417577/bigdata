package com.sheep.flink.exercise02;

import com.sheep.flink.exercise02.process.MyProcessWindowFunction;
import com.sheep.flink.exercise02.source.MySource;
import com.sheep.flink.utils.FlinkUtils;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkWateMake {

   public static final Logger logs =  LoggerFactory.getLogger(FlinkWateMake.class);

    public static void main(String[] args) throws Exception {

        logs.info("程序启动...");
        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironment();


        env.addSource(new MySource())
                .keyBy(persion->persion.getName())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new MyProcessWindowFunction())
                .print();


        env.execute("FlinkWateMake");

    }
}
