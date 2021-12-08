package com.sheep.flink.exercise03;

import com.sheep.flink.exercise02.process.MyProcessWindowFunction;
import com.sheep.flink.exercise02.source.MySource;
import com.sheep.flink.exercise03.watemake.MyTimestampAssigner;
import com.sheep.flink.exercise03.watemake.MyWatermarkGeneratorSupplier;
import com.sheep.flink.utils.FlinkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkEventTime {

   public static final Logger logs =  LoggerFactory.getLogger(FlinkEventTime.class);

    public static void main(String[] args) throws Exception {

        logs.info("程序启动...");
        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironment();

        env.addSource(new MySource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator(context -> new MyWatermarkGeneratorSupplier())
                                .withTimestampAssigner(context -> new MyTimestampAssigner())
                )
                .keyBy(persion->persion.getName())
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new MyProcessWindowFunction())
                .print();
        logs.info("测试git提交");

        logs.info("程序结束...");
        env.execute("FlinkWateMake");

    }
}
