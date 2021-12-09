package com.sheep.flink.exercise;

import com.sheep.flink.exercise.bean.WordTime;
import com.sheep.flink.exercise.process.WordTimeProcess;
import com.sheep.flink.exercise.watemake.WordTimeTimestampAssigner;
import com.sheep.flink.exercise.watemake.WordTimeWatermarkGeneratorSupplier;
import com.sheep.flink.utils.FlinkUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class FlinkWaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironment();

        String host = "cdh001";

        int post = 8756;

        env.socketTextStream(host, post)
                .flatMap(new RichFlatMapFunction<String, WordTime>() {
                    @Override
                    public void flatMap(String value, Collector<WordTime> collector) throws Exception {
                        String[] split = value.split(",");
                        collector.collect(new WordTime(split[0], Long.valueOf(split[1])));
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .forGenerator(ctx -> new WordTimeWatermarkGeneratorSupplier())
                                .withTimestampAssigner(ctx -> new WordTimeTimestampAssigner()))
                .keyBy(WordTime::getWord)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                .process(new WordTimeProcess())
                .print();


        env.execute();
    }
}
