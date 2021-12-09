package com.sheep.flink.exercise01.exercise;

import com.sheep.flink.utils.FlinkUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkWaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironment();
        
        String host ="cdh001";
        
        int post = 8756;
        
        env.socketTextStream(host,post)
                .flatMap(new RichFlatMapFunction<String, >() {
                })
        
        env.execute();
    }
}
