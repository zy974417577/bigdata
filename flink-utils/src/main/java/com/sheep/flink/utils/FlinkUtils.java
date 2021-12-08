package com.sheep.flink.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class FlinkUtils {

    static public StreamExecutionEnvironment getStreamExecutionEnvironment(){

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //TODO 设置内存 flink1.3加入
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        //TODO 设置HDFS存储
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://checkpoints");

        //TODO 设置WateMark触发时间
//        env.getConfig().setAutoWatermarkInterval(1000L);

        //TODO 设置checkpoint时间
        env.enableCheckpointing(10000);
        //TODO 设置checkpoint语意
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //TODO 设置checkpoint失败次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(1);


        //TODO 重启3次，每次失败后等待10000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        //TODO 在5分钟内，只能重启5次，每次失败后最少需要等待10秒
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                5,
                Time.of(5, TimeUnit.SECONDS),
                Time.of(10, TimeUnit.SECONDS)));

        return env;
    }
}
