package com.sheep.flink.exercise01;

import com.sheep.flink.utils.FlinkUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkCheckpoint {

    static final private Logger logs = LoggerFactory.getLogger(FlinkCheckpoint.class);

    public static void main(String[] args) throws Exception {

        logs.info("程序开始...");
        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironment();

        env.readTextFile("data/data.txt")
    //.map(x->new Tuple2<String,Integer>(x,1)).keyBy(0).sum(1).print();
                .flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String coll : value.split(",")) {
                            out.collect(new Tuple2(coll, 1));
                        }
                    }
                }).keyBy(0)
                .sum(1).print();

        logs.info("程序结束...");

        env.execute("checkpoint");
    }
}
