package com.sheep.project.etl;

import com.sheep.flink.utils.FlinkUtils;
import com.sheep.flink.utils.SimpleConsumerConsumerRecordSchema;
import com.sheep.project.etl.core.NationJoinCodeTaleProcess;
import com.sheep.project.etl.source.RedisNationSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;
import java.util.Properties;

public class Job {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils.getStreamExecutionEnvironment();

        // TODO 获取Redis码表数据
        DataStream<HashMap<String, String>> nationSource = env.addSource(new RedisNationSource()).broadcast();

        String topic = "flink-commerce-etl";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","cdh001:9092,cdh002:9092,cdh003:9092");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("group.id","flink-etl");
        properties.setProperty("auto.offset.reset","latest");

        FlinkKafkaConsumer<ConsumerRecord<String, String>> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleConsumerConsumerRecordSchema(), properties);


        env.addSource(kafkaConsumer)
                .map(message -> message.value())
                .connect(nationSource)
                .process(new NationJoinCodeTaleProcess())
                .print();

        env.execute("ETL-Job");
    }
}
