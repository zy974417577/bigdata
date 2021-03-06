package com.sheep.flink.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class SimpleConsumerConsumerRecordSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {
   private String encoding = "UTF8";

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> nextElement) {
        return false;
    }

    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        return new ConsumerRecord(record.topic(),
                record.partition(),
                record.offset(),
                record.timestamp(),
                record.timestampType(),
                record.checksum(),
                record.serializedKeySize(),
                record.serializedValueSize(),
                /*这里我没有进行空值判断，生产一定记得处理*/
                new  String(record.key(), encoding),
                new  String(record.value(), encoding));
    }

    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        return null;
    }
}
