package com.hpi.msd;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Created by nicolashoeck on 22.06.19.
 */
public class RecordSerde implements Serde<Record>{


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Record> serializer() {
        return new RecordSerializer();
    }

    @Override
    public Deserializer<Record> deserializer() {
        return new RecordDeserializer();
    }
}
