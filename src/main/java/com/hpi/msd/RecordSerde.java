package com.hpi.msd;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by nicolashoeck on 22.06.19.
 */
public class RecordSerde implements Serde<HashMap>{


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<HashMap> serializer() {
        return new RecordSerializer();
    }

    @Override
    public Deserializer<HashMap> deserializer() {
        return new RecordDeserializer();
    }
}
