package com.hpi.msd;

import com.google.common.collect.Multimap;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class MultimapSerde implements Serde<Multimap>{

    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public void close() {}

    @Override
    public Serializer<Multimap> serializer() {
        return new MultimapSerializer();
    }

    @Override
    public Deserializer<Multimap> deserializer() {
        return new MultimapDeserializer();
    }
}
