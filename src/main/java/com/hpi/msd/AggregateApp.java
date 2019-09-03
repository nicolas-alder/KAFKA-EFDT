package com.hpi.msd;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.*;

public class AggregateApp {
    public static void main(String[] args) {

               Properties props = new Properties();
               props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregateApp");
               props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
               props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
               props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class);

               final StreamsBuilder builder = new StreamsBuilder();
               final KStream<String , HashMap> recordStream = builder.stream("input");

               Duration window = Duration.ofSeconds(10) ;
               Duration grace = Duration.ofMillis(1);

               TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(new StringSerializer());
               TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer());
               Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);

               KTable<Windowed<String>,HashMap> aggregatedStream = recordStream.groupBy((String key, HashMap record) -> key).windowedBy(TimeWindows.of(window).advanceBy(window).grace(grace))
                       .aggregate(new Initializer<HashMap>() {
                                       @Override
                                       public HashMap apply() {
                                           HashMap<String, Double> map = new HashMap<>();
                                           return map;
                                       }
                                   },

                                new Aggregator<String, HashMap, HashMap>() { /* adder */
                                    @Override
                                    public HashMap apply(String aggKey, HashMap newValue, HashMap aggValue) {

                                        Iterator it = newValue.entrySet().iterator();
                                        while (it.hasNext()) {
                                            Map.Entry pair = (Map.Entry) it.next();
                                            if (!aggValue.containsKey(pair.getKey())) {
                                                aggValue.put(pair.getKey(), pair.getValue());
                                            } else {
                                                double oldCount = (Double) aggValue.get(pair.getKey());
                                                aggValue.put(pair.getKey(), oldCount + (Double) pair.getValue());
                                            }
                                        }

                                        return aggValue;
                                    }
                                },

                                Materialized.<String, HashMap, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store").withValueSerde(new RecordSerde()).withKeySerde(Serdes.String()));
                                aggregatedStream.toStream().to("aggregatedInput", Produced.with(windowedSerde,new RecordSerde()));

                                 final KafkaStreams streams = new KafkaStreams(builder.build(), props);
                                 streams.cleanUp();
                                 streams.start();



    }
}

