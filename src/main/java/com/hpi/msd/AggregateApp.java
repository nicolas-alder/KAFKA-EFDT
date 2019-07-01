package com.hpi.msd;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 * Created by nicolashoeck on 30.05.19.
 */
public class AggregateApp {
    public static void main(String[] args) {

               Properties props = new Properties();
               props.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregateApp");
               props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
               props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
               props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class);
            //   props.put(StreamsConfig.DEFAULT_WINDOWED_KEY_SERDE_INNER_CLASS, Serdes.String().getClass());
          //     props.put(StreamsConfig.DEFAULT_WINDOWED_VALUE_SERDE_INNER_CLASS, RecordSerde.class);
            //   props.put("value.deserializer", "com.hpi.msd.RecordDeserializer");
              // props.put("value.serializer", "com.hpi.msd.RecordSerializer");


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
                       //.suppress(untilWindowCloses(Suppressed.BufferConfig.unbounded());



                                aggregatedStream.toStream().to("aggregatedInput", Produced.with(windowedSerde,new RecordSerde()));

                                //.suppress(untilWindowCloses(Suppressed.BufferConfig.unbounded()))


                                    final KafkaStreams streams = new KafkaStreams(builder.build(), props);
                                 streams.cleanUp();
                                 streams.start();


               /*
                       final Topology topology = new Topology();




        RecordDeserializer value_deserializer = new RecordDeserializer();
        StringDeserializer key_deserializer = new StringDeserializer();
        RecordSerializer value_serializer = new RecordSerializer();
        StringSerializer key_serializer = new StringSerializer();



        // add the source processor node that takes Kafka topic "source-topic" as input
        topology.addSource("source", key_deserializer, value_deserializer, "input")

                // add the AttributeCountProcessorAPI node which takes the source processor as its upstream processor
                //.addProcessor("aggregate", () -> new AttributeCountProcessorAPI(), "source")

                // add the count store associated with the AttributeCountProcessorAPI processor
                //  .addStateStore(countStoreSupplier, "treeStore")

                // add the sink processor node that takes Kafka topic "sink-topic" as output
                // and the AttributeCountProcessorAPI node as its upstream processor
                .addSink("outgoingAggregates", "aggregatedRecords",key_serializer,value_serializer, "aggregate");


        KafkaStreams streaming = new KafkaStreams(topology, props);
        streaming.start();

                StoreBuilder<KeyValueStore<String, Integer>> countStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("nodeStatistics"),
                Serdes.String(),
                Serdes.Integer())
                .withLoggingDisabled(); // disable backing up the store to a changelog topic
*/


    }
}

