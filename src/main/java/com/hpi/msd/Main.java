package com.hpi.msd;

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
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;

/**
 * Created by nicolashoeck on 30.05.19.
 */
public class Main {
    public static void main(String[] args) {

               Properties props = new Properties();
               props.put(StreamsConfig.APPLICATION_ID_CONFIG, "treeworker");
               props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
               props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
               props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class);
            //   props.put("value.deserializer", "com.hpi.msd.RecordDeserializer");
              // props.put("value.serializer", "com.hpi.msd.RecordSerializer");


               final StreamsBuilder builder = new StreamsBuilder();



        final KStream<String , Record> recordStream = builder.stream("input");

               Duration window = Duration.ofSeconds(10) ;

               KTable<Windowed<String>,Record> aggregatedStream = recordStream.groupBy((String key, Record record) -> key).windowedBy(TimeWindows.of(window).advanceBy(window))


                        .aggregate(new Initializer<Record>() {
                                       @Override
                                       public Record apply() {
                                           HashMap<String, Integer> map = new HashMap<>();
                                           return new Record(map) ;
                                       }
                                   },

                                new Aggregator<String, Record, Record>() { /* adder */
                                    @Override
                                    public Record apply(String aggKey, Record newValue, Record aggValue) {
                                        HashMap newMap = newValue.getMap();
                                        HashMap aggMap = aggValue.getMap();
                                        Iterator it = newMap.entrySet().iterator();
                                        while (it.hasNext()) {
                                            Map.Entry pair = (Map.Entry) it.next();
                                            if (!aggMap.containsKey(pair.getKey())) {
                                                aggMap.put(pair.getKey(), pair.getValue());
                                            } else {
                                                int oldCount = (Integer) aggMap.get(pair.getKey());
                                                aggMap.put(pair.getKey(), oldCount + (Integer) pair.getValue());
                                            }
                                        }

                                        return new Record(aggMap);
                                    }
                                },

                                Materialized.<String, Record, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store").withValueSerde(new RecordSerde()).withKeySerde(Serdes.String()));

                                aggregatedStream.toStream().foreach(( key,  value) -> System.out.println(value.getMap().toString() + " " +new Timestamp(new Date().getTime())));


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

