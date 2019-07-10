package com.hpi.msd;

import com.google.common.collect.Multimap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
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

import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 * Created by nicolashoeck on 30.05.19.
 */
public class Treeworker {
    public static void main(String[] args) {

        TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(new StringSerializer());
        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer());
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer,windowedDeserializer);

               Properties props = new Properties();
               props.put(StreamsConfig.APPLICATION_ID_CONFIG, "treeworker");
               props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
               props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // windowedSerde.getClass()
               props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class);
            //   props.put("value.deserializer", "com.hpi.msd.RecordDeserializer");
              // props.put("value.serializer", "com.hpi.msd.RecordSerializer");


               //final StreamsBuilder builder = new StreamsBuilder();




     //   final KStream<String , Record> aggregatedInputStream = builder.stream("aggregatedInput");
       // aggregatedInputStream.print(Printed.toSysOut().withLabel());
        //System.out.println(value.getMap().toString() + " " +new Timestamp(new Date().getTime())));




        final Topology topology = new Topology();



        StoreBuilder<KeyValueStore<String, Multimap>> treeStructure = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("treestructure"),
                Serdes.String(),
                new MultimapSerde())
                .withLoggingDisabled(); // disable backing up the store to a changelog topic

        // add the source processor node that takes Kafka topic "source-topic" as input
        topology.addSource("inputTreeworker",  "aggregatedinput")

                // add the AttributeCountProcessorAPI node which takes the source processor as its upstream processor
                .addProcessor("treeworker", () -> new TreeworkerProcessor(), "inputTreeworker").addStateStore(treeStructure,"treeworker");

                // add the count store associated with the AttributeCountProcessorAPI processor
                //  .addStateStore(countStoreSupplier, "treeStore")

                // add the sink processor node that takes Kafka topic "sink-topic" as output
                // and the AttributeCountProcessorAPI node as its upstream processor
                //.addSink("resu", "aggregatedRecords",key_serializer,value_serializer, "aggregate");





                KafkaStreams streaming = new KafkaStreams(topology, props);
                streaming.cleanUp();
                streaming.start();


    }
}

