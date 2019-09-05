package com.hpi.msd;

import com.google.common.collect.Multimap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.*;

import java.util.*;



public class Treeworker {

    public static void main(String[] args) throws Exception {


        if (!args[0].isEmpty()){DatasetPath.getInstance().setDataset_path(args[0]);}else{System.out.println("Please provide absolute dataset path as first commandline argument");System.exit(0);}
        if (!args[1].isEmpty()){ThresholdParameter.getInstance().setThreshold(Double.parseDouble(args[1]));}else{System.out.println("Please provide security threshold from 0.01 to 1.0 as first commandline argument");System.exit(0);}

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "treeworker");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // windowedSerde.getClass()
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7070");

        final Topology topology = new Topology();

        StoreBuilder<KeyValueStore<String, Multimap>> treeStructure = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("treestructure"),
                Serdes.String(),
                new MultimapSerde())
                .withLoggingDisabled(); // disable backing up the store to a changelog topic

        // add the source processor node that takes Kafka topic "source-topic" as input
        //aggregatedinput contains our normal (also unaggregated) input
        topology.addSource("inputTreeworker", "aggregatedinput")

        // add the AttributeCountProcessorAPI node which takes the source processor as its upstream processor
        .addProcessor("treeworker", () -> new TreeworkerProcessor(), "inputTreeworker").addStateStore(treeStructure, "treeworker");

        KafkaStreams streaming = new KafkaStreams(topology, props);

        final Query restService = new Query(streaming, "treestructure", "localhost", 7070);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                streaming.close();restService.stop();
            } catch (Exception e) {
            }
        }));

        streaming.cleanUp();
        streaming.start();

        restService.start();
    }

}


