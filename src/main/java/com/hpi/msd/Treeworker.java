package com.hpi.msd;

import com.google.common.collect.Multimap;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;

import java.io.File;
import java.nio.file.Files;
import java.util.*;

/**
 * Created by nicolashoeck on 30.05.19.
 */
public class Treeworker {
    public static void main(String[] args) throws Exception {

        TimeWindowedSerializer<String> windowedSerializer = new TimeWindowedSerializer<>(new StringSerializer());
        TimeWindowedDeserializer<String> windowedDeserializer = new TimeWindowedDeserializer<>(new StringDeserializer());
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "treeworker");
    //    props.put(StreamsConfig.CLIENT_ID_CONFIG, "interactive-queries-treeworker");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass()); // windowedSerde.getClass()
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, RecordSerde.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:7070");
        //  final File example = Files.createTempDirectory(new File("/tmp").toPath(), "example").toFile();
        //    props.put(StreamsConfig.STATE_DIR_CONFIG, example.getPath());

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
        topology.addSource("inputTreeworker", "aggregatedinput")

                // add the AttributeCountProcessorAPI node which takes the source processor as its upstream processor
                .addProcessor("treeworker", () -> new TreeworkerProcessor(), "inputTreeworker").addStateStore(treeStructure, "treeworker");

        // add the count store associated with the AttributeCountProcessorAPI processor
        //  .addStateStore(countStoreSupplier, "treeStore")

        // add the sink processor node that takes Kafka topic "sink-topic" as output
        // and the AttributeCountProcessorAPI node as its upstream processor
        //.addSink("resu", "aggregatedRecords",key_serializer,value_serializer, "aggregate");


        KafkaStreams streaming = new KafkaStreams(topology, props);



        final Query restService =
                new Query(streaming, "treestructure", "localhost", 7070);

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


