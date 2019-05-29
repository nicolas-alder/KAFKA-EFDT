/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.*;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.sun.corba.se.spi.activation.IIOP_CLEAR_TEXT.value;

/**
 * In this example, we implement a simple WordCount program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * split each text line into words and then compute the word occurence histogram, write the continuous updated histogram
 * into a topic "streams-wordcount-output" where each record is an updated count of a single word.
 */
public class AttributeCount{

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-attributcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> stream = builder.<String, String>stream("attributj_node_i");
        //Nodenames will be dynamically generated with naming tree naming convention
        //Count Attribute values
        //Attribute values are counted from 1 to n for range of 1 to n discrete values, key represents possible values (key = 1 is value 1 etc.)
        //StateStore can be queried for currennt attribute statistics
        stream.groupByKey(Grouped.with(Serdes.String(), Serdes.String())).count(Materialized.<String, java.lang.Long, KeyValueStore<Bytes, byte[]>>as("CountsKeyValueStore"));

        stream.to("attributj_node_i_plus_1", Produced.with(Serdes.String(), Serdes.String()));
        stream.to("attributj_node_i_plus_2", Produced.with(Serdes.String(), Serdes.String()));


        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

       // stream.process(AttributeCount.query(streams));
        // This is for reset to work. Don't use in production - it causes the app to re-load the state from Kafka on every start
        streams.cleanUp();


        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });


            streams.start();

            //Testweises warten bis State ist initialisiert, weil sonst stream thread starting, not running fehler
             Thread.sleep(5000);

        try {
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static void query(KafkaStreams streams) {
        // Get the key-value store CountsKeyValueStore
        ReadOnlyKeyValueStore<String, String> keyValueStore =
                streams.store("CountsKeyValueStore", QueryableStoreTypes.keyValueStore());

        // Get the values for all of the keys available in this application instance
        KeyValueIterator<String, String> range = keyValueStore.all();
        while (range.hasNext()) {
            KeyValue<String, String> next = range.next();
            System.out.println("count for " + next.key + ": " + value);
        }
        // close the iterator to release resources
        range.close();
        return;
    }
}
