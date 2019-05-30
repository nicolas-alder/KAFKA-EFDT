package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * Created by nicolashoeck on 30.05.19.
 */
public class Main {
    public static void main(String[] args) {

               Properties props = new Properties();
               props.put(StreamsConfig.APPLICATION_ID_CONFIG, "treeworker");
               props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
               props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
               props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());

                StoreBuilder<KeyValueStore<String, Integer>> countStoreSupplier = Stores.keyValueStoreBuilder(
                Stores.inMemoryKeyValueStore("nodeStatistics"),
                Serdes.String(),
                Serdes.Integer())
                .withLoggingDisabled(); // disable backing up the store to a changelog topic

                Topology builder = new Topology();

                // add the source processor node that takes Kafka topic "source-topic" as input
                builder.addSource("Source", "topic_i")

                // add the AttributeCountProcessorAPI node which takes the source processor as its upstream processor
                .addProcessor("node_i", () -> new AttributeCountProcessorAPI(), "Source")

                // add the count store associated with the AttributeCountProcessorAPI processor
                .addStateStore(countStoreSupplier, "node_i")

                // add the sink processor node that takes Kafka topic "sink-topic" as output
                // and the AttributeCountProcessorAPI node as its upstream processor
                .addSink("node_i_plus_1", "topic_i_plus_1", "node_i")
                .addSink("node_i_plus_2", "topic_i_plus_2", "node_i");



                KafkaStreams streaming = new KafkaStreams(builder, props);
                streaming.start();

    }
}
