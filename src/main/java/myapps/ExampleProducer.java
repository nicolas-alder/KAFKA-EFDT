package myapps;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

/**
 * Created by nicolashoeck on 30.05.19.
 */
public class ExampleProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
     //   props.put("bootstrap.servers", "localhost:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //     props.put("acks", "all");
    //    props.put("retries", 0);
    //    props.put("batch.size", 16384);
    //    props.put("linger.ms", 1);
     //   props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        Producer<String, Integer> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, Integer>("topic_i", "Wert", 1));
            System.out.println(new ProducerRecord<String, Integer>("topic_i", 0,"Wert", 1));

        producer.close();
    }
}
