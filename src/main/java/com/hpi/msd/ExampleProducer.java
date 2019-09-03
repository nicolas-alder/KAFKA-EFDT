package com.hpi.msd;

import com.opencsv.CSVReaderHeaderAware;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.StreamsConfig;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ExampleProducer {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.hpi.msd.RecordSerializer");
        Producer<String, HashMap> producer = new KafkaProducer<>(props);

        try {
            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader("/Users/nicolashoeck/Downloads/Datensaetze/Bank/Shuffled/Bank_S_train1.csv"));
            Map<String, String> values;
            while((values = reader.readMap()) != null){

                // Record is created as dictionary
                HashMap<String,Double> map = new HashMap<>();
                String label = values.get("label");
                for (Map.Entry<String,String> value: values.entrySet()) {map.put(value.getKey()+"_"+value.getValue()+"_"+label,1.0);}

                // Send record to topic "aggregatedinput". Each key is the same, which is important for the "AggregateApp". Though it is not used because AggregateApp class is deprecated.
                producer.send(new ProducerRecord<String, HashMap>("aggregatedinput", "record_seq", map));}

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not read file.");
        }
        producer.close();
    }
}
