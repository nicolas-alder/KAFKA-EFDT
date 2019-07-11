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


/**
 * Created by nicolashoeck on 30.05.19.
 */
public class ExampleProducer {
    public static void main(String[] args) {
        /*
        Properties props = new Properties();
    //    props.put("bootstrap.servers", "localhost:9092");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    //    props.put("acks", "all");
    //    props.put("retries", 0);
    //    props.put("batch.size", 16384);
    //    props.put("linger.ms", 1);
    //    props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "myapps.RecordSerializer");


        HashMap<String,String> map = new HashMap<>();
        map.put("Ampelfarbe","gruen");
        map.put("Geschwindigkeit",">50km/h");
        map.put("Fahrzeug","fahren");

        Record record = new Record(map);

        Producer<String, Record> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++){
            producer.send(new ProducerRecord<String, Record>("input", "record_seq", record));

            System.out.println(record.getMap().toString());}
        producer.close();
        */
        Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.hpi.msd.RecordSerializer");
        Producer<String, HashMap> producer = new KafkaProducer<>(props);
     //   for (int i = 0; i < 100; i++) {


        try {
            // Lese Zeile für Zeile ein. Ergebnis ist ein Dictionary mit Key = Attribut und Value = Attributausprägung
            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader("/Users/nicolashoeck/KAFKA-EFDT/src/main/java/com/hpi/msd/new_KreditD2.csv"));
            Map<String, String> values;
            while((values = reader.readMap()) != null){

                // Zu versendener Record wird erstellt als Dictionary
                HashMap<String,Double> map = new HashMap<>();
                String label = values.get("label");
                for (Map.Entry<String,String> value: values.entrySet()) {
                    map.put(value.getKey()+"_"+value.getValue()+"_"+label,1.0);}

                // send record to topic
                producer.send(new ProducerRecord<String, HashMap>("aggregatedinput", "record_seq", map));
                System.out.println("Send!");
               /* try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                */
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not read file.");
        }
        //}
        producer.close();


    }


}
