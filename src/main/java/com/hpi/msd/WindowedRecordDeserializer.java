package com.hpi.msd;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nicolashoeck on 31.05.19.
 */
public class RecordDeserializer implements Deserializer<Record>{
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Record deserialize(String s, byte[] bytes) {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        HashMap<String,Integer> msg = null;
        try {
            ois = new ObjectInputStream(new BufferedInputStream(byteStream));
            msg = (HashMap<String, Integer>) ois.readObject();
            ois.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Record record = new Record(msg);
        return record;
    }

    @Override
    public void close() {

    }
}
