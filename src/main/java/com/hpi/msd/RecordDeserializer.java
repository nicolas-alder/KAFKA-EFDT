package com.hpi.msd;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map;

public class RecordDeserializer implements Deserializer<HashMap>{

    @Override
    public void configure(Map map, boolean b) {}

    @Override
    public HashMap deserialize(String s, byte[] bytes) {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        HashMap<String,HashMap> msg = null;
        try {
            ois = new ObjectInputStream(new BufferedInputStream(byteStream));
            msg = (HashMap<String, HashMap>) ois.readObject();
            ois.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return msg;
    }

    @Override
    public void close() {}
}
