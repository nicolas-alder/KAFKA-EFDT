package com.hpi.msd;

import com.google.common.collect.Multimap;
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
public class MultimapDeserializer implements Deserializer<Multimap>{
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Multimap deserialize(String s, byte[] bytes) {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = null;
        Multimap<String,Object> msg = null;
        try {
            ois = new ObjectInputStream(new BufferedInputStream(byteStream));
            msg = (Multimap<String,Object>) ois.readObject();
            ois.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        return msg;
    }

    @Override
    public void close() {

    }
}
