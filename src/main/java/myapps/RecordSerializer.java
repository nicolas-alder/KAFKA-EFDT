package myapps;

import org.apache.kafka.common.serialization.Serializer;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Map;

/**
 * Created by nicolashoeck on 31.05.19.
 */
public class RecordSerializer implements Serializer<Record> {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Record o) {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(5000);
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(new BufferedOutputStream(byteStream));
            oos.writeObject(o.getMap());
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] sendBuf = byteStream.toByteArray();
        return sendBuf;
    }

    @Override
    public void close() {

    }
}
