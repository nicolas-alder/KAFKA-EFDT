package myapps;

import java.util.HashMap;

/**
 * Created by nicolashoeck on 31.05.19.
 */
public class Record {
    HashMap<String,String> map;

    public Record(HashMap<String,String> map){
        this.map = map;
    }

    public HashMap<String, String> getMap() {
        return map;
    }

    public void setMap(HashMap<String, String> map) {
        this.map = map;
    }
}
