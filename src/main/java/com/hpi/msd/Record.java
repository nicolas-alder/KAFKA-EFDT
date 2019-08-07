package com.hpi.msd;

import java.util.HashMap;

/**
 * Created by nicolashoeck on 31.05.19.
 */
public class Record {
    HashMap<String,Integer> map;

    public Record(HashMap<String,Integer> map){
        this.map = map;
    }

    public HashMap<String, Integer> getMap() {
        return map;
    }

    public void setMap(HashMap<String, Integer> map) {
        this.map = map;
    }

}
