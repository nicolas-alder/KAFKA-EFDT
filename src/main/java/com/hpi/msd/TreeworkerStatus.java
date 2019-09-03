package com.hpi.msd;

import java.sql.Timestamp;

/**
 * Created by nicolashoeck on 03.09.19.
 */
public class TreeworkerStatus {

    Timestamp last_insertion = new Timestamp(System.currentTimeMillis());

    private static TreeworkerStatus instance;

    public void setLast_insertion(Timestamp last_insertion) {
        this.last_insertion = last_insertion;
    }

    public Timestamp getLast_insertion() {
        return last_insertion;
    }

    public static TreeworkerStatus getInstance() {
            if (TreeworkerStatus.instance == null) {
                TreeworkerStatus.instance = new TreeworkerStatus ();
            }
            return TreeworkerStatus.instance;
        }

    }


