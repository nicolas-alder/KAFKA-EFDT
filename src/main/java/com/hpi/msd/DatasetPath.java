package com.hpi.msd;

/**
 * Created by nicolashoeck on 03.09.19.
 */
public class DatasetPath {

    private static DatasetPath instance;
    String dataset_path;


    public void setDataset_path(String dataset_path) {
        this.dataset_path = dataset_path;
    }

    public String getDataset_path() {
        return dataset_path;
    }

    public static DatasetPath getInstance() {
        if (DatasetPath.instance == null) {
            DatasetPath.instance = new DatasetPath ();
        }
        return DatasetPath.instance;
    }

}
