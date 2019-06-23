import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

package com.hpi.msd;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;


public class EFDT_InfoGain {
    /* Class for calculating the InformationGain within a node */


    public static HashMap<String, Double> IG(HashMap<String, Double> map) {
        /* Method for preprocessing and InformationGain handling
         returns dict(Attribute, InformationGain) (Nullsplit included) */

        ListMultimap<String, Double> multimap = ArrayListMultimap.create();
        for (String key : map.keySet()) {

            String delStr1 = "_0";
            String delStr2 = "_1";
            String key2 = key.replace(delStr1, "");
            key2 = key2.replace(delStr2, "");

            multimap.put(key2, map.get(key));
        }

        ArrayList<String> AttributeList = new ArrayList<String>();
        for (String key : map.keySet()) {
            if (!AttributeList.contains(key.split("_")[1]) && !key.split("_")[1].equals("Label")) {
                AttributeList.add(key.split("_")[1]);
            }
        }

        ArrayList<List<Double>> Atts = new ArrayList<List<Double>>();
        ArrayList<List<Double>> Lab = new ArrayList<List<Double>>();
        HashMap<String, Double> IGS = new HashMap<String, Double>();
        for (String Att : AttributeList) {
            Atts.clear();
            Lab.clear();
            for (String key : multimap.keySet()) {
                if (key.split("_")[1].equals(Att)) {
                    Atts.add(multimap.get(key));
                }
                if (key.split("_")[1].equals("Label")) {
                    Lab.add(multimap.get(key));
                }
            }
            IGS.put(Att,IG_calc(Atts,Lab));
        }

        double Nullsplit=Collections.min(Lab.get(0))/Collections.max(Lab.get(0));
        IGS.put("Nullsplit", Nullsplit);

        return IGS;
    }


    public static double IG_calc(ArrayList<List<Double>> Atts, ArrayList<List<Double>> Lab) {
        /* Method for calculating the InformationGain */

        double log2 = Math.log(2);
        double Labsum=(Lab.get(0).get(1)+Lab.get(0).get(0));

        double p0 = (Lab.get(0).get(1)/Labsum);
        double HT = -p0 * Math.log(p0) / log2 - (1 - p0) * Math.log(1 - p0) / log2;

        double HTa=0;
        for (List<Double> values: Atts){
            double p1= values.get(0)/sum(values);
            if (p1==0 || p1==1){
                HTa-=0;
            }
            else {
                HTa += - sum(values) / Labsum * ((p1 * Math.log(p1) / log2 + (1 - p1) * Math.log(1 - p1) / log2));
            }
        }

        double IG = HT-HTa;
        return IG;
    }

    public static double sum(List<Double> list) {
        double sum = 0;

        for (double i : list)
            sum = sum + i;

        return sum;
    }

    public static double FindGXa(HashMap<String, Double> IG_collection) {
        /* Method for finding X_a */

        for (String key : IG_collection.keySet()){
            if (IG_collection.get(key).equals(Collections.max(IG_collection.values()))){
                System.out.println("Attribute with highest InfoGain is " + key);
            }
        }
        double GXa=Collections.max(IG_collection.values());
        return GXa;
    }

    public static double Numberofevents(HashMap<String, Double> map) {
        /* Method for finding # of examples*/
        double number =0;
        for (String key : map.keySet()) {
            if (key.split("_")[1].equals("Label")) {
                number += map.get(key);
            }
        }
        return number;
    }

    public static double HoeffdingSplit(double GXa, double Nullsplit, double epsilon) {
        /* Method for testing Hoeffding Split Criterion */

        if (GXa-Nullsplit>epsilon){
            System.out.println("Split wird durchgeführt");
        }
        else {
            System.out.println("Split wird nicht durchgeführt");
        }
        //do action
        return 0;
    }

    public static double HoeffdingTreshold(double safety, double Numberofevents) {
        /* Method for finding epsilon */

        int R=2;
        double epsilon=Math.sqrt((-R*R*Math.log(1-safety))/(2*Numberofevents));
        return epsilon;
    }



    public static void main(String[] args) {
        /* main with example dict */

        HashMap<String, Double> map = new HashMap<String, Double>();
        map.put("node_Outlook_Sunny_0", 2.0);
        map.put("node_Outlook_Sunny_1", 3.0);
        map.put("node_Outlook_Normal_0", 0.0);
        map.put("node_Outlook_Normal_1", 4.0);
        map.put("node_Outlook_Rainy_0", 3.0);
        map.put("node_Outlook_Rainy_1", 2.0);
        map.put("node_Temp_Hot_0", 2.0);
        map.put("node_Temp_Hot_1", 2.0);
        map.put("node_Temp_Mild_0", 4.0);
        map.put("node_Temp_Mild_1", 2.0);
        map.put("node_Temp_Cold_0", 3.0);
        map.put("node_Temp_Cold_1", 1.0);
        map.put("node_Humidity_High_0", 4.0);
        map.put("node_Humidity_High_1", 3.0);
        map.put("node_Humidity_Normal_0", 1.0);
        map.put("node_Humidity_Normal_1", 6.0);
        map.put("node_Windy_True_0", 3.0);
        map.put("node_Windy_True_1", 3.0);
        map.put("node_Windy_False_0", 2.0);
        map.put("node_Windy_False_1", 6.0);
        map.put("node_Label_0", 5.0);
        map.put("node_Label_1", 9.0);


        HashMap<String,Double> IGs= IG(map);
        System.out.println(IGs);


        double GXa= FindGXa(IGs);
        double Numberofevents=Numberofevents(map);
        double epsilon = HoeffdingTreshold(0.95, Numberofevents);

        HoeffdingSplit(GXa,IGs.get("Nullsplit"),epsilon);
    }
}
