package com.hpi.msd;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


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
            if (!AttributeList.contains(key.split("_")[1]) && !key.split("_")[0].equals("label")) {
                AttributeList.add(key.split("_")[0].concat("_").concat(key.split("_")[1]));
            }
        }

        ArrayList<List<Double>> Atts = new ArrayList<List<Double>>();
        ArrayList<List<Double>> Lab = new ArrayList<List<Double>>();
        HashMap<String, Double> IGS = new HashMap<String, Double>();
        for (String Att : AttributeList) {
            Atts.clear();
            Lab.clear();
            for (String key : multimap.keySet()) {
                if (key.equals(Att)) {
                    Atts.add(multimap.get(key));
                }
                if (key.equals("label")) {
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
        double Labsum;
        try{
            Labsum=(Lab.get(0).get(1)+Lab.get(0).get(0));
        }catch (IndexOutOfBoundsException e){
            Labsum=(0+Lab.get(0).get(0));
        }

        double p0;
        try{
            p0 = (Lab.get(0).get(1)/Labsum);
        }catch (IndexOutOfBoundsException e){
            p0 = 0.0;

        }
        double HT = -p0 * Math.log(p0) / log2 - (1 - p0) * Math.log(1 - p0) / log2;
        if(java.lang.Double.isNaN(HT)){HT = 0;}

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
        double sum = 0.0;
        sum = list.stream().mapToDouble(Double::doubleValue).sum();
        //for (double i : list){
          //  sum = sum + i;}

        return sum;
    }

    public static double avg(List<Double> list) {

        double avg= sum(list)/list.size();

        return avg;
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

    public static double FindXCurrent(HashMap<String, Double> IG_collection, String attribute) {
        /* Method for finding X_Current */
        return IG_collection.get(attribute);

    }

    public static double Numberofevents(HashMap<String, Double> map) {
        /* Method for finding # of examples*/
        double number =0;
        for (String key : map.keySet()) {
            if (key.split("_")[0].equals("label")) {
                number += map.get(key);
            }
        }
        return number;
    }

    public static boolean HoeffdingSplit(double GXa, double Nullsplit, double epsilon) {
        /* Method for testing Hoeffding Split Criterion */

        if (GXa-Nullsplit>epsilon){return true;}
        return false;

    }

    public static double HoeffdingTreshold(double safety, double Numberofevents) {
        /* Method for finding epsilon */

        int R=2;
        double epsilon=Math.sqrt((-R*R*Math.log(1-safety))/(2*Numberofevents));
        return epsilon;
    }

    public static String Classlabel(HashMap<String,Double> map) {
        /* Find majority class at l */

        for (String key : map.keySet()){
            double key1 = 0.0;
            double key2 = 0.0;
            if (key.contains("label_0")){
                key1 = map.get(key);
            }
            else if (key.contains("label_1")){
                key2 =map.get(key);
            }
            if (key1>key2){
                //smth
            }
            else {
                //smth
            }
        }
        return "ENDE";
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
        map.put("node_label_0", 5.0);
        map.put("node_label_1", 9.0);

        HashMap<String, Double> map2 = new HashMap<String, Double>();
        map2.put("node_Outlook_Sunny_0", 2.0);
        map2.put("node_Outlook_Sunny_1", 3.0);
        map2.put("node_Outlook_Normal_0", 0.0);
        map2.put("node_Outlook_Normal_1", 4.0);
        map2.put("node_Outlook_Rainy_0", 3.0);
        map2.put("node_Outlook_Rainy_1", 2.0);
        map2.put("node_label_0", 5.0);
        map2.put("node_label_1", 9.0);


        List GXa_List = new ArrayList<Double>();
        List GX0_List = new ArrayList<Double>();

        //per node
        HashMap<String,Double> IGs= IG(map);
        System.out.println(IGs);
        double GXa= FindGXa(IGs);
        System.out.println(GXa);


        GX0_List.add(IGs.get("Nullsplit"));
        GXa_List.add(GXa);

        Double GXa_avg=avg(GX0_List);
        Double GX0_avg=avg(GXa_List);

        double Numberofevents=Numberofevents(map);
        double Epsilon = HoeffdingTreshold(0.95, Numberofevents);

        HoeffdingSplit(GXa_avg,GX0_avg,Epsilon);

        System.out.println(map);

        System.out.println(Classlabel(map));

        System.out.println(map.containsKey("label"));

        //TEST AREA
        HashMap<String,HashMap<String,Double>> NodeStore= new HashMap<>();
        NodeStore.put("node1",map);
        NodeStore.put("node2",map2);
        //System.out.println(NodeStore);

    }

    public static String FindGXaKey(HashMap<String, Double> IG_collection) {
        /* Method for finding key of X_a */
        String splitkey = null;
        for (String key : IG_collection.keySet()){
            if (IG_collection.get(key).equals(Collections.max(IG_collection.values()))){
                splitkey = key;
            }
        }
        return splitkey;
    }
}