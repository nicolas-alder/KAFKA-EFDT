/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hpi.msd;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import javafx.util.Pair;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueStore;
import com.google.common.collect.ArrayListMultimap;


import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class TreeworkerProcessor implements Processor<Windowed<String>, HashMap> {

    private ProcessorContext context;
    private KeyValueStore<String, ListMultimap> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = context;
        ListMultimap<String, Object> multimap = ArrayListMultimap.create();
        multimap.put("GXA",null);
        multimap.put("GX0",0.5);
        multimap.put("splitAttribute", "0");
        multimap.put("childList", new HashMap<>());
        this.kvStore.put("node0",multimap);
        ListMultimap<String, Object> savedNodes = ArrayListMultimap.create();
        savedNodes.put("savedNodes",1);
        this.kvStore.put("savedNodes",savedNodes);

        // retrieve the key-value store named "nodeStatistics"
        //kvStore = (KeyValueStore) context.getStateStore("nodeStatistics");

    }

    @Override
    public void process(Windowed<String> key, HashMap value) {
        kvStore = (KeyValueStore) context.getStateStore("treeStructure");
        System.out.println("Key: " + key + " Record: "+value.toString());
       /* Iterator it = value.getMap().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry)it.next();
            String recordKey = (String) pair.getKey();
            String recordValue = (String) pair.getValue();
            String compound_key = recordKey.concat("_".concat(recordValue));
            Integer current_count = this.kvStore.get(compound_key);
            if (current_count == null){current_count=0;}
            this.kvStore.put(compound_key,current_count+1);
            System.out.println(compound_key + ": " + Integer.toString(current_count+1));

        }
        */
        context.forward(key,value);
        context.commit();
    }

    @Override
    public void close() {
        // nothing to do
    }

    public HashMap iterateTree(int node, KeyValueStore tree, HashMap value){


        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));

        //update statistics
        Iterator itValue = value.entrySet().iterator();

        while (itValue.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)itValue.next();
            double oldvalue;
            Collection oldValuesCollection;
            oldvalue = 0.0;
            if(!(oldValuesCollection = nodeMap.get(pair.getKey())).equals(null)){
                oldvalue = (double) oldValuesCollection.iterator().next();}

            nodeMap.put(pair.getKey(), oldvalue + (double) pair.getValue());
        }
        tree.put("node".concat(Integer.toString(node)),nodeMap);
        String splitAttribute = (String) nodeMap.get("splitAttribute").iterator().next();


        // Check if node is leaf
        if(tree.get("node".concat(Integer.toString(2* node))) == null){
            attemptToSplit(node, tree);
        }else{
            //reevaluate Split

            //iterateTree() nur attribute weitergeben, die nach dem split übrigbleiben!
        }

        return new HashMap();
    }

    public void attemptToSplit(int node, KeyValueStore tree){

        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));
        Iterator nodeMapIterator = nodeMap.keySet().iterator();
        HashMap<String, Double> attributeHashMap = new HashMap();

        while (nodeMapIterator.hasNext()){
            String key = (String) nodeMapIterator.next();
            if((key.equalsIgnoreCase("splitAttribute") || key.equalsIgnoreCase("GXA") || key.equalsIgnoreCase("GX0"))|| key.equalsIgnoreCase("childList")){continue;}
            attributeHashMap.put(key, (Double) nodeMap.get(key).iterator().next());
        }

        double count_label0 = attributeHashMap.get("label_0_0");
        double count_label1 = attributeHashMap.get("label_1_1");
        if(count_label0>count_label1){tree.put("splitAttribute", "0");}else{tree.put("splitAttribute", "1");}

        if(count_label0 == 0 || count_label1 == 0){return;}

        HashMap<String,Double> IGs= EFDT_InfoGain.IG(attributeHashMap);
        double GXa= EFDT_InfoGain.FindGXa(IGs);
        String GXa_key = EFDT_InfoGain.FindGXaKey(IGs);
       // System.out.println(GXa);

        nodeMap.put("GXA",GXa);
        nodeMap.put("GX0",IGs.get("Nullsplit"));

        Double GXa_avg=EFDT_InfoGain.avg((List<Double>) nodeMap.get("GXA"));
        Double GX0_avg=EFDT_InfoGain.avg((List<Double>) nodeMap.get("GX0"));

        double numberofevents=EFDT_InfoGain.Numberofevents(attributeHashMap);
        double epsilon = EFDT_InfoGain.HoeffdingTreshold(0.95, numberofevents);

        if(!(EFDT_InfoGain.HoeffdingSplit(GXa_avg,GX0_avg,epsilon) && !GXa_key.equalsIgnoreCase("Nullsplit"))){return;}

        // Schreibe in childList alle neuen Kinder des Splits nach dem Schema "attributausprägung: KindknotenID"
        String best_attribute = GXa_key.split("_")[0];
        tree.put("splitAttribute", best_attribute);
        HashMap childs = new HashMap();
        for (String element:attributeHashMap.keySet()) {
            if(element.split("_")[0].equalsIgnoreCase(best_attribute)){
                String attribute_value = element.split("_")[1];
                childs.put(attribute_value,null);
            }
        }

        // Weise den Kindern jeweils ihre eigene KnotenID zu
       Iterator childsIterator = childs.entrySet().iterator();
       ArrayList<Integer> newNodes = getNewNodeID(tree, childs.entrySet().size());
       for (int newNode:newNodes) {childs.put(childsIterator.next(),newNode);}

       // Initialisiere neue Kinder
        childsIterator = childs.entrySet().iterator();
       while(childsIterator.hasNext()){
           Map.Entry pair = (Map.Entry)childsIterator.next();
           createNewNode((int) pair.getValue(),tree);
       }
        tree.put("childList", childs);

        return;



    }

    public ArrayList<Integer> getNewNodeID(KeyValueStore tree, int amount){
        Multimap encapsulatedSavedNodes = (Multimap) tree.get("savedNodes");
        List<Integer> savedNodes = (List<Integer>) encapsulatedSavedNodes.get("savedNodes");
        List<Integer> range =  IntStream.range(0, Collections.max(savedNodes)).boxed().collect(Collectors.toList());
        range.removeAll(savedNodes);
        ArrayList<Integer> returnNodes = new ArrayList<>();
        if(range.size()<amount){
            for (int j = 0; j<range.size();j++){returnNodes.add(range.get(j));((Multimap) tree.get("savedNodes")).put("savedNodes",range.get(j));}
            for (int j = Collections.max(savedNodes); j < Collections.max(savedNodes) + amount; j++){returnNodes.add(Collections.max(savedNodes)+j);((Multimap) tree.get("savedNodes")).put("savedNodes",Collections.max(savedNodes)+j);}
            return returnNodes;
        }
        for (int j = 0; j<amount;j++){
            returnNodes.add(range.get(j));
        }
        return returnNodes;

    }

    public void createNewNode(int nodeID, KeyValueStore tree){
        ListMultimap<String, Object> multimap = ArrayListMultimap.create();
        multimap.put("GXA",null);
        multimap.put("GX0",0.5);
        multimap.put("splitAttribute", "0");
        multimap.put("childList", new HashMap<>());
        this.kvStore.put("node".concat(Integer.toString(nodeID)),multimap);
    }

    public void reEvaluateBestSplit(int node, KeyValueStore tree){
        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));
        Iterator nodeMapIterator = nodeMap.keySet().iterator();
        HashMap<String, Double> attributeHashMap = new HashMap();

        while (nodeMapIterator.hasNext()){
            String key = (String) nodeMapIterator.next();
            if((key.equalsIgnoreCase("splitAttribute") || key.equalsIgnoreCase("GXA") || key.equalsIgnoreCase("GX0"))|| key.equalsIgnoreCase("childList")){continue;}
            attributeHashMap.put(key, (Double) nodeMap.get(key).iterator().next());
        }

        HashMap<String,Double> IGs= EFDT_InfoGain.IG(attributeHashMap);
        double GXa= EFDT_InfoGain.FindGXa(IGs);
        String GXa_key = EFDT_InfoGain.FindGXaKey(IGs);

        //hier gehts weiter

    }


}
