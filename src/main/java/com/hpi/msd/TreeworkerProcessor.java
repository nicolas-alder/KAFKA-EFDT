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

import com.google.common.collect.*;
import javafx.util.Pair;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueStore;


import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hpi.msd.EFDT_InfoGain.avg;

public class TreeworkerProcessor implements Processor<String,HashMap> {

    private ProcessorContext context;
    private KeyValueStore<String, ListMultimap> kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        // keep the processor context locally because we need it in punctuate() and commit()
        //this.context = context;
        this.kvStore = (KeyValueStore) context.getStateStore("treestructure");


            ListMultimap<String, Object> multimap = ArrayListMultimap.create();
            multimap.put("GXA",0.6); // null nicht möglich, weil dann Fehler in sum von avg berechnun von GXA nullpointerexception
            multimap.put("GXA_seen",1.0);
            multimap.put("GX0",0.5); // GXO Berechnung wahrscheinlich falsch (von: Henrik)
            multimap.put("GX0_seen",1.0);
            multimap.put("splitAttribute", "0");
            multimap.put("XCurrent",0.4);
            multimap.put("XCurrent_seen",1.0);
            multimap.put("childList", new HashMap<>());
            multimap.put("label_1_1",0.0);
            multimap.put("label_0_0",0.0);


            this.kvStore.put("node0",multimap);
            ListMultimap<String, Object> savedNodes = ArrayListMultimap.create();
            savedNodes.put("savedNodes",1);
            this.kvStore.put("savedNodes",savedNodes);


        // retrieve the key-value store named "nodeStatistics"
        //kvStore = (KeyValueStore) context.getStateStore("nodeStatistics");

    }

    @Override
    public void process(String key, HashMap value) {
       // KeyValueStore tree = (KeyValueStore) this.context.getStateStore("treestructure");
        System.out.println("Key: " + key + " Record: "+value.toString());
        iterateTree(0,this.kvStore,value);
       Multimap nodeMap = (Multimap) this.kvStore.get("node".concat(Integer.toString(0)));
       System.out.println("Bla");
    //    context.forward(key,value);
        //context.commit( );
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
    }

    @Override
    public void close() {
        // nothing to do
    }

    public void iterateTree(int node, KeyValueStore tree, HashMap value){

        System.out.println(node);
        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));
        String angefragteNode = "node".concat(Integer.toString(node));
        //update statistics
        Iterator itValue = value.entrySet().iterator();

        while (itValue.hasNext()) {
            HashMap.Entry pair = (HashMap.Entry)itValue.next();
            double oldvalue;
            Collection oldValuesCollection;
            oldvalue = 0.0;
            if(!nodeMap.get(pair.getKey()).isEmpty()){
                oldvalue = (double) nodeMap.get(pair.getKey()).iterator().next();
                nodeMap.removeAll(pair.getKey());
                nodeMap.put(pair.getKey(), oldvalue + (double) pair.getValue());
                continue;
            }
                nodeMap.removeAll(pair.getKey());
                nodeMap.put(pair.getKey(), oldvalue + (double) pair.getValue());
        }
        tree.put("node".concat(Integer.toString(node)),nodeMap);
        String splitAttribute = (String) nodeMap.get("splitAttribute").iterator().next();
        //Multimap testMap = (Multimap) tree.get("node".concat(Integer.toString(node)));


        HashMap currentNodeChildList =  (HashMap) nodeMap.get("childList").iterator().next();
       boolean hasNoChild = currentNodeChildList.isEmpty();
        // Check if node is leaf
        if(hasNoChild){
            attemptToSplit(node, tree);
        }else{
            //falls bei reevaluate kein split sich verändert und alles bleibt wie es ist:
            if(reEvaluateBestSplit(node, tree)){
                HashMap childList = (HashMap) nodeMap.get("childList").iterator().next();
                Iterator allChilds = childList.entrySet().iterator();
                while(allChilds.hasNext()){
                    Map.Entry pair = (Map.Entry)allChilds.next();
                    //rufe Methode rekursiv auf für Kindsknoten. nur attribute in hashmap weitergeben, die nach dem split übrigbleiben!
                    iterateTree((int) pair.getValue(), tree,value);
                }
            }
            //falls reevaluate subtree gekillt hat/veärndert hat, sind wir fertig in der iteration
            return;

        }

        return;
    }

    public void attemptToSplit(int node, KeyValueStore tree){

        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));
        Iterator nodeMapIterator = nodeMap.keySet().iterator();
        HashMap<String, Double> attributeHashMap = new HashMap();

        while (nodeMapIterator.hasNext()){
            String key = (String) nodeMapIterator.next();
            if((key.equalsIgnoreCase("splitAttribute") || key.equalsIgnoreCase("GXA") || key.equalsIgnoreCase("GX0"))|| key.equalsIgnoreCase("childList") || key.equalsIgnoreCase("XCurrent")){continue;}
            attributeHashMap.put(key, (Double) nodeMap.get(key).iterator().next());
        }

        double count_label0 = attributeHashMap.get("label_0_0");
        double count_label1 = attributeHashMap.get("label_1_1");
        if(count_label0>count_label1){nodeMap.put("splitAttribute", "0");}else{nodeMap.put("splitAttribute", "1");}
        tree.put("node".concat(Integer.toString(node)),nodeMap);

        //if(count_label0 == 0 || count_label1 == 0){return;}

        HashMap<String,Double> IGs= EFDT_InfoGain.IG(attributeHashMap);
        double GXa= EFDT_InfoGain.FindGXa(IGs);
        String GXa_key = EFDT_InfoGain.FindGXaKey(IGs);
       // System.out.println(GXa);

        nodeMap.put("GXA",GXa);
        nodeMap.put("GX0",IGs.get("Nullsplit"));

        Double GX0_avg= avg((List<Double>) nodeMap.get("GX0"));
        Double GXa_avg= avg((List<Double>) nodeMap.get("GXA"));

        double numberofevents=EFDT_InfoGain.Numberofevents(attributeHashMap);
        double epsilon = EFDT_InfoGain.HoeffdingTreshold(0.95, numberofevents);
        System.out.println(epsilon);
        tree.put("node".concat(Integer.toString(node)),nodeMap);
        System.out.println("GXA: "+GXa_avg);
        System.out.println("GXO: "+GX0_avg);
        if(!(EFDT_InfoGain.HoeffdingSplit(GXa_avg,GX0_avg,epsilon) && !GXa_key.equalsIgnoreCase("Nullsplit"))){return;}

        // Schreibe in childList alle neuen Kinder des Splits nach dem Schema "attributausprägung: KindknotenID"
        String best_attribute = GXa_key.split("_")[0];
        nodeMap.put("splitAttribute", best_attribute);
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
       Iterator newNodesIt = newNodes.iterator();
        HashMap newChilds = new HashMap();
        while(newNodesIt.hasNext()){
           Map.Entry pair = (Map.Entry)childsIterator.next();
           newChilds.put(pair.getKey(),newNodesIt.next());
       }
        childs.putAll(newChilds);

        // for (int newNode:newNodes) {childs.put(childsIterator.next(),newNode);}

       // Initialisiere neue Kinder
        childsIterator = childs.entrySet().iterator();
       while(childsIterator.hasNext()){
           Map.Entry pair = (Map.Entry)childsIterator.next();
           createNewNode((int) pair.getValue(),tree);
       }
        nodeMap.removeAll("childList");
        nodeMap.put("childList", childs);
        tree.put("node".concat(Integer.toString(node)),nodeMap);

        return;



    }

    public ArrayList<Integer> getNewNodeID(KeyValueStore tree, int amount){
        Multimap encapsulatedSavedNodes = (Multimap) tree.get("savedNodes");
        List<Integer> savedNodes = (List<Integer>) encapsulatedSavedNodes.get("savedNodes");
        List<Integer> range =  IntStream.range(0, Collections.max(savedNodes)).boxed().collect(Collectors.toList());
        range.removeAll(savedNodes);
        ArrayList<Integer> returnNodes = new ArrayList<>();
        if(range.size()<amount-1){
            for (int j = 0; j<range.size()-1;j++){returnNodes.add(range.get(j));((Multimap) tree.get("savedNodes")).put("savedNodes",range.get(j));}
            for (int j = Collections.max(savedNodes); j < Collections.max(savedNodes) + amount; j++){returnNodes.add(Collections.max(savedNodes)+j);((Multimap) tree.get("savedNodes")).put("savedNodes",Collections.max(savedNodes)+j);}
            return returnNodes;
        }
        for (int j = 0; j<amount-1;j++){
            returnNodes.add(range.get(j));
        }
        return returnNodes;

    }

    public void createNewNode(int nodeID, KeyValueStore tree){
        ListMultimap<String, Object> multimap = ArrayListMultimap.create();
        multimap.put("GXA",0.6);
        multimap.put("GX0",0.5);
        multimap.put("splitAttribute", "0");
        multimap.put("childList", new HashMap<>());
        multimap.put("label_1_1",0.0);
        multimap.put("label_0_0",0.0);
        tree.put("node".concat(Integer.toString(nodeID)),multimap);
    }

    public boolean reEvaluateBestSplit(int node, KeyValueStore tree){
        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));
        Iterator nodeMapIterator = nodeMap.keySet().iterator();
        HashMap<String, Double> attributeHashMap = new HashMap();

        while (nodeMapIterator.hasNext()){
            String key = (String) nodeMapIterator.next();
            if((key.equalsIgnoreCase("splitAttribute") || key.equalsIgnoreCase("GXA") || key.equalsIgnoreCase("GX0"))|| key.equalsIgnoreCase("childList") || key.equalsIgnoreCase("XCurrent")){continue;}
            attributeHashMap.put(key, (Double) nodeMap.get(key).iterator().next());
        }

        HashMap<String,Double> IGs= EFDT_InfoGain.IG(attributeHashMap);
        double GXa= EFDT_InfoGain.FindGXa(IGs);
        String GXa_key = EFDT_InfoGain.FindGXaKey(IGs);

        String xCurrent = (String) Iterables.getLast(((List) nodeMap.get("splitAttribute")));
        double XCurrent_Infogain = EFDT_InfoGain.FindXCurrent(IGs, xCurrent);
        nodeMap.put("XCurrent",XCurrent_Infogain);
        nodeMap.put("GXA",GXa);
        double XCurrent_average = EFDT_InfoGain.avg((List<Double>) nodeMap.get("XCurrent"));
        double GXA_average  =EFDT_InfoGain.avg((List<Double>) nodeMap.get("GXA"));

        double treshold = EFDT_InfoGain.HoeffdingTreshold(0.95,EFDT_InfoGain.Numberofevents(attributeHashMap));
        if(!((GXA_average-XCurrent_average)>treshold)){return true;}

        if(GXa_key.equals("Nullsplit")){
            killSubtree(node, tree);
            //Change to Leaf
            ListMultimap<String, Object> multimap = ArrayListMultimap.create();
            multimap.put("GXA",null);
            multimap.put("GX0",0.5);
            multimap.put("splitAttribute", "0");
            multimap.put("XCurrent",null);
            multimap.put("childList", new HashMap<>());
            tree.put("node".concat(Integer.toString(node)),multimap);
        }else if(!GXa_key.equals(xCurrent)){
            killSubtree(node, tree);
            ListMultimap<String, Object> multimap = ArrayListMultimap.create();
            multimap.put("GXA",null);
            multimap.put("GX0",0.5);
            multimap.put("splitAttribute", GXa_key.split("_")[0]);
            multimap.put("XCurrent",GXa_key.split("_")[0]);
            multimap.put("childList", new HashMap<>());

            // Schreibe in childList alle neuen Kinder des Splits nach dem Schema "attributausprägung: KindknotenID"
            HashMap childs = new HashMap();
            for (String element:attributeHashMap.keySet()) {
                if(element.split("_")[0].equalsIgnoreCase(GXa_key.split("_")[0])){
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
            multimap.put("childList", childs);
            tree.put("node".concat(Integer.toString(node)),multimap);
        }

        return false;

    }

    public void killSubtree(int node, KeyValueStore tree){
        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));
        HashMap<String, Integer> childList = (HashMap) nodeMap.get("childList").iterator().next();
        Iterator childIterator = childList.entrySet().iterator();
        while(childIterator.hasNext()){
            Map.Entry child = (Map.Entry)childIterator.next();
            killSubtree((int) child.getValue(),tree);
            tree.delete("node".concat(Integer.toString((int) child.getValue())));
        }
        return;
    }


}
