package com.hpi.msd;

import com.google.common.collect.*;
import com.opencsv.CSVReaderHeaderAware;

import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;
import org.graphstream.ui.layout.HierarchicalLayout;
import org.graphstream.ui.view.Viewer;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class TreeworkerProcessor implements Processor<String,HashMap> {

    private ProcessorContext context;
    private KeyValueStore<String, ListMultimap> kvStore;
    private Graph graph;
    private Viewer viewer;
    private HierarchicalLayout layout;
    private String dataset_path;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.dataset_path = DatasetPath.getInstance().getDataset_path();

        this.kvStore = (KeyValueStore) context.getStateStore("treestructure");

        ListMultimap<String, Object> multimap = ArrayListMultimap.create();
        multimap.put("GXA",0.6); // null nicht möglich, weil dann Fehler in sum von avg berechnun von GXA nullpointerexception
        multimap.put("GXA_seen",1.0);
        multimap.put("GX0",0.5);
        multimap.put("GX0_seen",1.0);
        multimap.put("splitAttribute", "0");
        multimap.put("XCurrent",0.6);
        multimap.put("XCurrent_seen",1.0);
        multimap.put("childList", new HashMap<>());
        initializeNodeAttributes(multimap);

        this.kvStore.put("node0",multimap);
        ListMultimap<String, Object> savedNodes = ArrayListMultimap.create();
        savedNodes.put("savedNodes",0);
        this.kvStore.put("savedNodes",savedNodes);

        this.graph = new SingleGraph("efdtGraph");
        graph.addAttribute("ui.quality");
        this.viewer = graph.display();
        HierarchicalLayout hl = new HierarchicalLayout();
        this.layout = hl;
        viewer.enableAutoLayout(this.layout);
    }

    @Override
    public void process(String key, HashMap value) {
        KeyValueStore treeStore = kvStore;
        iterateTree(0,treeStore,value);

        ListMultimap<String, Object> savedNodesMultimap = (ListMultimap) treeStore.get("savedNodes");
        int max_node = (int) savedNodesMultimap.get("savedNodes").get(savedNodesMultimap.get("savedNodes").size()-1);

        int current_node = 0;
        graph.clear();

        while(current_node<=max_node){
            ListMultimap<String, Object> nodeMultimap = (ListMultimap<String, Object>) treeStore.get("node".concat(Integer.toString(current_node)));
            if(nodeMultimap == null) {current_node++;continue;}

            try{graph.addNode(Integer.toString(current_node));}catch (Exception e){//System.out.println("Knoten " + Integer.toString(current_node) + " bereits angelegt");
            }

            HashMap childs = (HashMap) nodeMultimap.get("childList").iterator().next();
            Iterator child_iterator = childs.entrySet().iterator();
            while (child_iterator.hasNext()) {
                Map.Entry pair = (Map.Entry)child_iterator.next();

                try{
                    graph.addNode(Integer.toString((int) pair.getValue()));
                    if(current_node==0){this.layout.setRoots(Integer.toString((int) pair.getValue()));}
                }catch (Exception e){//System.out.println("Knoten " + Integer.toString(current_node) + " bereits angelegt");
                }

                try{
                    graph.addEdge(((String) pair.getKey()).concat(Integer.toString((int) pair.getValue())),Integer.toString(current_node),Integer.toString((int) pair.getValue()));
                    Edge edge = graph.getEdge(((String) pair.getKey()).concat(Integer.toString((int) pair.getValue())));
                    edge.addAttribute("ui.label", (String) pair.getKey());
                }catch (Exception e){//System.out.println("Edge " + Integer.toString(current_node) + " bereits angelegt");
                }
            }

            //Annotate with split attribute or label
            try{
                String label_splitAttribute = (String) nodeMultimap.get("splitAttribute").iterator().next();
                Node node = graph.getNode(Integer.toString(current_node));
                node.addAttribute("ui.label", label_splitAttribute);
            }catch (Exception e){//System.out.println("Knoten " + Integer.toString(current_node) + " bereits angelegt");
            }
            current_node++;
        }
    }

    @Override
    public void close() {}

    public void iterateTree(int node, KeyValueStore tree, HashMap value){
        graph.addAttribute("ui.screenshot", "/Users/nicolashoeck/KAFKA-EFDT/src/main/java/com/hpi/msd/screenshot.png");

        //System.out.println(node);
        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));

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

        HashMap currentNodeChildList =  (HashMap) nodeMap.get("childList").iterator().next();
        boolean hasNoChild = currentNodeChildList.isEmpty();

        // Check if node is leaf
        if(hasNoChild){attemptToSplit(node, tree);
        }else{
            //if no split at reevaluate and everything stays the same
            if(reEvaluateBestSplit(node, tree)){
                HashMap childList = (HashMap) nodeMap.get("childList").iterator().next();
                Iterator allChilds = childList.entrySet().iterator();
                while(allChilds.hasNext()){
                    Map.Entry pair = (Map.Entry)allChilds.next();

                    //call method recursive for child nodes. only propagate attribute in hashmap that are left over after the split (no split attributes)
                    if(value.containsKey(splitAttribute.concat("_").concat((String) pair.getKey()).concat("_1"))){
                        value.remove(splitAttribute.concat("_").concat((String) pair.getKey()).concat("_1"));
                        iterateTree((int) pair.getValue(), tree,value);
                    }else if(value.containsKey(splitAttribute.concat("_").concat((String) pair.getKey()).concat("_0"))){
                        value.remove(splitAttribute.concat("_").concat((String) pair.getKey()).concat("_0"));
                        iterateTree((int) pair.getValue(), tree,value);
                    }
                }
            }
            // if reevaluate method killed a subtree/changed the tree, we are finished and do not need to iterate through any child nodes
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
            if((key.equalsIgnoreCase("splitAttribute") || key.equalsIgnoreCase("GXA")|| key.equalsIgnoreCase("GXA_seen") || key.equalsIgnoreCase("GX0"))|| key.equalsIgnoreCase("GX0_seen") || key.equalsIgnoreCase("childList") || key.equalsIgnoreCase("XCurrent")|| key.equalsIgnoreCase("XCurrent_seen")){continue;}
            attributeHashMap.put(key, (Double) nodeMap.get(key).iterator().next());
        }

        double count_label0 = attributeHashMap.get("label_0_0");
        double count_label1 = attributeHashMap.get("label_1_1");
        if(count_label0>count_label1){nodeMap.removeAll("splitAttribute");nodeMap.put("splitAttribute", "0");}else{nodeMap.removeAll("splitAttribute");nodeMap.put("splitAttribute", "1");}
        tree.put("node".concat(Integer.toString(node)),nodeMap);

        HashMap<String,Double> IGs= EFDT_InfoGain.IG(attributeHashMap);
        double GXa_single= EFDT_InfoGain.FindGXa(IGs);
        String GXa_key = EFDT_InfoGain.FindGXaKey(IGs);
        double GX0_single=IGs.get("Nullsplit");

        double gxa_seen = (double) nodeMap.get("GXA_seen").iterator().next();
        nodeMap.removeAll("GXA_seen");
        nodeMap.put("GXA_seen",gxa_seen+1.0);
        double gx0_seen = (double) nodeMap.get("GX0_seen").iterator().next();
        nodeMap.removeAll("GX0_seen");
        nodeMap.put("GX0_seen",gx0_seen+1.0);

        Double GX0_average= EFDT_InfoGain.updateGX((double) nodeMap.get("GX0").iterator().next(),GX0_single,(double) nodeMap.get("GX0_seen").iterator().next());
        Double GXA_average= EFDT_InfoGain.updateGX((double) nodeMap.get("GXA").iterator().next(),GXa_single,(double) nodeMap.get("GXA_seen").iterator().next());

        nodeMap.removeAll("GXA");
        nodeMap.removeAll("GX0");
        nodeMap.put("GXA",GXA_average);
        nodeMap.put("GX0",GX0_average);

        double numberofevents=EFDT_InfoGain.Numberofevents(attributeHashMap);
        double epsilon = EFDT_InfoGain.HoeffdingTreshold(0.9, numberofevents);
        System.out.println("Epsilon: " + epsilon);
        System.out.println("GXA: "+GXA_average + "(" + GXa_key + ")");
        System.out.println("GXO: "+GX0_average + "(Nullsplit)");
        if(!(EFDT_InfoGain.HoeffdingSplit(GXA_average,GX0_average,epsilon) && !GXa_key.equalsIgnoreCase("Nullsplit"))){tree.put("node".concat(Integer.toString(node)),nodeMap);return;}

        // write all new childs of the split to childlists with schema "discrete attribute value: child node id"
        String best_attribute = GXa_key.split("_")[0];
        nodeMap.removeAll("splitAttribute");
        nodeMap.put("splitAttribute", best_attribute);
        HashMap childs = new HashMap();
        for (String element:attributeHashMap.keySet()) {
            if(element.split("_")[0].equalsIgnoreCase(best_attribute)){
                String attribute_value = element.split("_")[1];
                childs.put(attribute_value,null);
            }
        }
        tree.put("node".concat(Integer.toString(node)),nodeMap);

        // give childs their own ids
        Iterator childsIterator = childs.entrySet().iterator();
        ArrayList<Integer> newNodes = getNewNodeID(tree, childs.entrySet().size());
        Iterator newNodesIt = newNodes.iterator();
        HashMap newChilds = new HashMap();
        while(newNodesIt.hasNext()){
           Map.Entry pair = (Map.Entry)childsIterator.next();
           newChilds.put(pair.getKey(),newNodesIt.next());
        }
        childs.putAll(newChilds);


       // initialize new childs
        childsIterator = childs.entrySet().iterator();
        while(childsIterator.hasNext()){
           Map.Entry pair = (Map.Entry)childsIterator.next();
           createNewNode((int) pair.getValue(),tree, node);
        }
        nodeMap.removeAll("childList");
        nodeMap.put("childList", childs);

        tree.put("node".concat(Integer.toString(node)),nodeMap);

        return;
    }

    public ArrayList<Integer> getNewNodeID(KeyValueStore tree, int amount){

        Multimap encapsulatedSavedNodes = (Multimap) tree.get("savedNodes");
        List<Integer> savedNodes = (List<Integer>) encapsulatedSavedNodes.get("savedNodes");
        ArrayList<Integer> returnNodes = new ArrayList<>();
        int max = Collections.max(savedNodes);

        for (int j = 1; j<=amount;j++){
            returnNodes.add(max+j);
            encapsulatedSavedNodes.put("savedNodes", Collections.max(savedNodes)+1);
        }

        tree.put("savedNodes",encapsulatedSavedNodes);

        return returnNodes;
    }

    public void createNewNode(int nodeID, KeyValueStore tree, int parentnode){

        ListMultimap<String, Object> multimap = ArrayListMultimap.create();
        multimap.put("GXA",0.6);
        multimap.put("GXA_seen",1.0);
        multimap.put("GX0",0.5);
        multimap.put("GX0_seen",1.0);
        multimap.put("splitAttribute", "0");
        multimap.put("childList", new HashMap<>());
        multimap.put("XCurrent",0.4);
        multimap.put("XCurrent_seen",1.0);
        initializeNodeAttributes(multimap, (ListMultimap) tree.get("node".concat(Integer.toString(parentnode))));
        tree.put("node".concat(Integer.toString(nodeID)),multimap);
    }

    public boolean reEvaluateBestSplit(int node, KeyValueStore tree){

        ListMultimap nodeMap = (ListMultimap) tree.get("node".concat(Integer.toString(node)));
        Iterator nodeMapIterator = nodeMap.keySet().iterator();
        HashMap<String, Double> attributeHashMap = new HashMap();

        while (nodeMapIterator.hasNext()){
            String key = (String) nodeMapIterator.next();
            if((key.equalsIgnoreCase("splitAttribute") || key.equalsIgnoreCase("GXA")|| key.equalsIgnoreCase("GXA_seen") || key.equalsIgnoreCase("GX0"))|| key.equalsIgnoreCase("GX0_seen") || key.equalsIgnoreCase("childList") || key.equalsIgnoreCase("XCurrent")|| key.equalsIgnoreCase("XCurrent_seen")){continue;}
            attributeHashMap.put(key, (Double) nodeMap.get(key).iterator().next());
        }

        HashMap<String,Double> IGs= EFDT_InfoGain.IG(attributeHashMap);
        double GXa_single= EFDT_InfoGain.FindGXa(IGs);
        String GXa_key = EFDT_InfoGain.FindGXaKey(IGs);
        String xCurrent = (String) Iterables.getLast(((List) nodeMap.get("splitAttribute")));
        double XCurrent_Infogain = EFDT_InfoGain.FindXCurrent(IGs, xCurrent);

        double xCurrent_seen = (double) nodeMap.get("XCurrent_seen").iterator().next();
        nodeMap.removeAll("XCurrent_seen");
        nodeMap.put("XCurrent_seen",xCurrent_seen+1.0);
        double gxa_seen = (double) nodeMap.get("GXA_seen").iterator().next();
        nodeMap.removeAll("GXA_seen");
        nodeMap.put("GXA_seen",gxa_seen+1.0);

        System.out.println("XCurrent: "+ xCurrent);
        System.out.println("XCurrent: "+ nodeMap.get("XCurrent").iterator().next());

        double XCurrent_average= EFDT_InfoGain.updateGX((double) nodeMap.get("XCurrent").iterator().next(),XCurrent_Infogain,(double) nodeMap.get("XCurrent_seen").iterator().next());
        double GXA_average= EFDT_InfoGain.updateGX((double) nodeMap.get("GXA").iterator().next(),GXa_single,(double) nodeMap.get("GXA_seen").iterator().next());

        nodeMap.removeAll("XCurrent");
        nodeMap.removeAll("GXA");
        nodeMap.put("XCurrent",XCurrent_average);
        nodeMap.put("GXA",GXA_average);

        tree.put("node".concat(Integer.toString(node)), nodeMap);

        double treshold = EFDT_InfoGain.HoeffdingTreshold(0.99,EFDT_InfoGain.Numberofevents(attributeHashMap));
        if(!((GXA_average-XCurrent_average)>treshold)){return true;}

        if(GXa_key.equals("Nullsplit")){
            killSubtree(node, tree);
            System.out.println("Subtree Kill XCurrent: "+XCurrent_average);
            System.out.println("Subtree Kill GXA: "+GXA_average);
            System.out.println("Subtree Kill Epsilon: "+treshold);

            //Change to Leaf
            ListMultimap<String, Object> multimap = ArrayListMultimap.create();
            multimap.put("GXA",0.6); // null nicht möglich, weil dann Fehler in sum von avg berechnun von GXA nullpointerexception
            multimap.put("GXA_seen",1.0);
            multimap.put("GX0",0.5); // GXO Berechnung wahrscheinlich falsch (von: Henrik)
            multimap.put("GX0_seen",1.0);
            multimap.put("splitAttribute", "0");
            multimap.put("XCurrent",0.6);
            multimap.put("XCurrent_seen",1.0);
            multimap.put("childList", new HashMap<>());

            initializeNodeAttributes(multimap, nodeMap);
            tree.put("node".concat(Integer.toString(node)),multimap);

            return false;

        }else if(!GXa_key.equals(xCurrent)){

            killSubtree(node, tree);
            System.out.println("Subtree Kill XCurrent: "+XCurrent_average);
            System.out.println("Subtree Kill GXA: "+GXA_average);
            System.out.println("Subtree Kill Epsilon: "+treshold);

            ListMultimap<String, Object> multimap = ArrayListMultimap.create();
            multimap.put("GXA",0.6);
            multimap.put("GXA_seen",1.0);
            multimap.put("GX0",0.5);
            multimap.put("GX0_seen",1.0);
            multimap.put("splitAttribute", GXa_key.split("_")[0]);
            multimap.put("XCurrent",GXA_average);
            multimap.put("XCurrent_seen",1.0);
            multimap.put("childList", new HashMap<>());
            initializeNodeAttributes(multimap, nodeMap);

            // write all new child nodes of the split in childlist with schema "discrete attribute value : child node id"
            HashMap childs = new HashMap();

            for (String element:attributeHashMap.keySet()) {
                if(element.split("_")[0].equalsIgnoreCase(GXa_key.split("_")[0])){
                    String attribute_value = element.split("_")[1];
                    childs.put(attribute_value,null);
                }
            }

            // Give childs their own child ids
            Iterator childsIterator = childs.entrySet().iterator();
            ArrayList<Integer> newNodes = getNewNodeID(tree, childs.entrySet().size());
            Iterator newNodesIt = newNodes.iterator();
            HashMap newChilds = new HashMap();
            while(newNodesIt.hasNext()){
                Map.Entry pair = (Map.Entry)childsIterator.next();
                newChilds.put(pair.getKey(),newNodesIt.next());
            }

            childs.putAll(newChilds);

            // initialize new childs
            childsIterator = childs.entrySet().iterator();

            while(childsIterator.hasNext()){
                Map.Entry pair = (Map.Entry)childsIterator.next();
                createNewNode((int) pair.getValue(),tree, node);
            }

            multimap.removeAll("childList");
            multimap.put("childList", childs);
            tree.put("node".concat(Integer.toString(node)),multimap);

            return false;
        }
        return true;
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



    public void initializeNodeAttributes(ListMultimap<String,Object> multimap){

        HashSet attribute_combinations = new HashSet();

        try {
            // Read line by line. Result is dictionary with key = attribute and value = discrete attribute value.
            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(this.dataset_path));
            Map<String, String> values;

            while((values = reader.readMap()) != null){
                String label = values.get("label");

                for (Map.Entry<String,String> value: values.entrySet()) {
                    // Sort out statistics that are split attributes or unsuitable
                        attribute_combinations.add(value.getKey() + "_" + value.getValue() + "_" + label);}
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not read file.");
        }

        Iterator attribute_combinations_iterator = attribute_combinations.iterator();

        while(attribute_combinations_iterator.hasNext()) {multimap.put((String) attribute_combinations_iterator.next(),0.0);}
    }

    public void initializeNodeAttributes(ListMultimap<String,Object> multimap, ListMultimap parentAttributesMultiMap){

        String splitattribute = (String) parentAttributesMultiMap.get("splitAttribute").iterator().next();
        if (splitattribute.equalsIgnoreCase("0")||splitattribute.equalsIgnoreCase("1")){splitattribute="keinAttribut";}
        HashSet attribute_combinations = new HashSet();

        try {
            // Read line by line. Result is dictionary with key = attribute and value = discrete attribute value.
            CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(dataset_path));
            Map<String, String> values;

            while((values = reader.readMap()) != null){
                String label = values.get("label");

                for (Map.Entry<String,String> value: values.entrySet()) {
                    // Sort out statistics that are split attributes or unsuitable
                    if (parentAttributesMultiMap.containsKey(value.getKey() + "_" + value.getValue() + "_" + label) && !((value.getKey() + "_" + value.getValue() + "_" + label).contains(splitattribute))) {
                        attribute_combinations.add(value.getKey() + "_" + value.getValue() + "_" + label);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Could not read file.");
        }

        Iterator attribute_combinations_iterator = attribute_combinations.iterator();
        while(attribute_combinations_iterator.hasNext()) {multimap.put((String) attribute_combinations_iterator.next(),0.0);}
    }
}

