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
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueStore;
import com.google.common.collect.ArrayListMultimap;


import java.util.HashMap;

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
        multimap.put("GX0",Double.valueOf(0.5));
        multimap.put("splitAttribute", null);
        this.kvStore.put("node0",multimap);

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

    public HashMap iterateTree(int node, KeyValueStore tree){


        Multimap nodeMap = (Multimap) tree.get("node".concat(Integer.toString(node)));

        //update statistics

        // Check if node is leaf
        if(tree.get("node".concat(Integer.toString(2*node)))== null){
            //attemptToSplit
        }else{
            //reevaluate Split

            String splitAttribute = (String) nodeMap.get("splitAttribute").iterator().next();
            iterateTree()
        }


    }



}
