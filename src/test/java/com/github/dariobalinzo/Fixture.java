package com.github.dariobalinzo;

import java.util.*;

public class Fixture {

    public static Map<String, Object> createMapDataset(){
        Map<String,Object> dataset = new HashMap<>();

        List<String> features  = new ArrayList<>();
        features.add("a");
        features.add("b");
        features.add("c");


        Map<String,Object> join = new HashMap<>();

        Map<String,Object> join1 = new HashMap<>();
        join1.put("left","colomnA");
        join1.put("right","colomnB");

        Map<String,Object> join2 = new HashMap<>();
        join2.put("left","colomnC");
        join2.put("right","colomnD");

        join.put("columns", Arrays.asList(join1,join2));
        join.put("joinType","INNER");

        Map<String,Object> column = new HashMap<>();
        column.put("nullable",true);
        column.put("colType","string");
        column.put("name","day");

        dataset.put("features",features);
        dataset.put("createdAt",1570455768701L);
        dataset.put("name","datasetName");
        dataset.put("cmSchema","schemaid");
        dataset.put("join",join);
        dataset.put("additionalColumns", Collections.singletonList(column));
        return dataset;
    }

}
