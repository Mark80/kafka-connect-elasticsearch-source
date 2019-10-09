package com.github.dariobalinzo.task;

import org.apache.kafka.connect.source.SourceTaskContext;

import java.util.Collections;
import java.util.Map;

public class TaskContextClient {

    private SourceTaskContext context;
    private String INDEX;

    public TaskContextClient(String INDEX, SourceTaskContext context){
        this.INDEX = INDEX;
        this.context = context;

    }


    public Map<String, Object> getOffsetFromContext(String index) {
        return context.offsetStorageReader().offset(Collections.singletonMap(INDEX, index));
    }

}
