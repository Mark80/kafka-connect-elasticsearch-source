package com.github.dariobalinzo.task;

import com.github.dariobalinzo.ElasticClient;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Offset {

    private static final Logger logger = LoggerFactory.getLogger(Offset.class);
    private static final String POSITION = "position";
    private String incrementingField;
    private ElasticClient elClient;
    private TaskContextClient contextClient;
    private Map<String, String> last;


    public Offset(String incrementingField, ElasticClient elClient,TaskContextClient contextClient) {
        this.incrementingField = incrementingField;
        this.elClient = elClient;
        this.contextClient = contextClient;
        this.last = new HashMap<>();
    }

    public Offset(String incrementingField, ElasticClient elClient,Map<String, String> last,TaskContextClient contextClient) {
        this.incrementingField = incrementingField;
        this.elClient = elClient;
        this.last = last;
        this.contextClient = contextClient;
    }


    public String fetchLastOffset(String index) {

        if (last.get(index) != null) {
            return last.get(index);
        }

        Map<String, Object> offset = getOffsetFromContext(index);
        if (offset != null) {
            return (String) offset.get(POSITION);
        } else {

            ElasticResponse searchResponse =  elClient.getDocumentOrderedByIncrementingField(index);

            if (searchResponse == null)
                return null;
            List<String> hits = searchResponse.getIncrementalValues();
            int totalShards = searchResponse.getTotalShards();
            int successfulShards = searchResponse.getSuccessfulShards();
            logErrorAndThrowException(searchResponse);
            logger.info("total shard {}, successuful: {}", totalShards, successfulShards);

            return hits.get(0);

        }

    }

    private Map<String, Object> getOffsetFromContext(String index) {
        return contextClient.getOffsetFromContext(index);
    }


    private void logErrorAndThrowException(ElasticResponse searchResponse) {
        int failedShards = searchResponse.getFailedShards();

        if (failedShards > 0) {
            for (ShardSearchFailure failure : searchResponse.getFailure()) {
                // failures should be handled here
                //logger.error("failed {}", failure);
            }
            throw new RuntimeException("failed shard in search");
        }
    }

    public void updateLastOffset(String index, Map<String, Object> sourceAsMap) {
        last.put(index, sourceAsMap.get(incrementingField).toString());
    }

    public boolean isIncludeLower(String index) {
        return last.get(index) == null;
    }


}
