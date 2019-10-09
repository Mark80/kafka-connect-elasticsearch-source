package com.github.dariobalinzo.task;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public  class ElasticResponse {

    private  ShardSearchFailure[] failure;
    private  int failedShards;
    private  int totalShards;
    private  int successfulShards;
    private String incrementingField;
    private SearchResponse searchResponse;
    private List<String> incrementalValues;


    public ElasticResponse(){}

    public ElasticResponse(SearchResponse searchResponse,String incrementingField){
        this.searchResponse = searchResponse;
        this.incrementalValues = getFirstHit(searchResponse.getHits().getHits());
        this.totalShards = searchResponse.getTotalShards();
        this.successfulShards = searchResponse.getSuccessfulShards();
        this.incrementingField = incrementingField;
        this.failedShards = searchResponse.getFailedShards();
        this.failure = searchResponse.getShardFailures();
    }

    private List<String> getFirstHit(SearchHit[] searchHits) {
        return Arrays.stream(searchHits).map(hit -> hit.getSourceAsMap().get(incrementingField).toString()).collect(Collectors.toList());
    }

    public int getTotalShards() {
        return totalShards;
    }

    public int getSuccessfulShards() {
        return successfulShards;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    public List<String> getIncrementalValues() {
        return incrementalValues;
    }

    public int getFailedShards() {
        return failedShards;
    }

    public ShardSearchFailure[] getFailure() {
        return failure;
    }
}
