package com.github.dariobalinzo;/*
© 2019 Nokia.All rights reserved
 */

import com.github.dariobalinzo.task.ElasticResponse;
import com.github.dariobalinzo.utils.ElasticConnection;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;

public class ElasticClient {

    private static final Logger logger = LoggerFactory.getLogger(ElasticClient.class);

    private int maxConnectionAttempts;
    private long connectionRetryBackoff;
    private String incrementingField;
    private ElasticConnection es;

    public ElasticClient(int maxConnectionAttempts,
                         long connectionRetryBackoff, String incrementingField, ElasticConnection es) {

        this.maxConnectionAttempts = maxConnectionAttempts;
        this.connectionRetryBackoff = connectionRetryBackoff;
        this.incrementingField = incrementingField;
        this.es = es;

    }

    public ElasticResponse getDocumentOrderedByIncrementingField(String index) {
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                matchAllQuery()
        ).sort(incrementingField, SortOrder.ASC);
        searchSourceBuilder.size(1); // only one record
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        try {
            for (int i = 0; i < maxConnectionAttempts; ++i) {
                try {
                    searchResponse = es.getClient().search(searchRequest);
                    break;
                } catch (IOException e) {
                    logger.error("error in scroll");
                    Thread.sleep(connectionRetryBackoff);
                }
            }
            if (searchResponse == null) {
                throw new RuntimeException("connection failed");
            }
        } catch (Exception e) {
            logger.error("error fetching min value", e);
            return null;
        }
        return new ElasticResponse(searchResponse,incrementingField);
    }


}
