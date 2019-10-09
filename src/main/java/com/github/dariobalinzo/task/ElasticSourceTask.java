/*
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dariobalinzo.task;

import com.github.dariobalinzo.ElasticClient;
import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.utils.ElasticConnection;
import com.github.dariobalinzo.utils.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class ElasticSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceTask.class);
    private static final String INDEX = "index";

    private ElasticSourceTaskConfig config;
    private ElasticConnection es;

    private AtomicBoolean stopping = new AtomicBoolean(false);
    private List<String> indices;
    private long connectionRetryBackoff;
    private int maxConnectionAttempts;
    private String topic;
    private String incrementingField;
    private int size;
    private int pollingMs;
    //private Map<String, Integer> sent = new HashMap<>();
    private Offset offsetHandler;


    public ElasticSourceTask() {

    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            config = new ElasticSourceTaskConfig(properties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start ElasticSourceTask due to configuration error", e);
        }

        initEsConnection();

        indices = Arrays.asList(config.getString(ElasticSourceTaskConfig.INDICES_CONFIG).split(","));
        if (indices.isEmpty()) {
            throw new ConnectException("Invalid configuration: each ElasticSourceTask must have at "
                    + "least one index assigned to it");
        }

        topic = config.getString(ElasticSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
        incrementingField = config.getString(ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG);
        size = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG));
        pollingMs = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG));

        ElasticClient elasticClient = new ElasticClient(maxConnectionAttempts, connectionRetryBackoff, incrementingField, es);
        offsetHandler = new Offset(incrementingField, elasticClient,new TaskContextClient(INDEX,context));
    }

    private void initEsConnection() {

        final String esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);
        final int esPort = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.ES_PORT_CONF));

        final String esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        final String esPwd = config.getString(ElasticSourceConnectorConfig.ES_PWD_CONF);

        maxConnectionAttempts = Integer.parseInt(config.getString(
                ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        ));
        connectionRetryBackoff = Long.parseLong(config.getString(
                ElasticSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
        ));
        if (esUser == null || esUser.isEmpty()) {
            es = new ElasticConnection(
                    esHost,
                    esPort,
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );
        } else {
            es = new ElasticConnection(
                    esHost,
                    esPort,
                    esUser,
                    esPwd,
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );

        }

    }


    //will be called by connect with a different thread than the stop thread
    @Override
    public List<SourceRecord> poll() throws InterruptedException {

        List<SourceRecord> results = new ArrayList<>();
        indices.forEach(
                index -> {
                    if (!stopping.get()) {
                        logger.info("fetching from {}", index);
                        String lastValue = offsetHandler.fetchLastOffset(index);
                        logger.info("found last value {}", lastValue);
                        if (lastValue != null) {
                            executeScroll(index, lastValue, results);
                        }
                    }
                }
        );
        if (results.isEmpty()) {
            logger.info("no data found, sleeping for {} ms", pollingMs);
            Thread.sleep(pollingMs);
        }
        return results;
    }

    private void executeScroll(String index, String lastValue, List<SourceRecord> results) {

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.scroll(TimeValue.timeValueMinutes(1L));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                rangeQuery(incrementingField)
                        .from(lastValue, offsetHandler.isIncludeLower(index))
        ).sort(incrementingField, SortOrder.ASC); //TODO configure custom query
        searchSourceBuilder.size(1000);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        String scrollId = null;
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
            scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = parseSearchResult(index, results, searchResponse, scrollId);

            while (!stopping.get() && searchHits != null && searchHits.length > 0 && results.size() < size) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
                searchResponse = es.getClient().searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = parseSearchResult(index, results, searchResponse, scrollId);
            }
        } catch (Throwable t) {
            logger.error("error", t);
        } finally {
            closeScrollQuietly(scrollId);
        }


    }


    private SearchHit[] parseSearchResult(String index,List<SourceRecord> results, SearchResponse searchResponse, Object scrollId) {

        if (results.size() > size) {
            return null; //nothing to do: limit reached
        }

        SearchHits hits = searchResponse.getHits();
        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();

        logger.info("total shard {}, successuful: {}", totalShards, successfulShards);
        logger.info("retrived {}, scroll id : {}", hits, scrollId);

        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
            logger.error("failed {}", failure);
        }
        if (failedShards > 0) {
            throw new RuntimeException("failed shard in search");
        }

        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            SourceRecord sourceRecord = SourceRecordBuilder.build(index, hit.getId(), sourceAsMap, incrementingField, topic);
            results.add(sourceRecord);

            offsetHandler.updateLastOffset(index, sourceAsMap);
            //sent.merge(index, 1, Integer::sum);
        }
        return searchHits;
    }



    private void closeScrollQuietly(String scrollId) {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = es.getClient().clearScroll(clearScrollRequest);
        } catch (IOException e) {
            logger.error("error in clear scroll", e);
        }
        boolean succeeded = clearScrollResponse != null && clearScrollResponse.isSucceeded();
        logger.info("scroll {} cleared: {}", scrollId, succeeded);
    }

    //will be called by connect with a different thread than poll thread
    public void stop() {

        if (stopping != null) {
            stopping.set(true);
        }

        if (es != null) {
            es.closeQuietly();
        }

    }

}
