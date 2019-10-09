package com.github.dariobalinzo;

import com.github.dariobalinzo.task.ElasticResponse;
import com.github.dariobalinzo.task.Offset;
import com.github.dariobalinzo.task.TaskContextClient;
import org.elasticsearch.action.search.SearchResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.*;

public class OffsetTest {

    private ElasticClient elasticClient = mock(ElasticClient.class);
    private TaskContextClient contextClient = mock(TaskContextClient.class);
    private String index = "index";

    @Before
    public void resetMock(){
        reset(elasticClient,contextClient);
    }


    @Test
    public void fetchLastOffsetFromCache() {

        Map<String, String> offsetCache = new HashMap<>();
        String expectedOffset = "1234";
        offsetCache.put(index, expectedOffset);
        Offset offsetTest = new Offset("createAt", elasticClient,offsetCache, contextClient);

        String offset = offsetTest.fetchLastOffset(index);

        Assert.assertEquals(offset, expectedOffset);

    }

    @Test
    public void fetchLastOffsetFromTaskContext() {

        Map<String, Object> offsetContext = new HashMap<>();
        String expectedOffset = "1234";
        offsetContext.put("position", expectedOffset);

        when(contextClient.getOffsetFromContext(index)).thenReturn(offsetContext);

        Offset offsetTest = new Offset("createAt", elasticClient, new HashMap<>(),contextClient);

        String offset = offsetTest.fetchLastOffset(index);
        Assert.assertEquals(expectedOffset,offset);

    }

    @Test
    public void fetchLastOffsetFromElastic() {

        final String expectedOffset = "1234";
        ElasticResponse response = mockResponse(expectedOffset);

        when(contextClient.getOffsetFromContext(index)).thenReturn(null);
        when(elasticClient.getDocumentOrderedByIncrementingField(index)).thenReturn(response);

        Offset offsetTest = new Offset("createAt", elasticClient, new HashMap<>(),contextClient);

        String offset = offsetTest.fetchLastOffset(index);
        Assert.assertEquals(expectedOffset,offset);

    }

    private ElasticResponse mockResponse(String expectedOffset) {
        return new ElasticResponse(){
            @Override
            public int getTotalShards() {
                return 1;
            }

            @Override
            public int getSuccessfulShards() {
                return 1;
            }

            @Override
            public SearchResponse getSearchResponse() {
                return super.getSearchResponse();
            }

            @Override
            public List<String> getIncrementalValues() {
                return Collections.singletonList(expectedOffset);
            }
        };
    }


}
