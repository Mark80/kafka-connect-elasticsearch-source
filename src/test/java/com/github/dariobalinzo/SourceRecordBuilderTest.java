package com.github.dariobalinzo;


import com.github.dariobalinzo.task.SourceRecordBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.Map;

import static com.github.dariobalinzo.Fixture.createMapDataset;
import static org.junit.Assert.assertEquals;

public class SourceRecordBuilderTest {

    private Map<String, Object> dataset = createMapDataset();

    @Test
    public void sourceRecordCreation() {
        SourceRecord sourceRecord = SourceRecordBuilder.build("dataset", "asdfadsfds", dataset, "createdAt", "es_");

        assertEquals(sourceRecord.topic(),"es_dataset");
        assertEquals(sourceRecord.key(),"asdfadsfds");
        assertEquals(Long.valueOf((String)sourceRecord.sourceOffset().get("position")),Long.valueOf(1570455768701L));

    }




}
