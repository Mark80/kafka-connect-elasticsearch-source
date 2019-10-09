package com.github.dariobalinzo.task;


import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Collections;
import java.util.Map;

public class SourceRecordBuilder {

    private static final String INDEX = "index";
    private static final String POSITION = "position";

    public static SourceRecord build(String index, String key, Map<String, Object> sourceAsMap, String incrementingField, String topicPrefix) {
        Map sourcePartition = Collections.singletonMap(INDEX, index);
        Map sourceOffset = Collections.singletonMap(POSITION, sourceAsMap.get(incrementingField).toString());
        Schema schema = SchemaConverter.convertElasticMapping2AvroSchema(sourceAsMap, index);
        Struct struct = StructConverter.convertElasticDocument2AvroStruct(sourceAsMap, schema);

        return new SourceRecord(
                sourcePartition,
                sourceOffset,
                topicPrefix + index,
                Schema.STRING_SCHEMA,
                key,
                schema,
                struct
        );
    }

}
