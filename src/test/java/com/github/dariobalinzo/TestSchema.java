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

package com.github.dariobalinzo;


import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

import java.util.*;

import static com.github.dariobalinzo.Fixture.createMapDataset;
import static org.junit.Assert.assertEquals;

public class TestSchema  {


    @Test
    public void testMappingConvertList(){

        Map<String,Object> toConvert = new HashMap<>();

        List<String> stringList = Arrays.asList("uno", "due", "tre");
        toConvert.put("field1" , stringList);
        Schema schema = SchemaConverter.convertElasticMapping2AvroSchema(toConvert, "test");
        Struct struct = StructConverter.convertElasticDocument2AvroStruct(toConvert,schema);

        assertEquals(struct.getArray("field1"),stringList);

    }

    @Test
    public void testMappingConvertBoolean(){

        Map<String,Object> toConvert = new HashMap<>();

        toConvert.put("field1" , true);
        Schema schema = SchemaConverter.convertElasticMapping2AvroSchema(toConvert, "test");
        Struct struct = StructConverter.convertElasticDocument2AvroStruct(toConvert,schema);

        assertEquals(struct.getBoolean("field1"),true);

    }

    @Test
    public void testMappingConvertMap(){

        Map<String,Object> toConvert = new HashMap<>();
        Map<String,Object> mapField = new HashMap<>();

        mapField.put("subField","value1");
        mapField.put("subField2","value2");

        toConvert.put("field1" ,mapField);
        Schema schema = SchemaConverter.convertElasticMapping2AvroSchema(toConvert, "test");
        Struct struct = StructConverter.convertElasticDocument2AvroStruct(toConvert,schema);

        assertEquals(struct.getStruct("field1").get("subField"),"value1");

    }

    @Test
    public void completeObjectParsing(){

        Map<String, Object> dataset = createMapDataset();

        Schema schema = SchemaConverter.convertElasticMapping2AvroSchema(dataset, "test");
        Struct struct = StructConverter.convertElasticDocument2AvroStruct(dataset,schema);

        assertEquals(((List)struct.getStruct("join").get("columns")).size(),2);

    }



}
