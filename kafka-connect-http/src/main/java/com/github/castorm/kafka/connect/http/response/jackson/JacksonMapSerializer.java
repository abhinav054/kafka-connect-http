package com.github.castorm.kafka.connect.http.response.jackson;

/*-
 * #%L
 * Kafka Connect HTTP
 * %%
 * Copyright (C) 2020 - 2021 Cástor Rodríguez
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.castorm.kafka.connect.http.response.spi.KVSerializer;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class JacksonMapSerializer extends KVSerializer {

    public  JacksonMapSerializer(){}

    public JacksonMapSerializer(ObjectMapper objectMapper){
        super(objectMapper);
    }


    @Override
    public Stream<JsonNode> getArrayAt(JsonNode node, JsonPointer pointer) {
        log.info("pointer ..........................................................................................................");
        log.info("pointer ......."+pointer.toString());
        JsonNode object  = getRequiredAt(node,pointer);

        List<JsonNode> array = new ArrayList<>();
        for(Iterator<Map.Entry<String,JsonNode>> it = object.fields();it.hasNext();) {
            Map.Entry<String, JsonNode> ob = it.next();
            JsonNode value = ob.getValue();
            array.add(value);
        }
        return array.stream();
    }
}
