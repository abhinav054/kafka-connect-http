package com.github.castorm.kafka.connect.http.response.spi;

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
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.io.IOException;
import java.util.stream.Stream;

import static com.fasterxml.jackson.core.JsonPointer.compile;

@RequiredArgsConstructor
public class KVSerializer {

    private static final JsonPointer JSON_ROOT = compile("/");

    public final ObjectMapper objectMapper;

    public KVSerializer(){
        this(new ObjectMapper());
    }

    @SneakyThrows(IOException.class)
    public JsonNode deserialize(byte[] body){
        return objectMapper.readTree(body);
    }

    @SneakyThrows(IOException.class)
    public String serialize(JsonNode node){
        return objectMapper.writeValueAsString(node);
    }

    public JsonNode getObjectAt(JsonNode node, JsonPointer pointer){
        return getRequiredAt(node, pointer);
    };

    public Stream<JsonNode> getArrayAt(JsonNode node, JsonPointer pointer){
        return null;
    };

    public static JsonNode getRequiredAt(JsonNode body, JsonPointer recordsPointer) {
        return JSON_ROOT.equals(recordsPointer) ? body : body.requiredAt(recordsPointer);
    }


}
