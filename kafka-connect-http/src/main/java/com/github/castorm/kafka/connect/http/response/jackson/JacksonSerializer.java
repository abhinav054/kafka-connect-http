package com.github.castorm.kafka.connect.http.response.jackson;

/*-
 * #%L
 * kafka-connect-http
 * %%
 * Copyright (C) 2020 CastorM
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
import java.util.stream.Stream;

import static java.util.stream.StreamSupport.stream;

class JacksonSerializer extends KVSerializer {



    public JacksonSerializer(){}

    public JacksonSerializer(ObjectMapper objectMapper){
        super(objectMapper);
    }


    @Override
    public Stream<JsonNode> getArrayAt(JsonNode node, JsonPointer pointer) {
        JsonNode array = getRequiredAt(node, pointer);
        if (array.isArray()) {
            return stream(array.spliterator(), false);
        } else if (array.isNull()) {
            return Stream.empty();
        }
        return Stream.of(array);
    }

}
