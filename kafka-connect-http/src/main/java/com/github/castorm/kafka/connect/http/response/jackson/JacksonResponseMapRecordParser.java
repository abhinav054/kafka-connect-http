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

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import com.github.castorm.kafka.connect.http.response.jackson.model.JacksonRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Configurable;

import static com.github.castorm.kafka.connect.common.CollectionUtils.merge;
import static java.util.Collections.emptyMap;


@RequiredArgsConstructor
@Slf4j
public class JacksonResponseMapRecordParser implements Configurable {

    private final Function<Map<String, ?>, JacksonRecordParserConfig> configFactory;

    private final JacksonRecordParser recordParser;

    private final JacksonMapSerializer serializer;

    private JsonPointer recordsPointer;

    JacksonRecordParserConfig sampleConfig;

    public JacksonResponseMapRecordParser(){this(new JacksonRecordParser(),new JacksonMapSerializer(new ObjectMapper()));}

    public JacksonResponseMapRecordParser(JacksonRecordParser recordParser, JacksonMapSerializer serializer){
        this(JacksonRecordParserConfig::new, recordParser, serializer);
    }

    @Override
    public void configure(Map<String, ?> settings){
        log.info("map parser settings ....................");
        log.info("!!!!!!!!!!!!!!!!"+settings);
        JacksonRecordParserConfig config = configFactory.apply(settings);
        sampleConfig = config;
        log.info("map records pointer ............");
        recordsPointer = config.getRecordsPointer();
    }

    Stream<JacksonRecord> getRecords(byte[] body) {

        JsonNode jsonBody = serializer.deserialize(body);
//        log.info("jsonbody ......."+jsonBody.toPrettyString());

        log.info("sample config .........."+sampleConfig.toString());
        recordsPointer = sampleConfig.getRecordsPointer();
        Map<String, Object> responseOffset = getResponseOffset(jsonBody);

        return serializer.getArrayAt(jsonBody, recordsPointer)
                .map(jsonRecord -> toJacksonRecord(jsonRecord, responseOffset));
    }

    private Map<String, Object> getResponseOffset(JsonNode node) {
        return emptyMap();
    }

    private JacksonRecord toJacksonRecord(JsonNode jsonRecord, Map<String, Object> responseOffset) {
        return JacksonRecord.builder()
                .key(recordParser.getKey(jsonRecord).orElse(null))
                .timestamp(recordParser.getTimestamp(jsonRecord).orElse(null))
                .offset(merge(responseOffset, recordParser.getOffset(jsonRecord)))
                .body(recordParser.getValue(jsonRecord))
                .build();
    }


}
