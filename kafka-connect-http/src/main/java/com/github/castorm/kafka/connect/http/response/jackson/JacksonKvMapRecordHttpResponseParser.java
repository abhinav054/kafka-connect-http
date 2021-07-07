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

import com.github.castorm.kafka.connect.http.model.HttpResponse;
import com.github.castorm.kafka.connect.http.model.Offset;
import com.github.castorm.kafka.connect.http.record.model.KvRecord;
import com.github.castorm.kafka.connect.http.response.jackson.model.JacksonRecord;
import com.github.castorm.kafka.connect.http.response.spi.KvRecordHttpResponseParser;
import com.github.castorm.kafka.connect.http.response.timestamp.spi.TimestampParser;
import lombok.RequiredArgsConstructor;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Optional.ofNullable;
import static java.util.UUID.nameUUIDFromBytes;

@RequiredArgsConstructor
public class JacksonKvMapRecordHttpResponseParser implements KvRecordHttpResponseParser {

    private final Function<Map<String, ?>,JacksonKvMapRecordHttpResponseParserConfig> configFactory;

    private JacksonResponseMapRecordParser responseParser;

    private TimestampParser timestampParser;


    public JacksonKvMapRecordHttpResponseParser(){this(JacksonKvMapRecordHttpResponseParserConfig::new);}

    @Override
    public void configure(Map<String,?> configs){
        JacksonKvMapRecordHttpResponseParserConfig config = configFactory.apply(configs);
        responseParser = config.getResponseParser();
        timestampParser = config.getTimestampParser();
    }

    @Override
    public List<KvRecord> parse(HttpResponse response) {
        return responseParser.getRecords(response.getBody())
                .map(this::map)
                .collect(Collectors.toList());
    }

    private KvRecord map(JacksonRecord record) {

        Map<String, Object> offsets = record.getOffset();

        String key = ofNullable(record.getKey())
                .map(Optional::of)
                .orElseGet(() -> ofNullable(offsets.get("key")).map(String.class::cast))
                .orElseGet(() -> generateConsistentKey(record.getBody()));

        Optional<Instant> timestamp = ofNullable(record.getTimestamp())
                .map(Optional::of)
                .orElseGet(() -> ofNullable(offsets.get("timestamp")).map(String.class::cast))
                .map(timestampParser::parse);

        Offset offset = timestamp
                .map(ts -> Offset.of(offsets, key, ts))
                .orElseGet(() -> Offset.of(offsets, key));

        return KvRecord.builder()
                .key(key)
                .value(record.getBody())
                .offset(offset)
                .build();
    }

    private static String generateConsistentKey(String body) {
        return nameUUIDFromBytes(body.getBytes()).toString();
    }
}
