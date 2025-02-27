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
import com.github.castorm.kafka.connect.http.response.timestamp.spi.TimestampParser;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;
import java.util.stream.Stream;

import static com.github.castorm.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParserTest.Fixture.*;
import static com.github.castorm.kafka.connect.http.response.jackson.JacksonKvRecordHttpResponseParserTest.Fixture.record;
import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.parse;
import static java.util.Collections.emptyMap;
import static java.util.UUID.nameUUIDFromBytes;
import static java.util.stream.Stream.empty;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class JacksonKvMapRecordHttpResponseParserTest {

    JacksonKvMapRecordHttpResponseParser parser;

    @Mock
    JacksonKvMapRecordHttpResponseParserConfig config;

    @Mock
    JacksonResponseMapRecordParser responseParser;


    @Mock
    TimestampParser timestampParser;

    @BeforeEach
    void setUp(){
        parser = new JacksonKvMapRecordHttpResponseParser(__ -> config);
        given(config.getResponseParser()).willReturn(responseParser);
        given(config.getTimestampParser()).willReturn(timestampParser);
        parser.configure(emptyMap());
    }

    @Test
    void givenNoItems_thenEmpty() {

        givenRecords(empty());

        assertThat(parser.parse(response)).isEmpty();
    }

    @Test
    void givenOneItem_thenKeyMapped() {

        givenRecords(Stream.of(record.withKey("key")));

        assertThat(parser.parse(response)).first().extracting(KvRecord::getKey).isEqualTo("key");
    }

    @Test
    void givenOneItemWithNoKeyButOffset_thenKeyMappedFromOffset() {

        givenRecords(Stream.of(record.withKey(null).withOffset(ImmutableMap.of("key", "value"))));

        assertThat(parser.parse(response)).first().extracting(KvRecord::getKey).isEqualTo("value");
    }

    @Test
    void givenOneItemWithNoNoKey_thenKeyDefault() {

        givenRecords(Stream.of(record.withKey(null)));

        assertThat(parser.parse(response)).first().extracting(KvRecord::getKey).isNotNull();
    }

    @Test
    void givenOneItem_thenValueMapped() {

        givenRecords(Stream.of(record.withBody("value")));

        assertThat(parser.parse(response)).first().extracting(KvRecord::getValue).isEqualTo("value");
    }

    @Test
    void givenOneItem_thenTimestampMapped() {

        givenRecords(Stream.of(record.withTimestamp(timestampIso)));
        given(timestampParser.parse(timestampIso)).willReturn(timestamp);

        assertThat(parser.parse(response)).first().extracting(KvRecord::getOffset).extracting(Offset::getTimestamp).isEqualTo(Optional.of(timestamp));
    }

    @Test
    void givenOneItemWitNoTimestampButOffset_thenTimestampMappedFromOffset() {

        givenRecords(Stream.of(record.withTimestamp(null).withOffset(ImmutableMap.of("timestamp", timestampIso))));
        given(timestampParser.parse(timestampIso)).willReturn(timestamp);

        assertThat(parser.parse(response)).first().extracting(KvRecord::getOffset).extracting(Offset::getTimestamp).isEqualTo(Optional.of(timestamp));
    }

    @Test
    void givenOneItemWithNoTimestamp_thenDefault() {

        givenRecords(Stream.of(record.withTimestamp(null)));

        assertThat(parser.parse(response)).first().extracting(KvRecord::getOffset).extracting(Offset::getTimestamp).isNotNull();
    }

    @Test
    void givenOneItem_thenOffsetMapped() {

        givenRecords(Stream.of(record.withOffset(ImmutableMap.of("offset-key", "offset-value"))));

        assertThat(parser.parse(response).stream().findFirst().get().getOffset().toMap().get("offset-key")).isEqualTo("offset-value");
    }

    @Test
    void givenOneItem_thenTimestampMappedToOffset() {

        givenRecords(Stream.of(record.withTimestamp(timestampIso).withOffset(emptyMap())));
        given(timestampParser.parse(timestampIso)).willReturn(timestamp);

        assertThat(parser.parse(response).stream().findFirst().get().getOffset().getTimestamp()).contains(parse(timestampIso));
    }

    @Test
    void givenOneItem_thenKeyMappedToOffset() {

        givenRecords(Stream.of(record.withKey("value").withOffset(emptyMap())));

        assertThat(parser.parse(response).stream().findFirst().get().getOffset().getKey()).contains("value");
    }

    @Test
    void givenOneItemWithNoKey_thenConsistentUUIDMappedToOffset() {

        givenRecords(Stream.of(record.withKey(null).withOffset(emptyMap())));

        assertThat(parser.parse(response).stream().findFirst().get().getOffset().getKey()).contains(nameUUIDFromBytes(record.getBody().toString().getBytes()).toString());
    }


    private void givenRecords(Stream<JacksonRecord> records) {
        given(responseParser.getRecords(bytes)).willReturn(records);
    }

    interface Fixture {
        byte[] bytes = "bytes".getBytes();
        HttpResponse response = HttpResponse.builder().body(bytes).build();
        Instant timestamp = ofEpochMilli(43L);
        String timestampIso = timestamp.toString();
        JacksonRecord record = JacksonRecord.builder().body("Binary Value").build();
    }

}
