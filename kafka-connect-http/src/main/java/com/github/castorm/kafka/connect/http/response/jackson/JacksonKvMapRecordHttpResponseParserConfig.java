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

import com.github.castorm.kafka.connect.http.response.timestamp.EpochMillisOrDelegateTimestampParser;
import com.github.castorm.kafka.connect.http.response.timestamp.spi.TimestampParser;
import lombok.Getter;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Type.CLASS;

@Getter
public class JacksonKvMapRecordHttpResponseParserConfig extends AbstractConfig {

    private static final String RECORD_TIMESTAMP_PARSER_CLASS = "http.response.record.timestamp.parser";

    private final JacksonResponseMapRecordParser responseParser;
    private final TimestampParser timestampParser;

    JacksonKvMapRecordHttpResponseParserConfig(Map<String, ?> originals){
        super(config(),originals);
        JacksonMapSerializer serializer = new JacksonMapSerializer();
        JacksonRecordParser recordParser = new JacksonRecordParser(serializer);
        recordParser.configure(originals);
        responseParser = new JacksonResponseMapRecordParser(recordParser,serializer);
        recordParser.configure(originals);
        timestampParser = getConfiguredInstance(RECORD_TIMESTAMP_PARSER_CLASS,TimestampParser.class);

    }


    public static ConfigDef config() {
        return new ConfigDef()
                .define(RECORD_TIMESTAMP_PARSER_CLASS, CLASS, EpochMillisOrDelegateTimestampParser.class, LOW, "Record Timestamp parser class");
    }

}
