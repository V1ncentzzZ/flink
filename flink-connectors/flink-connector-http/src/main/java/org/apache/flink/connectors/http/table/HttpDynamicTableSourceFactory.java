/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.http.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.http.table.deserialization.HttpDeserializationFormatFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connectors.http.table.options.HttpOptions.FORMAT_PROPERTIES_PREFIX;
import static org.apache.flink.connectors.http.table.options.HttpOptions.IGNORE_INVOKE_ERRORS;
import static org.apache.flink.connectors.http.table.options.HttpOptions.LOOKUP_ASYNC;
import static org.apache.flink.connectors.http.table.options.HttpOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connectors.http.table.options.HttpOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_BATCH_SIZE;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_CONNECT_TIMEOUT;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_HEADERS;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_MAX_RETRIES;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_METHOD;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_PARAMETERS;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_SEND_INTERVAL;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_SOCKET_TIMEOUT;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_TIMEOUT;
import static org.apache.flink.connectors.http.table.options.HttpOptions.REQUEST_URL;
import static org.apache.flink.connectors.http.table.options.HttpOptions.RESPONSE_DATA_FIELDS;
import static org.apache.flink.formats.json.JsonOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.json.JsonOptions.TIMESTAMP_FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/** Http connector factory. */
public class HttpDynamicTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "http";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);

		helper.validateExcept(FORMAT_PROPERTIES_PREFIX);
        helper.validate();

        final ReadableConfig tableOptions = helper.getOptions();

		DecodingFormat<DeserializationSchema<List<RowData>>> decodingFormat = helper.discoverDecodingFormat(
			HttpDeserializationFormatFactory.class,
			FORMAT);

		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());

		DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        return new HttpDynamicTableSource(physicalSchema, tableOptions, producedDataType, decodingFormat);
    }


    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(REQUEST_URL);
        set.add(FORMAT);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(REQUEST_METHOD);
        set.add(REQUEST_HEADERS);
        set.add(REQUEST_PARAMETERS);
        set.add(REQUEST_BATCH_SIZE);
        set.add(REQUEST_SEND_INTERVAL);
        set.add(REQUEST_TIMEOUT);
        set.add(REQUEST_MAX_RETRIES);
        set.add(REQUEST_SOCKET_TIMEOUT);
        set.add(REQUEST_CONNECT_TIMEOUT);
        set.add(LOOKUP_ASYNC);
        set.add(LOOKUP_CACHE_MAX_ROWS);
        set.add(LOOKUP_CACHE_TTL);
        set.add(IGNORE_INVOKE_ERRORS);
        set.add(FAIL_ON_MISSING_FIELD);
        set.add(IGNORE_PARSE_ERRORS);
        set.add(TIMESTAMP_FORMAT);
		set.add(RESPONSE_DATA_FIELDS);
        return set;
    }
}
