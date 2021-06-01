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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.connectors.http.table.HttpOptions.LOOKUP_ASYNC;
import static org.apache.flink.connectors.http.table.HttpOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connectors.http.table.HttpOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connectors.http.table.HttpOptions.LOOKUP_MAX_RETRIES;
import static org.apache.flink.connectors.http.table.HttpOptions.REQUEST_HEADERS;
import static org.apache.flink.connectors.http.table.HttpOptions.REQUEST_METHOD;
import static org.apache.flink.connectors.http.table.HttpOptions.REQUEST_URL;
import static org.apache.flink.connectors.http.table.HttpOptions.TABLE_NAME;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/** Http connector factory. */
public class HttpDynamicTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "http";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
		System.out.println("stack: " + Arrays.toString(Thread.currentThread().getStackTrace()));
        TableFactoryHelper helper = createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig tableOptions = helper.getOptions();

		TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        return new HttpDynamicTableSource(physicalSchema, tableOptions);
    }


    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(TABLE_NAME);
        set.add(REQUEST_URL);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(REQUEST_METHOD);
        set.add(REQUEST_HEADERS);
        set.add(LOOKUP_ASYNC);
        set.add(LOOKUP_CACHE_MAX_ROWS);
        set.add(LOOKUP_CACHE_TTL);
        set.add(LOOKUP_MAX_RETRIES);
        return set;
    }
}
