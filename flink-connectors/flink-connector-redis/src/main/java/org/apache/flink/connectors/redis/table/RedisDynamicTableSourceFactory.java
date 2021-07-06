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

package org.apache.flink.connectors.redis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.connectors.redis.table.options.RedisOptions.HASH_NAME;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.IGNORE_INVOKE_ERRORS;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.LOOKUP_ASYNC;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.LOOKUP_BATCH_SIZE;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.LOOKUP_CACHE_MAX_ROWS;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.LOOKUP_CACHE_TTL;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.LOOKUP_SEND_INTERVAL;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.MASTER_NAME;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.REDIS_DATABASE;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.REDIS_MODE;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.REDIS_NODES;
import static org.apache.flink.connectors.redis.table.options.RedisOptions.REDIS_PASSWORD;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/** Redis connector factory. */
public class RedisDynamicTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "shopee-redis";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        TableFactoryHelper helper = createTableFactoryHelper(this, context);

        final ReadableConfig tableOptions = helper.getOptions();

        helper.validateExcept(tableOptions.get(FORMAT));
        helper.validate();


        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);

        TableSchema schema = context.getCatalogTable().getSchema();
        TableSchema physicalSchema =
                TableSchemaUtils.getPhysicalSchema(schema);

        DataType producedDataType = toPhysicalRowDataType(schema);

        return new RedisDynamicTableSource(
                physicalSchema, tableOptions, producedDataType, decodingFormat);
    }

    public DataType toPhysicalRowDataType(TableSchema schema) {
        List<TableColumn> columns = schema.getTableColumns();
        String pkName = schema.getPrimaryKey().get().getColumns().get(0);
        final DataTypes.Field[] fields =
                columns.stream()
                        .filter(e -> !e.getName().equals(pkName))
                        .filter(TableColumn::isPhysical)
                        .map(column -> FIELD(column.getName(), column.getType()))
                        .toArray(DataTypes.Field[]::new);
        // The row should be never null.
        return ROW(fields).notNull();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(FORMAT);
        set.add(REDIS_NODES);
        set.add(REDIS_MODE);
        return set;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> set = new HashSet<>();
        set.add(REDIS_PASSWORD);
        set.add(REDIS_DATABASE);
        set.add(HASH_NAME);
        set.add(MASTER_NAME);
        set.add(LOOKUP_ASYNC);
        set.add(LOOKUP_CACHE_MAX_ROWS);
        set.add(LOOKUP_CACHE_TTL);
        set.add(LOOKUP_BATCH_SIZE);
        set.add(LOOKUP_SEND_INTERVAL);
        set.add(IGNORE_INVOKE_ERRORS);
        return set;
    }
}
