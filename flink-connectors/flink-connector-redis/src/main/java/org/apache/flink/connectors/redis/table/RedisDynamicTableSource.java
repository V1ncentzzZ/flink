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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.redis.table.function.RedisRowDataAsyncLookupFunction;
import org.apache.flink.connectors.redis.table.function.RedisRowDataLookupFunction;
import org.apache.flink.connectors.redis.table.options.RedisConnectionOptions;
import org.apache.flink.connectors.redis.table.options.RedisLookupOptions;
import org.apache.flink.connectors.redis.table.options.RedisOptionalOptions;
import org.apache.flink.connectors.redis.table.options.RedisOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** Redis table source implementation. */
@Internal
public class RedisDynamicTableSource implements LookupTableSource, SupportsProjectionPushDown {

    private final TableSchema tableSchema;
    private final ReadableConfig tableOptions;
    private final RedisLookupOptions lookupOptions;
    private final RedisConnectionOptions connectionOptions;
    private final RedisOptionalOptions optionalOptions;
    private final DataType outputDataType;
    private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;

    public RedisDynamicTableSource(
            TableSchema tableSchema,
            ReadableConfig tableOptions,
            DataType outputDataType,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
        this.tableSchema = tableSchema;
        this.tableOptions = tableOptions;
        this.lookupOptions = RedisOptions.getRedisLookupOptions(tableOptions);
        this.connectionOptions = RedisOptions.getRedisConnectionOptions(tableOptions);
        this.optionalOptions = RedisOptions.getRedisOptionalOptions(tableOptions);
        this.outputDataType = outputDataType;
        this.decodingFormat = decodingFormat;
    }

    @Override
    public LookupTableSource.LookupRuntimeProvider getLookupRuntimeProvider(
            LookupTableSource.LookupContext context) {

        String[] keyNames = new String[context.getKeys().length];
        for (int i = 0; i < keyNames.length; i++) {
            int[] innerKeyArr = context.getKeys()[i];
            Preconditions.checkArgument(
                    innerKeyArr.length == 1,
                    "Redis connector only support non-nested look up keys");
            keyNames[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
        }

        DeserializationSchema<RowData> deserializationSchema =
                this.decodingFormat.createRuntimeDecoder(context, this.outputDataType);

        if (lookupOptions.getLookupAsync()) {
            return AsyncTableFunctionProvider.of(
                    new RedisRowDataAsyncLookupFunction(
                            tableSchema,
                            keyNames,
                            lookupOptions,
                            connectionOptions,
                            optionalOptions,
                            deserializationSchema));
        } else {
            return TableFunctionProvider.of(
                    new RedisRowDataLookupFunction(
                            tableSchema,
                            keyNames,
                            lookupOptions,
                            connectionOptions,
                            optionalOptions,
                            deserializationSchema));
        }
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields) {}

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(
                tableSchema, tableOptions, outputDataType, decodingFormat);
    }

    @Override
    public String asSummaryString() {
        return "REDIS";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RedisDynamicTableSource)) {
            return false;
        }
        RedisDynamicTableSource that = (RedisDynamicTableSource) o;
        return Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(tableOptions, that.tableOptions)
                && Objects.equals(lookupOptions, that.lookupOptions)
                && Objects.equals(outputDataType, that.outputDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableSchema, tableOptions, lookupOptions, outputDataType);
    }
}
