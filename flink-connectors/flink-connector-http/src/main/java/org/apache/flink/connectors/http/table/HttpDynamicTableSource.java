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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.http.table.function.HttpRowDataAsyncLookupFunction;
import org.apache.flink.connectors.http.table.function.HttpRowDataLookupFunction;
import org.apache.flink.connectors.http.table.options.HttpLookupOptions;
import org.apache.flink.connectors.http.table.options.HttpOptionalOptions;
import org.apache.flink.connectors.http.table.options.HttpOptions;
import org.apache.flink.connectors.http.table.options.HttpRequestOptions;
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

import java.util.List;
import java.util.Objects;

/** Http table source implementation. */
@Internal
public class HttpDynamicTableSource implements LookupTableSource, SupportsProjectionPushDown {

	private final TableSchema tableSchema;
	private final ReadableConfig tableOptions;
	private final HttpRequestOptions requestOptions;
	private final HttpLookupOptions lookupOptions;
	private final HttpOptionalOptions optionalOptions;
	private final DataType outputDataType;
	private final DecodingFormat<DeserializationSchema<List<RowData>>> decodingFormat;

    public HttpDynamicTableSource(
		TableSchema tableSchema,
		ReadableConfig tableOptions,
		DataType outputDataType,
		DecodingFormat<DeserializationSchema<List<RowData>>> decodingFormat) {
        this.tableSchema = tableSchema;
        this.tableOptions = tableOptions;
        this.requestOptions = HttpOptions.getHttpRequestOptions(tableOptions);
        this.lookupOptions = HttpOptions.getHttpLookupOptions(tableOptions);
        this.optionalOptions = HttpOptions.getHttpOptionalOptions(tableOptions);
        this.outputDataType = outputDataType;
        this.decodingFormat = decodingFormat;
    }

    @Override
    public LookupTableSource.LookupRuntimeProvider getLookupRuntimeProvider(LookupTableSource.LookupContext context) {

		String[] keyNames = new String[context.getKeys().length];
		for (int i = 0; i < keyNames.length; i++) {
			int[] innerKeyArr = context.getKeys()[i];
			Preconditions.checkArgument(
				innerKeyArr.length == 1, "HTTP connector only support non-nested look up keys");
			keyNames[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
		}

		DeserializationSchema<List<RowData>> deserializationSchema =
			this.decodingFormat.createRuntimeDecoder(context, this.outputDataType);

        if (lookupOptions.getLookupAsync()) {
            return AsyncTableFunctionProvider.of(new HttpRowDataAsyncLookupFunction(
            	tableSchema, keyNames, requestOptions, lookupOptions, optionalOptions, deserializationSchema));
        } else {
			return TableFunctionProvider.of(new HttpRowDataLookupFunction(
				tableSchema, keyNames, requestOptions, lookupOptions, optionalOptions, deserializationSchema));
        }
    }

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
	}

	@Override
	public DynamicTableSource copy() {
		return new HttpDynamicTableSource(
			tableSchema,
			tableOptions,
			outputDataType,
			decodingFormat);
	}

	@Override
	public String asSummaryString() {
		return "HTTP";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof HttpDynamicTableSource)) {
			return false;
		}
		HttpDynamicTableSource that = (HttpDynamicTableSource) o;
		return Objects.equals(tableSchema, that.tableSchema)
			&& Objects.equals(tableOptions, that.tableOptions)
			&& Objects.equals(requestOptions, that.requestOptions)
			&& Objects.equals(lookupOptions, that.lookupOptions)
			&& Objects.equals(optionalOptions, that.optionalOptions)
			&& Objects.equals(outputDataType, that.outputDataType)
			&& Objects.equals(decodingFormat, that.decodingFormat);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			tableSchema,
			tableOptions,
			requestOptions,
			lookupOptions,
			optionalOptions,
			outputDataType,
			decodingFormat);
	}
}
