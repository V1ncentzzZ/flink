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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connectors.http.table.function.HttpRowDataAsyncLookupFunction;
import org.apache.flink.connectors.http.table.function.HttpRowDataLookupFunction;
import org.apache.flink.connectors.http.table.options.HttpLookupOptions;
import org.apache.flink.connectors.http.table.options.HttpOptions;
import org.apache.flink.connectors.http.table.options.HttpRequestOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/** Http table source implementation. */
@Internal
public class HttpDynamicTableSource implements LookupTableSource, SupportsProjectionPushDown {

	private final TableSchema tableSchema;
	private final ReadableConfig tableOptions;
	private final HttpRequestOptions requestOptions;
	private final HttpLookupOptions lookupOptions;

    public HttpDynamicTableSource(
            TableSchema tableSchema,
			ReadableConfig tableOptions) {
        this.tableSchema = tableSchema;
        this.tableOptions = tableOptions;
        this.requestOptions = HttpOptions.getHttpRequestOptions(tableOptions);
        this.lookupOptions = HttpOptions.getHttpLookupOptions(tableOptions);
    }

    // left join时调用该方法根据join的key去lookup对应的数据
    @Override
    public LookupTableSource.LookupRuntimeProvider getLookupRuntimeProvider(LookupTableSource.LookupContext context) {

		String[] keyNames = new String[context.getKeys().length];
		for (int i = 0; i < keyNames.length; i++) {
			int[] innerKeyArr = context.getKeys()[i];
			Preconditions.checkArgument(
				innerKeyArr.length == 1, "HTTP only support non-nested look up keys");
			keyNames[i] = tableSchema.getFieldNames()[innerKeyArr[0]];
		}

		// TODO add some checks
        if (lookupOptions.getLookupAsync()) {
            return AsyncTableFunctionProvider.of(
                    new HttpRowDataAsyncLookupFunction(tableSchema, keyNames, requestOptions, lookupOptions));
        } else {
			return TableFunctionProvider.of(
				new HttpRowDataLookupFunction(tableSchema, keyNames, requestOptions, lookupOptions));
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
		return new HttpDynamicTableSource(tableSchema, tableOptions);
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
		return Objects.equals(tableSchema, that.tableSchema) &&
			Objects.equals(tableOptions, that.tableOptions) &&
			Objects.equals(requestOptions, that.requestOptions) &&
			Objects.equals(lookupOptions, that.lookupOptions);
	}

	@Override
	public int hashCode() {
		return Objects.hash(tableSchema, tableOptions, requestOptions, lookupOptions);
	}
}
