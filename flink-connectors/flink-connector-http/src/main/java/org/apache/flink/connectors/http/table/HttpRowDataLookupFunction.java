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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The HttpRowDataLookupFunction is a standard user-defined table function, it can be used in tableAPI
 * and also useful for temporal table join plan in SQL. It looks up the result as {@link RowData}.
 */
@Internal
public class HttpRowDataLookupFunction extends TableFunction<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(HttpRowDataLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private String tableName;
	private TableSchema tableSchema;

	public HttpRowDataLookupFunction(
			String tableName,
			TableSchema tableSchema) {
		this.tableName = tableName;
		this.tableSchema = tableSchema;
	}

	/**
	 * The invoke entry point of lookup function.
	 * @param key the lookup key. Currently only support single key.
	 */
	public void eval(Object key) throws IOException {
		// fetch result

	}

	@Override
	public void open(FunctionContext context) {
		LOG.info("start open ...");
		LOG.info("end open.");
	}

	@Override
	public void close() {
		LOG.info("start close ...");
		LOG.info("end close.");
	}

	@VisibleForTesting
	public String getTableName() {
		return tableName;
	}
}
