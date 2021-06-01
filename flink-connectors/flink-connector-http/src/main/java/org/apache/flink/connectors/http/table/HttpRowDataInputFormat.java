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
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import org.apache.flink.utils.HttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 * InputFormat for {@link HttpDynamicTableSource}.
 */
@Internal
public class HttpRowDataInputFormat extends RichInputFormat<RowData, InputSplit> implements ResultTypeQueryable<RowData> {

	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(HttpRowDataInputFormat.class);

	private String tableName;
	private TableSchema tableSchema;

	private transient Connection dbConn;
	private transient PreparedStatement statement;
	private transient ResultSet resultSet;
	private transient boolean hasNext;

	private transient HttpClient httpClient;

	public HttpRowDataInputFormat(String tableName, TableSchema tableSchema) {
		this.tableName = tableName;
		this.tableSchema = tableSchema;
	}

	@Override
	public void configure(Configuration parameters) {
		//do nothing here
	}

	@Override
	public void openInputFormat() {
		//called once per inputFormat (on open)
		try {
			// TODO construct http client and init http post params
			httpClient = new HttpClient();

		} catch (Exception se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		}
	}

	@Override
	public void closeInputFormat() {
		//called once per inputFormat (on close)

	}

	/**
	 * Connects to the source database and executes the query in a <b>parallel
	 * fashion</b> if
	 * this {@link InputFormat} is built using a parameterized query (i.e. using
	 * a {@link PreparedStatement})
	 * and a proper in a <b>non-parallel
	 * fashion</b> otherwise.
	 *
	 * @param inputSplit which is ignored if this InputFormat is executed as a
	 *                   non-parallel source,
	 *                   a "hook" to the query parameters otherwise (using its
	 *                   <i>splitNumber</i>)
	 * @throws IOException if there's an error during the execution of the query
	 */
	@Override
	public void open(InputSplit inputSplit) throws IOException {
		try {

		} catch (Exception se) {
			throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
		}
	}

	/**
	 * Closes all resources used.
	 *
	 * @throws IOException Indicates that a resource could not be closed.
	 */
	@Override
	public void close() throws IOException {

	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return null;
	}

	/**
	 * Checks whether all data has been read.
	 *
	 * @return boolean value indication whether all data has been read.
	 * @throws IOException
	 */
	@Override
	public boolean reachedEnd() throws IOException {
		return !hasNext;
	}

	/**
	 * Stores the next resultSet row in a tuple.
	 *
	 * @param reuse row to be reused.
	 * @return row containing next {@link RowData}
	 * @throws IOException
	 */
	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		return null;
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		return cachedStatistics;
	}

	@Override
	public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return null;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
		return new DefaultInputSplitAssigner(inputSplits);
	}
}
