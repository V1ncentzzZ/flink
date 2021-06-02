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

package org.apache.flink.connectors.http.table.function;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.flink.connectors.http.table.converter.HttpRowConverter;
import org.apache.flink.connectors.http.table.options.HttpLookupOptions;
import org.apache.flink.connectors.http.table.options.HttpRequestOptions;
import org.apache.flink.metrics.Gauge;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;

import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.utils.HttpClient;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The HttpRowDataLookupFunction is a standard user-defined table function, it can be used in tableAPI
 * and also useful for temporal table join plan in SQL. It looks up the result as {@link RowData}.
 */
@Internal
public class HttpRowDataLookupFunction extends TableFunction<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(HttpRowDataLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final HttpClient httpClient = HttpClient.getInstance(60000);

	private final String requestUrl;
	private final String requestMethod;
	private final Map<String, String> requestHeaders;
	private final Long requestBatchSize;

	private final long cacheMaxSize;
	private final long cacheExpireMs;

	private final HttpRowConverter httpRowConverter;

	private transient Cache<Object, RowData> cache;

	public HttpRowDataLookupFunction(
		TableSchema tableSchema,
		String[] keyNames, HttpRequestOptions requestOptions,
		HttpLookupOptions lookupOptions) {
//		this.tableSchema = tableSchema;

		this.requestUrl = requestOptions.getRequestUrl();
		this.requestMethod = requestOptions.getRequestMethod();
		this.requestHeaders = requestOptions.getRequestHeaders();
		this.requestBatchSize = requestOptions.getRequestBatchSize();

		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.cacheExpireMs = lookupOptions.getCacheExpireMs();

		final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

		this.httpRowConverter = new HttpRowConverter(rowType);
	}

	/**
	 * The invoke entry point of lookup function.
	 * @param orderId the lookup key. Currently only support single key.
	 */
	public void eval(Object orderId) throws IOException {
		int currentRetry = 0;
		if (cache != null) {
			RowData cacheRowData = cache.getIfPresent(orderId);
			if (cacheRowData != null) {
				collect(cacheRowData);
				return;
			}
		}
		// fetch result
		fetchResult(currentRetry, orderId);
	}

	private void fetchResult(int currentRetry, Object orderId) {
		try {
			Tuple2<Integer, String> resp;
			if (isPostRequest()) {
				HttpPost post = new HttpPost(requestUrl);
				if (MapUtils.isNotEmpty(requestHeaders)) {
					for (String key : requestHeaders.keySet()) {
						post.addHeader(key, requestHeaders.get(key));
					}
				}
				StringEntity entity = new StringEntity(
					OBJECT_MAPPER.writeValueAsString(Collections.singletonMap("orderId", String.valueOf(orderId))),
					StandardCharsets.UTF_8);
				post.setEntity(entity);

				resp = httpClient.request(post);
			} else {
				URIBuilder uriBuilder = new URIBuilder(requestUrl)
					.addParameter("orderId", String.valueOf(orderId));
				HttpGet get = new HttpGet(uriBuilder.build());

				resp = httpClient.request(get);
			}
//				Tuple2<Integer, String> resp = new Tuple2(HttpStatus.SC_OK, "{\"orderId\":"+orderId+"}");
			if (resp._1 == HttpStatus.SC_OK && resp._2 != null) {
				String resp2 = resp._2;
				if (StringUtils.isBlank(resp2)) {
					if (cache != null) {
						collect(new GenericRowData(0));
						cache.put(orderId, new GenericRowData(0));
					}
				} else {
					Map map = OBJECT_MAPPER.readValue(resp2, Map.class);
					RowData rowData = httpRowConverter.toInternal(map);
					collect(rowData);
					if (cache != null) {
						cache.put(orderId, rowData);
					}
				}
			} else {
				fetchResult(currentRetry + 1, orderId);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void open(FunctionContext context) {
		LOG.info("start open ...");
		try {
			this.cache =
				cacheMaxSize <= 0 || cacheExpireMs <= 0
					? null
					: CacheBuilder.newBuilder()
					.recordStats()
					.expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
					.maximumSize(cacheMaxSize)
					.build();
			if (cache != null && context != null) {
				context.getMetricGroup()
					.gauge("lookupCacheHitRate", (Gauge<Double>) () -> cache.stats().hitRate());
			}
		} catch (Exception e) {
			LOG.error("Exception while creating connection to Http.", e);
			throw new RuntimeException("Cannot create connection to Http.", e);
		}
		LOG.info("end open.");
	}

	@Override
	public void close() {
		LOG.info("start close ...");
		LOG.info("end close.");
	}


	private boolean isPostRequest() {
		return StringUtils.equalsIgnoreCase("POST", requestMethod);
	}
}
