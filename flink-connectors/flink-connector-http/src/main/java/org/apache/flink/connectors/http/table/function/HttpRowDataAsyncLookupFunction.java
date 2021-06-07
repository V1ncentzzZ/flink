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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.http.table.converter.HttpRowConverter;
import org.apache.flink.connectors.http.table.options.HttpLookupOptions;
import org.apache.flink.connectors.http.table.options.HttpOptionalOptions;
import org.apache.flink.connectors.http.table.options.HttpRequestOptions;
import org.apache.flink.metrics.Gauge;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.utils.HttpClient;

import org.apache.flink.utils.HttpUtils;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The HttpRowDataAsyncLookupFunction is an implemenation to lookup Http data by key in async
 * fashion. It looks up the result as {@link RowData}.
 */
@Internal
public class HttpRowDataAsyncLookupFunction extends AsyncTableFunction<RowData> {

	private static final Logger LOG = LoggerFactory.getLogger(HttpRowDataAsyncLookupFunction.class);
	private static final long serialVersionUID = 1L;

	private final int fieldCount;
	private final String[] lookupKeys;
	private final String requestUrl;
	private final String requestMethod;
	private final List<String> requestParameters;
	private final Map<String, String> requestHeaders;
	private final boolean enableBatchQuery;
	private final Long requestBatchSize;
	private final Long requestSendInterval;
	private final Integer requestTimeout;
	private final Integer requestMaxRetries;
	private final Integer requestSocketTimeout;
	private final Integer requestConnectTimout;

	private final long cacheMaxSize;
	private final long cacheExpireMs;

	private final boolean ignoreInvokeErrors;

	private final HttpRowConverter httpRowConverter;

	private transient ConcurrentHashMap<Object[], CompletableFuture<Collection<RowData>>> batchCollection;
	private transient Cache<Object, List<RowData>> cache;
	private transient HttpClient httpClient;
	private transient ObjectMapper objectMapper;
	private transient ScheduledExecutorService executorService;

	public HttpRowDataAsyncLookupFunction(
		TableSchema tableSchema,
		String[] lookupKeys,
		HttpRequestOptions requestOptions,
		HttpLookupOptions lookupOptions,
		HttpOptionalOptions optionalOptions) {
		this.fieldCount = tableSchema.getFieldCount();
		this.lookupKeys = lookupKeys;

		this.requestUrl = requestOptions.getRequestUrl();
		this.requestMethod = requestOptions.getRequestMethod();
		this.requestParameters = requestOptions.getRequestParameters();
		this.requestHeaders = requestOptions.getRequestHeaders();
		this.requestBatchSize = requestOptions.getRequestBatchSize();
		this.enableBatchQuery = requestBatchSize != -1L;
		this.requestSendInterval = requestOptions.getRequestSendInterval();
		this.requestTimeout = requestOptions.getRequestTimeout();
		this.requestMaxRetries = requestOptions.getRequestMaxRetries();
		this.requestSocketTimeout = requestOptions.getRequestSocketTimeout();
		this.requestConnectTimout = requestOptions.getRequestConnectTimout();

		this.cacheMaxSize = lookupOptions.getCacheMaxSize();
		this.cacheExpireMs = lookupOptions.getCacheExpireMs();

		this.ignoreInvokeErrors = optionalOptions.isIgnoreInvokeErrors();

		final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

		this.httpRowConverter = new HttpRowConverter(rowType);
	}

	@Override
	public void open(FunctionContext context) {
		LOG.info("start open ...");
		try {
			this.httpClient =
				new HttpClient(
					requestTimeout,
					requestMaxRetries,
					requestSocketTimeout,
					requestConnectTimout);
			if (enableBatchQuery) {
				this.batchCollection = new ConcurrentHashMap<>();
				this.executorService = Executors.newSingleThreadScheduledExecutor();
			}
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
			this.objectMapper = new ObjectMapper();
		} catch (Exception e) {
			LOG.error("Exception while creating connection to Http.", e);
			throw new RuntimeException("Cannot create connection to Http.", e);
		}
		LOG.info("end open.");
	}

	/**
	 * The invoke entry point of lookup function.
	 *
	 * @param future The result or exception is returned.
	 * @param keys the lookup keys
	 */
	public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
		if (enableBatchQuery) {
			if (MapUtils.isEmpty(batchCollection)) {
				this.executorService.schedule(
					this::triggerFetchResult,
					requestSendInterval,
					TimeUnit.MILLISECONDS);
			}
			if (batchCollection.size() >= requestBatchSize) {
				triggerFetchResult();
			}
			System.out.println("add keys: " + Arrays.toString(keys));
			batchCollection.put(keys, future);
		} else {
			fetchResult(future, keys);
		}
	}

	public void fetchResult(CompletableFuture<Collection<RowData>> future, Object... keys) {
		if (cache != null) {
			RowData keyRow = GenericRowData.of(keys);
			List<RowData> cacheRowData = cache.getIfPresent(keyRow);
			if (cacheRowData != null) {
				LOG.debug("found row data from cache: {}", cacheRowData);
				if (CollectionUtils.isEmpty(cacheRowData)) {
					future.complete(Collections.emptyList());
				} else {
					future.complete(cacheRowData);
				}
				return;
			}
		}
		// fetch result
		LOG.info("not found data from cache, do fetch result，keys: {}", Arrays.toString(keys));
		fetch(future, keys);
	}


	/**
	 * Execute async fetch result .
	 *
	 * @param resultFuture The result or exception is returned.
	 * @param params the lookup key.
	 */
	private void fetch(
		CompletableFuture<Collection<RowData>> resultFuture, Object... params) {
		CompletableFuture.runAsync(() -> {
			try {
				List<Object> requestParams = new ArrayList<>();
				Collections.addAll(requestParams, params);
				Tuple2<Integer, String> resp =
					HttpUtils.isPostRequest(requestMethod)
						? doPost(requestParams)
						: doGet(requestParams);
				if (resp._1 == HttpStatus.SC_OK && resp._2 != null) {
					String resp2 = resp._2;
					if (StringUtils.isBlank(resp2)) {
						resultFuture.complete(Collections.emptyList());
						if (cache != null) {
							cache.put(
								GenericRowData.of(params),
								Collections.singletonList(new GenericRowData(fieldCount)));
						}
					} else {
						List<Map> respList = objectMapper.readValue(resp2, List.class);
						ArrayList<RowData> rows = new ArrayList<>();
						for (Map map : respList) {
							RowData rowData = convertToRow(map);
							rows.add(rowData);
						}
						if (cache != null) {
							resultFuture.complete(rows);
							cache.put(GenericRowData.of(params), rows);
						} else {
							resultFuture.complete(rows);
						}
					}
				}
			} catch (Exception e) {
				LOG.error("Failed to fetch result, exception: ", e);
				if (ignoreInvokeErrors) {
					resultFuture.complete(Collections.emptyList());
				} else {
					resultFuture.completeExceptionally(e);
				}
			}
		});
	}

	private void batchFetchResult(Map<Object[], CompletableFuture<Collection<RowData>>> unhandCollection) {
		if (cache != null) {
			Iterator<Map.Entry<Object[], CompletableFuture<Collection<RowData>>>> it = unhandCollection
				.entrySet()
				.iterator();
			while (it.hasNext()) {
				Map.Entry<Object[], CompletableFuture<Collection<RowData>>> entry = it.next();
				RowData keyRow = GenericRowData.of(entry.getKey());
				List<RowData> cacheRowData = cache.getIfPresent(keyRow);
				if (cacheRowData != null) {
					LOG.info("found row data from cache: {}", cacheRowData);
					if (CollectionUtils.isEmpty(cacheRowData)) {
						entry.getValue().complete(Collections.emptyList());
					} else {
						entry.getValue().complete(cacheRowData);
					}
					it.remove();
				}
			}
		}

		List<Object> batchRequestParams = new ArrayList<>();
		for (int i = 0; i < lookupKeys.length; i++) {
			List<Object> params = new ArrayList<>();
			for (Object[] key : unhandCollection.keySet()) {
				params.add(key[i]);
			}
			batchRequestParams.add(params);
		}
		System.out.println("batch request params: " + batchRequestParams);

		batchFetch(batchRequestParams, unhandCollection);
	}

	private void batchFetch(
		List<Object> batchRequestParams,
		Map<Object[], CompletableFuture<Collection<RowData>>> unhandCollection) {
		CompletableFuture.runAsync(() -> {
			try {
				Tuple2<Integer, String> resp =
					HttpUtils.isPostRequest(requestMethod)
						? doPost(batchRequestParams)
						: doGet(batchRequestParams);
				if (resp._1 == HttpStatus.SC_OK && resp._2 != null) {
					String resp2 = resp._2;
					unhandCollection.forEach((k, v) -> {
						try {
							if (StringUtils.isBlank(resp2)) {
								v.complete(Collections.emptyList());
								if (cache != null) {
									cache.put(
										GenericRowData.of(k),
										Collections.singletonList(new GenericRowData(fieldCount)));
								}
							} else {
								List<Map> respList = objectMapper.readValue(resp2, List.class);
								ArrayList<RowData> rows = new ArrayList<>();
								for (Map res : respList) {
									boolean checkKeys = false;
									for (int i = 0; i < k.length; i++) {
										checkKeys = Objects.equals(
											res.get(lookupKeys[i]),
											convert2JavaType(k[i]));
									}
									if (checkKeys) {
										RowData rowData = convertToRow(res);
										rows.add(rowData);
									}
								}
								if (cache != null) {
									cache.put(GenericRowData.of(k), rows);
								}
								v.complete(rows);

							}
						} catch (Exception e) {
							LOG.error("Failed to fetch result, exception: ", e);
							if (ignoreInvokeErrors) {
								v.complete(Collections.emptyList());
							} else {
								v.completeExceptionally(e);
							}
						}
					});
				} else {
					unhandCollection.forEach((k, v) -> v.completeExceptionally(new RuntimeException(resp.toString())));
				}
			} catch (Exception e) {
				LOG.error("Failed to fetch result, exception: ", e);
				if (ignoreInvokeErrors) {
					unhandCollection.forEach((k, v) -> v.complete(Collections.emptyList()));
				} else {
					unhandCollection.forEach((k, v) -> v.completeExceptionally(e));
				}
			} finally {
				System.out.println("clear batch collection");
				unhandCollection.keySet().forEach(k -> batchCollection.remove(k));
			}
		});
	}

	private void triggerFetchResult() {
		if (MapUtils.isNotEmpty(batchCollection)) {
			Map<Object[], CompletableFuture<Collection<RowData>>> unhandCollection = new HashMap<>();
			Iterator<Map.Entry<Object[], CompletableFuture<Collection<RowData>>>> it = batchCollection
				.entrySet()
				.iterator();
			while (it.hasNext()) {
				Map.Entry<Object[], CompletableFuture<Collection<RowData>>> next = it.next();
				unhandCollection.put(next.getKey(), next.getValue());
				it.remove();
			}
			batchFetchResult(unhandCollection);
		}
	}

	@Override
	public void close() {
		LOG.info("start close ...");
		if (MapUtils.isNotEmpty(batchCollection)) {
			triggerFetchResult();
		}
		if (executorService != null) {
			executorService.shutdown();
		}
		LOG.info("end close.");
	}


	private Tuple2<Integer, String> doPost(List<Object> params) throws IOException {
		HttpPost post = HttpUtils.wrapPostRequest(
			requestUrl,
			requestHeaders,
			CollectionUtils.isNotEmpty(requestParameters) ? requestParameters : Arrays.asList(lookupKeys),
			params.stream().map(this::convert2JavaType).collect(Collectors.toList()));
		return httpClient.request(post);
	}


	private Tuple2<Integer, String> doGet(List<Object> params) throws Exception {
		HttpGet get = HttpUtils.wrapGetRequest(
			requestUrl,
			requestHeaders,
			Arrays.asList(lookupKeys),
			params);
		return httpClient.request(get);
	}

	private RowData convertToRow(Map map) throws SQLException {
		return httpRowConverter.toInternal(map);
	}

	public Object convert2JavaType(Object o) {
		if (o instanceof BinaryStringData) {
			return String.valueOf(o);
		} else if (o instanceof List) {
			return ((List) o).stream().map(this::convert2JavaType).collect(Collectors.toList());
		} else {
			return o;
		}
	}
}
