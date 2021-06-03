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
import org.apache.flink.connectors.http.table.options.HttpRequestOptions;
import org.apache.flink.metrics.Gauge;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

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
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * The HttpRowDataAsyncLookupFunction is an implemenation to lookup Http data by key in async
 * fashion. It looks up the result as {@link RowData}.
 */
@Internal
public class HttpRowDataAsyncLookupFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRowDataAsyncLookupFunction.class);
    private static final long serialVersionUID = 1L;

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final HttpClient httpClient = HttpClient.getInstance(60000);

	private final String[] lookupKeys;
    private final String requestUrl;
    private final String requestMethod;
    private final Map<String, String> requestHeaders;
    private final Long requestBatchSize;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

	private final HttpRowConverter httpRowConverter;

	private transient Set<Object> batchSet;
	private transient Cache<Object, List<RowData>> cache;

    public HttpRowDataAsyncLookupFunction(
		TableSchema tableSchema,
		String[] lookupKeys,
		HttpRequestOptions requestOptions,
		HttpLookupOptions lookupOptions) {
//        this.tableSchema = tableSchema;

		this.lookupKeys = lookupKeys;

        this.requestUrl = requestOptions.getRequestUrl();
        this.requestMethod = requestOptions.getRequestMethod();
        this.requestHeaders = requestOptions.getRequestHeaders();
        this.requestBatchSize = requestOptions.getRequestBatchSize();

        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();

		final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

		this.httpRowConverter = new HttpRowConverter(rowType);
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
		try {
			this.batchSet = requestBatchSize == -1L ? null : new HashSet<>(requestBatchSize.intValue());
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

    /**
     * The invoke entry point of lookup function.
     *
     * @param future The result or exception is returned.
     * @param keys the lookup key. Currently only support single rowkey.
     */
    public void eval(CompletableFuture<Collection<RowData>> future, Object...keys) {
        int currentRetry = 0;
        if (cache != null) {
			RowData keyRow = GenericRowData.of(keys);
            List<RowData> cacheRowData = cache.getIfPresent(keyRow);
            if (cacheRowData != null) {
				LOG.info("found row data from cache: {}", cacheRowData);
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
        fetchResult(future, currentRetry, keys);
    }

    /**
     * Execute async fetch result .
     *
     * @param resultFuture The result or exception is returned.
     * @param currentRetry Current number of retries.
     * @param params the lookup key.
     */
    private void fetchResult(
            CompletableFuture<Collection<RowData>> resultFuture, int currentRetry, Object...params) {
    	CompletableFuture.runAsync(() -> {
    		try {
				Tuple2<Integer, String> resp = isPostRequest() ? doPost(params) : doGet(params);
				if (resp._1 == HttpStatus.SC_OK && resp._2 != null) {
					String resp2 = resp._2;
					if (StringUtils.isBlank(resp2)) {
						resultFuture.complete(Collections.emptyList());
						if (cache != null) {
							cache.put(GenericRowData.of(params),
								Collections.singletonList(new GenericRowData(0)));
						}
					} else {
						List<Map> respList = OBJECT_MAPPER.readValue(resp2, List.class);
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
				} else {
					fetchResult(resultFuture, currentRetry + 1, params);
				}
			} catch (Exception e) {
				resultFuture.completeExceptionally(e);
			}
		});
    }

    private RowData convertToRow(Map map) throws SQLException {
		return httpRowConverter.toInternal(map);
	}

    @Override
    public void close() {
        LOG.info("start close ...");
        LOG.info("end close.");
    }

    private boolean isPostRequest() {
    	return StringUtils.equalsIgnoreCase("POST", requestMethod);
	}

	private Tuple2<Integer, String> doPost(Object...params) throws IOException {
		HttpPost post = new HttpPost(requestUrl);
		if (MapUtils.isNotEmpty(requestHeaders)) {
			for (String key : requestHeaders.keySet()) {
				post.addHeader(key, requestHeaders.get(key));
			}
		}
		Map<String, Object> request = new HashMap<>();
		for (int i = 0; i < params.length; i++) {
			request.put(lookupKeys[i], String.valueOf(params[i]));
		}
		StringEntity entity = new StringEntity(
			OBJECT_MAPPER.writeValueAsString(request),
			StandardCharsets.UTF_8);
		post.setEntity(entity);
		return httpClient.request(post);
	}

	private Tuple2<Integer, String> doGet(Object...params) throws IOException, URISyntaxException {
		URIBuilder uriBuilder = new URIBuilder(requestUrl);
		for (int i = 0; i < params.length; i++) {
			uriBuilder.addParameter(lookupKeys[i], String.valueOf(params[i]));
		}
		HttpGet get = new HttpGet(uriBuilder.build());
		if (MapUtils.isNotEmpty(requestHeaders)) {
			for (String key : requestHeaders.keySet()) {
				get.addHeader(key, requestHeaders.get(key));
			}
		}
		return httpClient.request(get);
	}
}
