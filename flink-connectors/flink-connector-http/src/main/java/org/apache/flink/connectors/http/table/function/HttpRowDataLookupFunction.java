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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connectors.http.table.options.HttpLookupOptions;
import org.apache.flink.connectors.http.table.options.HttpOptionalOptions;
import org.apache.flink.connectors.http.table.options.HttpRequestOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.utils.HttpClient;
import org.apache.flink.utils.HttpUtils;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * The HttpRowDataLookupFunction is a standard user-defined table function, it can be used in
 * tableAPI and also useful for temporal table join plan in SQL. It looks up the result as {@link
 * RowData}.
 */
@Internal
public class HttpRowDataLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(HttpRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final int fieldCount;
    private final String[] lookupKeys;

    private final String requestUrl;
    private final String requestMethod;
    private final List<String> requestParameters;
    private final Map<String, String> requestHeaders;
    private final Integer requestTimeout;
    private final Integer requestMaxRetries;
    private final Integer requestSocketTimeout;
    private final Integer requestConnectTimout;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    private final boolean ignoreInvokeErrors;

    private final DeserializationSchema<List<RowData>> deserializationSchema;

    private transient Cache<Object, List<RowData>> cache;
    private transient HttpClient httpClient;

    public HttpRowDataLookupFunction(
            TableSchema tableSchema,
            String[] lookupKeys,
            HttpRequestOptions requestOptions,
            HttpLookupOptions lookupOptions,
            HttpOptionalOptions optionalOptions,
            DeserializationSchema<List<RowData>> deserializationSchema) {

        this.fieldCount = tableSchema.getFieldCount();
        this.lookupKeys = lookupKeys;

        this.requestUrl = requestOptions.getRequestUrl();
        this.requestMethod = requestOptions.getRequestMethod();
        this.requestParameters = requestOptions.getRequestParameters();
        this.requestHeaders = requestOptions.getRequestHeaders();
        this.requestTimeout = requestOptions.getRequestTimeout();
        this.requestMaxRetries = requestOptions.getRequestMaxRetries();
        this.requestSocketTimeout = requestOptions.getRequestSocketTimeout();
        this.requestConnectTimout = requestOptions.getRequestConnectTimout();

        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();

        this.ignoreInvokeErrors = optionalOptions.isIgnoreInvokeErrors();

        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        this.httpClient =
                new HttpClient(
                        requestTimeout,
                        requestMaxRetries,
                        requestSocketTimeout,
                        requestConnectTimout);
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
                LOG.info("register metric: {}", "lookupCacheHitRate");
                DecimalFormat df = new DecimalFormat("0.00");
                context.getMetricGroup()
                        .gauge(
                                "lookupCacheHitRate",
                                (Gauge<Double>)
                                        () ->
                                                Double.parseDouble(
                                                                df.format(cache.stats().hitRate()))
                                                        * 100);
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
     * @param keys the lookup key.
     */
    public void eval(Object... keys) {
        fetchResult(keys);
    }

    private void fetchResult(Object... params) {
        if (cache != null) {
            List<RowData> cacheRowData = cache.getIfPresent(GenericRowData.of(params));
            if (cacheRowData != null) {
                LOG.info("found row data from cache: {}", cacheRowData);
                for (RowData rowData : cacheRowData) {
                    collect(rowData);
                }
                return;
            }
        }
        // fetch result
        LOG.info("not found data from cache, do fetch result，keys: {}", Arrays.toString(params));
        fetch(params);
    }

    private void fetch(Object... params) {
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
                    collect(new GenericRowData(fieldCount));
                    if (cache != null) {
                        cache.put(
                                GenericRowData.of(params),
                                Collections.singletonList(new GenericRowData(fieldCount)));
                    }
                } else {
                    List<RowData> rows =
                            deserializationSchema.deserialize(
                                    resp2.getBytes(StandardCharsets.UTF_8));
                    if (cache != null) {
                        cache.put(GenericRowData.of(params), rows);
                    }
                    for (RowData rowData : rows) {
                        collect(rowData);
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to fetch result, exception: ", e);
            if (!ignoreInvokeErrors) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        LOG.info("end close.");
    }

    private Tuple2<Integer, String> doPost(List<Object> params) throws IOException {
        HttpPost post =
                HttpUtils.wrapPostRequest(
                        requestUrl,
                        requestHeaders,
                        CollectionUtils.isNotEmpty(requestParameters)
                                ? requestParameters
                                : Arrays.asList(lookupKeys),
                        params.stream().map(this::convert2JavaType).collect(Collectors.toList()));
        return httpClient.request(post);
    }

    private Tuple2<Integer, String> doGet(List<Object> params) throws Exception {
        HttpGet get =
                HttpUtils.wrapGetRequest(
                        requestUrl, requestHeaders, Arrays.asList(lookupKeys), params);
        return httpClient.request(get);
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
