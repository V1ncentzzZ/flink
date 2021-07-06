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

package org.apache.flink.connectors.redis.table.function;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connectors.redis.table.options.RedisConnectionOptions;
import org.apache.flink.connectors.redis.table.options.RedisLookupOptions;
import org.apache.flink.connectors.redis.table.options.RedisOptionalOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.utils.RedisMode;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * The HttpRowDataAsyncLookupFunction is an implemenation to lookup Http data by key in async
 * fashion. It looks up the result as {@link RowData}.
 */
@Internal
public class RedisRowDataAsyncLookupFunction extends AsyncTableFunction<RowData> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RedisRowDataAsyncLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final int fieldCount;
    private final String[] lookupKeys;
    private final Integer[] lookupKeyIndexes;
    private final LogicalType[] lookupKeyTypes;

    private final String nodes;
    private final int db;
    private final String mode;
    private final String password;
    private final String hashName;
    private final String masterName;

    private final boolean enableBatchQuery;
    private final Long lookupBatchSize;
    private final Long lookupSendInterval;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    private final boolean ignoreInvokeErrors;

    private final DeserializationSchema<RowData> deserializationSchema;

    private transient Cache<Object, List<RowData>> cache;
    private transient RedisClient redisClient;
    private transient RedisAsyncCommands<String, String> async;
    private transient ConcurrentHashMap<Object[], CompletableFuture<Collection<RowData>>>
            batchCollection;
    private transient ScheduledExecutorService executorService;

    public RedisRowDataAsyncLookupFunction(
            TableSchema tableSchema,
            String[] lookupKeys,
            RedisLookupOptions lookupOptions,
            RedisConnectionOptions connectionOptions,
            RedisOptionalOptions optionalOptions,
            DeserializationSchema<RowData> deserializationSchema) {

        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
        List<String> fieldNames = Arrays.asList(tableSchema.getFieldNames());
        DataType[] fieldTypes = tableSchema.getFieldDataTypes();

        this.fieldCount = tableSchema.getFieldCount();
        this.lookupKeys = lookupKeys;
        this.lookupKeyIndexes =
                Arrays.stream(lookupKeys).map(rowType::getFieldIndex).toArray(Integer[]::new);
        this.lookupKeyTypes =
                Arrays.stream(lookupKeys)
                        .map(
                                s -> {
                                    checkArgument(
                                            fieldNames.contains(s),
                                            "keyName %s can't find in fieldNames %s.",
                                            s,
                                            fieldNames);
                                    return fieldTypes[fieldNames.indexOf(s)].getLogicalType();
                                })
                        .toArray(LogicalType[]::new);

        this.nodes = connectionOptions.getNodes();
        this.db = connectionOptions.getDatabase();
        this.mode = connectionOptions.getMode();
        this.password = connectionOptions.getPassword();
        this.hashName = connectionOptions.getHashName();
        this.masterName = connectionOptions.getMasterName();
        LOG.info("connection options: {}", connectionOptions);

        this.lookupBatchSize = lookupOptions.getLookupBatchSize();
        this.enableBatchQuery = lookupBatchSize != -1L;
        this.lookupSendInterval = lookupOptions.getLookupSendInterval();

        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();

        this.ignoreInvokeErrors = optionalOptions.isIgnoreInvokeErrors();

        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        try {
            if (enableBatchQuery) {
                this.batchCollection = new ConcurrentHashMap<>();
                this.executorService = Executors.newSingleThreadScheduledExecutor();
            }
            buildRedisClient();
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
            LOG.error("Exception while creating connection to Redis.", e);
            throw new RuntimeException("Cannot create connection to Redis.", e);
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
                        this::triggerFetchResult, lookupSendInterval, TimeUnit.MILLISECONDS);
            }
            LOG.info("lookupSendInterval: {}", lookupSendInterval);
            if (batchCollection.size() >= lookupBatchSize) {
                LOG.info(
                        "batchCollection size: {}, lookUpBatchSize: {}",
                        batchCollection.size(),
                        lookupBatchSize);
                triggerFetchResult();
            }
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
        LOG.debug("not found data from cache, do fetch result，key: {}", keys);
        fetch(future, keys);
    }

    /**
     * Execute async fetch result .
     *
     * @param resultFuture The result or exception is returned.
     * @param keys the lookup key. Currently only support single lookup key.
     */
    private void fetch(CompletableFuture<Collection<RowData>> resultFuture, Object... keys) {
        RedisFuture<String> responseFeature =
                StringUtils.isBlank(hashName)
                        ? async.get(String.valueOf(keys[0]))
                        : async.hget(hashName, String.valueOf(keys[0]));
        responseFeature.whenCompleteAsync(
                (result, throwable) -> {
                    if (throwable != null) {
                        LOG.error("Redis asyncLookup error", throwable);
                        resultFuture.completeExceptionally(throwable);
                    } else {
                        try {
                            if (StringUtils.isBlank(result)) {
                                List<RowData> rows =
                                        Collections.singletonList(new GenericRowData(fieldCount));
                                resultFuture.complete(rows);
                                if (cache != null) {
                                    cache.put(GenericRowData.of(keys), rows);
                                }
                            } else {
                                RowData row =
                                        deserializationSchema.deserialize(
                                                result.getBytes(StandardCharsets.UTF_8));
                                List<RowData> rows =
                                        Collections.singletonList(new JoinedRowData(GenericRowData.of(keys[0]), row));
                                resultFuture.complete(rows);
                                if (cache != null) {
                                    cache.put(GenericRowData.of(keys), rows);
                                }
                            }
                        } catch (Exception e) {
                            LOG.error("Redis asyncLookup error", e);
                            if (ignoreInvokeErrors) {
                                resultFuture.complete(Collections.emptyList());
                            } else {
                                resultFuture.completeExceptionally(e);
                            }
                        }
                    }
                });
    }

    private void batchFetchResult(
            Map<Object[], CompletableFuture<Collection<RowData>>> unhandCollection) {
        if (cache != null) {
            Iterator<Map.Entry<Object[], CompletableFuture<Collection<RowData>>>> it =
                    unhandCollection.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Object[], CompletableFuture<Collection<RowData>>> entry = it.next();
                RowData keyRow = GenericRowData.of(entry.getKey());
                List<RowData> cacheRowData = cache.getIfPresent(keyRow);
                if (cacheRowData != null) {
                    LOG.debug("found row data from cache: {}", cacheRowData);
                    if (CollectionUtils.isEmpty(cacheRowData)) {
                        entry.getValue().complete(Collections.emptyList());
                    } else {
                        entry.getValue().complete(cacheRowData);
                    }
                    it.remove();
                }
            }
        }

        batchFetch(unhandCollection);
    }

    private void batchFetch(
            Map<Object[], CompletableFuture<Collection<RowData>>> unhandCollection) {

        LOG.info("unhandCollection: {}", unhandCollection);
        String[] ks =
                unhandCollection.keySet().stream().map(o -> o[0].toString()).toArray(String[]::new);
        LOG.info("ks: {}", Arrays.toString(ks));
        LOG.info("hash name : {}", hashName);
        RedisFuture<List<KeyValue<String, String>>> responseFuture =
                StringUtils.isBlank(hashName) ? async.mget(ks) : async.hmget(hashName, ks);
        responseFuture.whenCompleteAsync(
                (result, throwable) -> {
                    try {
                        unhandCollection.forEach(
                                (k, v) -> {
                                    if (throwable != null) {
                                        LOG.error("Redis asyncLookup error", throwable);
                                        v.completeExceptionally(throwable);
                                    } else {
                                        if (result.isEmpty()) {
                                            v.complete(Collections.emptyList());
                                            if (cache != null) {
                                                cache.put(
                                                        GenericRowData.of(k),
                                                        Collections.singletonList(
                                                                new GenericRowData(fieldCount)));
                                            }
                                        } else {
                                            LOG.info("result is: {}", result);
                                            LOG.info("k: {}, type: {}", k, k.getClass());
                                            LOG.info("v is: {}", v);
                                            result.stream()
                                                    .filter(r -> r.getKey().equals(k[0].toString()))
                                                    .findAny()
                                                    .ifPresent(
                                                            r -> {
                                                                LOG.info("r: {}", r);
                                                                try {
                                                                    RowData row =
                                                                            r.hasValue()
                                                                                    ? deserializationSchema
                                                                                            .deserialize(
                                                                                                    r.getValue()
                                                                                                            .getBytes(
                                                                                                                    StandardCharsets
                                                                                                                            .UTF_8))
                                                                                    : new GenericRowData(
                                                                                            fieldCount);
                                                                    List<RowData> rows =
                                                                            Collections.singletonList(new JoinedRowData(GenericRowData.of(r.getKey()), row));
                                                                    LOG.info("rows: {}", rows);
                                                                    v.complete(rows);
                                                                    if (cache != null) {
                                                                        cache.put(
                                                                                GenericRowData.of(
                                                                                        k),
                                                                                rows);
                                                                    }
                                                                } catch (Exception e) {
                                                                    LOG.error(
                                                                            "Redis asyncLookup error",
                                                                            e);
                                                                    if (ignoreInvokeErrors) {
                                                                        v.complete(
                                                                                Collections
                                                                                        .emptyList());
                                                                    } else {
                                                                        v.completeExceptionally(e);
                                                                    }
                                                                }
                                                            });
                                        }
                                    }
                                });
                    } finally {
                        unhandCollection
                                .keySet()
                                .forEach(
                                        k -> {
                                            CompletableFuture<Collection<RowData>> removeFuture =
                                                    batchCollection.remove(k);
                                            LOG.info(
                                                    "k: {}, remove future is: {}", k, removeFuture);
                                        });
                    }
                });
    }

    private void triggerFetchResult() {
        if (MapUtils.isNotEmpty(batchCollection)) {
            Map<Object[], CompletableFuture<Collection<RowData>>> unhandCollection =
                    new HashMap<>();
            Iterator<Map.Entry<Object[], CompletableFuture<Collection<RowData>>>> it =
                    batchCollection.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Object[], CompletableFuture<Collection<RowData>>> next = it.next();
                unhandCollection.put(next.getKey(), next.getValue());
                it.remove();
            }
            batchFetchResult(unhandCollection);
        }
    }

    private void buildRedisClient() {
        switch (RedisMode.parse(mode)) {
            case SINGLE:
                RedisURI redisUri = RedisURI.create("redis://" + nodes);
                redisUri.setDatabase(db);
                if (StringUtils.isNotBlank(password)) {
                    redisUri.setPassword(password);
                }
                redisClient = RedisClient.create(redisUri);
                async = redisClient.connect().async();
                break;
            case SENTINEL:
                RedisURI redisSentinelUri =
                        RedisURI.create("redis-sentinel://" + nodes + "?timeout=10s#" + masterName);
                redisSentinelUri.setDatabase(db);
                if (StringUtils.isNotBlank(password)) {
                    redisSentinelUri.setPassword(password);
                }
                redisSentinelUri.setSentinelMasterId(masterName);
                redisClient = RedisClient.create(redisSentinelUri);
                async = redisClient.connect().async();
                break;
            case CLUSTER:
                RedisURI redisClusterUri = RedisURI.create("redis://" + nodes);
                redisClusterUri.setDatabase(db);
                if (StringUtils.isNotBlank(password)) {
                    redisClusterUri.setPassword(password);
                }
                redisClient = RedisClient.create(redisClusterUri);
                this.async = redisClient.connect().async();
            default:
                LOG.error("Illegal argument of redis mode: " + mode);
                break;
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
        if (redisClient != null) {
            redisClient.shutdown();
        }
        LOG.info("end close.");
    }
}
