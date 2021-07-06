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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.utils.RedisMode;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The HttpRowDataLookupFunction is a standard user-defined table function, it can be used in
 * tableAPI and also useful for temporal table join plan in SQL. It looks up the result as {@link
 * RowData}.
 */
@Internal
public class RedisRowDataLookupFunction extends TableFunction<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisRowDataLookupFunction.class);
    private static final long serialVersionUID = 1L;

    private final int fieldCount;
    private final String[] lookupKeys;

    private final String nodes;
    private final int db;
    private final String mode;
    private final String password;
    private final String hashName;
    private final String masterName;

    private final long cacheMaxSize;
    private final long cacheExpireMs;

    private final boolean ignoreInvokeErrors;

    private final DeserializationSchema<RowData> deserializationSchema;

    private transient Cache<Object, List<RowData>> cache;
    private transient RedisClient redisClient;
    private transient RedisCommands<String, String> sync;

    public RedisRowDataLookupFunction(
            TableSchema tableSchema,
            String[] lookupKeys,
            RedisLookupOptions lookupOptions,
            RedisConnectionOptions connectionOptions,
            RedisOptionalOptions optionalOptions,
            DeserializationSchema<RowData> deserializationSchema) {

        this.fieldCount = tableSchema.getFieldCount();
        this.lookupKeys = lookupKeys;

        this.nodes = connectionOptions.getNodes();
        this.db = connectionOptions.getDatabase();
        this.mode = connectionOptions.getMode();
        this.password = connectionOptions.getPassword();
        this.hashName = connectionOptions.getHashName();
        this.masterName = connectionOptions.getMasterName();

        this.cacheMaxSize = lookupOptions.getCacheMaxSize();
        this.cacheExpireMs = lookupOptions.getCacheExpireMs();

        this.ignoreInvokeErrors = optionalOptions.isIgnoreInvokeErrors();

        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(FunctionContext context) {
        LOG.info("start open ...");
        try {
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
     * @param key the lookup key.
     */
    public void eval(Object key) {
        if (cache != null) {
            List<RowData> cacheRowData = cache.getIfPresent(GenericRowData.of(key));
            if (cacheRowData != null) {
                LOG.info("found row data from cache: {}", cacheRowData);
                for (RowData rowData : cacheRowData) {
                    collect(rowData);
                }
                return;
            }
        }
        // fetch result
        LOG.info("not found data from cache, do fetch result，keys: {}", key);
        fetchResult(key);
    }

    private void fetchResult(Object key) {
        try {
            String keyValue =
                    StringUtils.isBlank(hashName)
                            ? sync.get(String.valueOf(key))
                            : sync.hget(hashName, String.valueOf(key));
            RowData row =
                    StringUtils.isBlank(keyValue)
                            ? new GenericRowData(fieldCount)
                            : deserializationSchema.deserialize(
                                    keyValue.getBytes(StandardCharsets.UTF_8));
            collect(new JoinedRowData(GenericRowData.of(key), row));
            if (cache != null) {
                cache.put(GenericRowData.of(key), Collections.singletonList(row));
            }
        } catch (Exception e) {
            LOG.error("Failed to fetch result from redis.", e);
            if (!ignoreInvokeErrors) {
                throw new RuntimeException(e);
            }
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
                sync = redisClient.connect().sync();
                break;
            case SENTINEL:
                RedisURI redisSentinelUri =
                        RedisURI.create("redis-sentinel://" + nodes + "?timeout=10s#mymaster");
                redisSentinelUri.setDatabase(db);
                if (StringUtils.isNotBlank(password)) {
                    redisSentinelUri.setPassword(password);
                }
                redisSentinelUri.setSentinelMasterId(masterName);
                redisClient = RedisClient.create(redisSentinelUri);
                sync = redisClient.connect().sync();
                break;
            case CLUSTER:
                RedisURI redisClusterUri = RedisURI.create("redis://" + nodes);
                redisClusterUri.setDatabase(db);
                if (StringUtils.isNotBlank(password)) {
                    redisClusterUri.setPassword(password);
                }
                redisClient = RedisClient.create(redisClusterUri);
                sync = redisClient.connect().sync();
            default:
                LOG.error("Illegal argument of redis mode: " + mode);
                break;
        }
    }

    @Override
    public void close() {
        LOG.info("start close ...");
        if (redisClient != null) {
            redisClient.shutdown();
        }
        LOG.info("end close.");
    }
}
