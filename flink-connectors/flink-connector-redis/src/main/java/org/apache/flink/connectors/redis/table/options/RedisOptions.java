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

package org.apache.flink.connectors.redis.table.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.time.Duration;

/** Common Options for Redis. */
@Internal
public class RedisOptions {

    public static final ConfigOption<String> REDIS_MODE =
            ConfigOptions.key("redis.mode")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The mode of redis component.");

    public static final ConfigOption<String> REDIS_NODES =
            ConfigOptions.key("redis.nodes")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The nodes of redis component.");

    public static final ConfigOption<Integer> REDIS_DATABASE =
            ConfigOptions.key("redis.database")
                    .intType()
                    .defaultValue(0)
                    .withDescription("The database number of redis component, default value is 0.");

    public static final ConfigOption<String> REDIS_PASSWORD =
            ConfigOptions.key("redis.password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The password of redis component.");

    public static final ConfigOption<String> HASH_NAME =
            ConfigOptions.key("hash.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The hashName of redis component.");

    public static final ConfigOption<String> MASTER_NAME =
            ConfigOptions.key("master.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The master name of redis component, only affect sentinel mode.");

    public static final ConfigOption<Boolean> LOOKUP_ASYNC =
            ConfigOptions.key("lookup.async")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("whether to set async lookup.");

    public static final ConfigOption<Long> LOOKUP_CACHE_MAX_ROWS =
            ConfigOptions.key("lookup.cache.max-rows")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription(
                            "the max number of rows of lookup cache, over this value, the oldest rows will "
                                    + "be eliminated. \"cache.max-rows\" and \"cache.ttl\" options must all be specified if any of them is "
                                    + "specified. Cache is not enabled as default.");

    public static final ConfigOption<Duration> LOOKUP_CACHE_TTL =
            ConfigOptions.key("lookup.cache.ttl")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(0))
                    .withDescription("the cache time to live.");

    public static final ConfigOption<Long> LOOKUP_BATCH_SIZE =
            ConfigOptions.key("lookup.batch.size")
                    .longType()
                    .defaultValue(-1L)
                    .withDescription("The request batch size of http table to request.");

    public static final ConfigOption<Duration> LOOKUP_SEND_INTERVAL =
            ConfigOptions.key("lookup.send.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(60))
                    .withDescription("The request send interval of http table to request.");

    public static final ConfigOption<Boolean> IGNORE_INVOKE_ERRORS =
            ConfigOptions.key("ignore.invoke.errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip errors when send a http request, false by default");

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static RedisLookupOptions getRedisLookupOptions(ReadableConfig tableOptions) {
        RedisLookupOptions.Builder builder = RedisLookupOptions.builder();
        builder.setLookupAsync(tableOptions.get(LOOKUP_ASYNC));
        builder.setCacheExpireMs(tableOptions.get(LOOKUP_CACHE_TTL).toMillis());
        builder.setCacheMaxSize(tableOptions.get(LOOKUP_CACHE_MAX_ROWS));
        builder.setLookupBatchSize(tableOptions.get(LOOKUP_BATCH_SIZE));
        builder.setLookupSendInterval(tableOptions.get(LOOKUP_SEND_INTERVAL).toMillis());
        return builder.build();
    }

    public static RedisOptionalOptions getRedisOptionalOptions(ReadableConfig tableOptions) {
        RedisOptionalOptions.Builder builder = RedisOptionalOptions.builder();
        builder.setIgnoreInvokeErrors(tableOptions.get(IGNORE_INVOKE_ERRORS));
        return builder.build();
    }

    public static RedisConnectionOptions getRedisConnectionOptions(ReadableConfig tableOptions) {
        RedisConnectionOptions.Builder builder = RedisConnectionOptions.builder();
        builder.setNodes(tableOptions.get(REDIS_NODES));
        builder.setDatabase(tableOptions.get(REDIS_DATABASE));
        builder.setPassword(tableOptions.get(REDIS_PASSWORD));
        builder.setMode(tableOptions.get(REDIS_MODE));
        builder.setHashName(tableOptions.get(HASH_NAME));
        builder.setMasterName(tableOptions.get(MASTER_NAME));
        return builder.build();
    }
}
