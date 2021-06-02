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

package org.apache.flink.connectors.http.table.options;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;


import java.time.Duration;
import java.util.Map;


/** Common Options for HTTP. */
@Internal
public class HttpOptions {

	public static final ConfigOption<String> REQUEST_URL =
			ConfigOptions.key("request.url")
				.stringType()
				.noDefaultValue()
				.withDescription("The url of http table to request.");

	public static final ConfigOption<String> REQUEST_METHOD =
			ConfigOptions.key("request.method")
				.stringType()
				.defaultValue("POST")
				.withDescription("The request method of http table to request.");

	public static final ConfigOption<Map<String, String>> REQUEST_HEADERS =
			ConfigOptions.key("request.headers")
				.mapType()
				.noDefaultValue()
				.withDescription("The request headers of http table to request.");

	public static final ConfigOption<Long> REQUEST_BATCH_SIZE =
			ConfigOptions.key("request.batch.size")
					.longType()
					.defaultValue(-1L)
					.withDescription("The request batch size of http table to request.");

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

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

	public static HttpRequestOptions getHttpRequestOptions(ReadableConfig tableOptions) {
		HttpRequestOptions.Builder builder = HttpRequestOptions.builder();
		builder.setRequestUrl(tableOptions.get(REQUEST_URL));
		builder.setRequestMethod(tableOptions.get(REQUEST_METHOD));
		builder.setRequestHeaders(tableOptions.get(REQUEST_HEADERS));
		builder.setRequestBatchSize(tableOptions.get(REQUEST_BATCH_SIZE));
		return builder.build();
	}

    public static HttpLookupOptions getHttpLookupOptions(ReadableConfig tableOptions) {
		HttpLookupOptions.Builder builder = HttpLookupOptions.builder();
        builder.setLookupAsync(tableOptions.get(LOOKUP_ASYNC));
        builder.setCacheExpireMs(tableOptions.get(LOOKUP_CACHE_TTL).toMillis());
        builder.setCacheMaxSize(tableOptions.get(LOOKUP_CACHE_MAX_ROWS));
        return builder.build();
    }
}
