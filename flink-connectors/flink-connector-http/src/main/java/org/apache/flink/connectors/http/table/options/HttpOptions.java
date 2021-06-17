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
import java.util.List;
import java.util.Map;

import static org.apache.flink.connectors.http.table.deserialization.HttpJsonFormatFactory.IDENTIFIER;
import static org.apache.flink.utils.HttpConstants.DEFAULT_CONNECT_TIMEOUT;
import static org.apache.flink.utils.HttpConstants.DEFAULT_REQUEST_TIMEOUT;
import static org.apache.flink.utils.HttpConstants.DEFAULT_RETRY_COUNT;
import static org.apache.flink.utils.HttpConstants.DEFAULT_SOCKET_TIMEOUT;

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

    public static final ConfigOption<List<String>> REQUEST_PARAMETERS =
            ConfigOptions.key("request.parameters")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("The parameter names of http request.");

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

    public static final ConfigOption<Duration> REQUEST_SEND_INTERVAL =
            ConfigOptions.key("request.send.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("The request send interval of http table to request.");

    public static final ConfigOption<Duration> REQUEST_TIMEOUT =
            ConfigOptions.key("request.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(DEFAULT_REQUEST_TIMEOUT))
                    .withDescription("The timeout to send a http request");

    public static final ConfigOption<Integer> REQUEST_MAX_RETRIES =
            ConfigOptions.key("request.max.retries")
                    .intType()
                    .defaultValue(DEFAULT_RETRY_COUNT)
                    .withDescription("The max retries to send a http request");

    public static final ConfigOption<Duration> REQUEST_SOCKET_TIMEOUT =
            ConfigOptions.key("request.socket.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(DEFAULT_SOCKET_TIMEOUT))
                    .withDescription("The socket timeout to send a http request");

    public static final ConfigOption<Duration> REQUEST_CONNECT_TIMEOUT =
            ConfigOptions.key("request.connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofMillis(DEFAULT_CONNECT_TIMEOUT))
                    .withDescription("The connect timeout to send a http request");

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

    public static final ConfigOption<Boolean> IGNORE_INVOKE_ERRORS =
            ConfigOptions.key("ignore.invoke.errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip errors when send a http request, false by default");

    public static final ConfigOption<List<String>> RESPONSE_DATA_FIELDS =
            ConfigOptions.key("response.data.fields")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("The data fields of response.");

    public static final String FORMAT_PROPERTIES_PREFIX = IDENTIFIER + ".";

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    public static HttpRequestOptions getHttpRequestOptions(ReadableConfig tableOptions) {
        HttpRequestOptions.Builder builder = HttpRequestOptions.builder();
        builder.setRequestUrl(tableOptions.get(REQUEST_URL));
        builder.setRequestMethod(tableOptions.get(REQUEST_METHOD));
        builder.setRequestParameters(tableOptions.get(REQUEST_PARAMETERS));
        builder.setRequestHeaders(tableOptions.get(REQUEST_HEADERS));
        builder.setRequestBatchSize(tableOptions.get(REQUEST_BATCH_SIZE));
        builder.setRequestSendInterval(tableOptions.get(REQUEST_SEND_INTERVAL).toMillis());
        builder.setRequestTimeout((int) tableOptions.get(REQUEST_TIMEOUT).toMillis());
        builder.setRequestMaxRetries(tableOptions.get(REQUEST_MAX_RETRIES));
        builder.setRequestSocketTimeout((int) tableOptions.get(REQUEST_SOCKET_TIMEOUT).toMillis());
        builder.setRequestConnectTimout((int) tableOptions.get(REQUEST_CONNECT_TIMEOUT).toMillis());
        return builder.build();
    }

    public static HttpLookupOptions getHttpLookupOptions(ReadableConfig tableOptions) {
        HttpLookupOptions.Builder builder = HttpLookupOptions.builder();
        builder.setLookupAsync(tableOptions.get(LOOKUP_ASYNC));
        builder.setCacheExpireMs(tableOptions.get(LOOKUP_CACHE_TTL).toMillis());
        builder.setCacheMaxSize(tableOptions.get(LOOKUP_CACHE_MAX_ROWS));
        return builder.build();
    }

    public static HttpOptionalOptions getHttpOptionalOptions(ReadableConfig tableOptions) {
        HttpOptionalOptions.Builder builder = HttpOptionalOptions.builder();
        builder.setIgnoreInvokeErrors(tableOptions.get(IGNORE_INVOKE_ERRORS));
        return builder.build();
    }
}
