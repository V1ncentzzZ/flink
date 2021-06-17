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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Options for the Http lookup. */
@Internal
public class HttpRequestOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String requestUrl;
    private final String requestMethod;
    private final List<String> requestParameters;
    private final Map<String, String> requestHeaders;
    private final Long requestBatchSize;
    private final Long requestSendInterval;
    private final Integer requestTimeout;
    private final Integer requestMaxRetries;
    private final Integer requestSocketTimeout;
    private final Integer requestConnectTimout;

    public HttpRequestOptions(
            String requestUrl,
            String requestMethod,
            List<String> requestParameters,
            Map<String, String> requestHeaders,
            Long requestBatchSize,
            Long requestSendInterval,
            Integer requestTimeout,
            Integer requestMaxRetries,
            Integer requestSocketTimeout,
            Integer requestConnectTimout) {
        this.requestUrl = requestUrl;
        this.requestMethod = requestMethod;
        this.requestParameters = requestParameters;
        this.requestHeaders = requestHeaders;
        this.requestBatchSize = requestBatchSize;
        this.requestSendInterval = requestSendInterval;
        this.requestTimeout = requestTimeout;
        this.requestMaxRetries = requestMaxRetries;
        this.requestSocketTimeout = requestSocketTimeout;
        this.requestConnectTimout = requestConnectTimout;
    }

    public String getRequestUrl() {
        return requestUrl;
    }

    public String getRequestMethod() {
        return requestMethod;
    }

    public List<String> getRequestParameters() {
        return requestParameters;
    }

    public Map<String, String> getRequestHeaders() {
        return requestHeaders;
    }

    public Long getRequestBatchSize() {
        return requestBatchSize;
    }

    public Long getRequestSendInterval() {
        return requestSendInterval;
    }

    public Integer getRequestTimeout() {
        return requestTimeout;
    }

    public Integer getRequestMaxRetries() {
        return requestMaxRetries;
    }

    public Integer getRequestSocketTimeout() {
        return requestSocketTimeout;
    }

    public Integer getRequestConnectTimout() {
        return requestConnectTimout;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof HttpRequestOptions) {
            HttpRequestOptions options = (HttpRequestOptions) o;
            return Objects.equals(requestUrl, options.requestUrl)
                    && Objects.equals(requestMethod, options.requestMethod)
                    && Objects.equals(requestParameters, options.requestParameters)
                    && Objects.equals(requestHeaders, options.requestHeaders)
                    && Objects.equals(requestBatchSize, options.requestBatchSize)
                    && Objects.equals(requestSendInterval, options.requestSendInterval)
                    && Objects.equals(requestTimeout, options.requestTimeout)
                    && Objects.equals(requestMaxRetries, options.requestMaxRetries)
                    && Objects.equals(requestSocketTimeout, options.requestSocketTimeout)
                    && Objects.equals(requestConnectTimout, options.requestConnectTimout);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                requestUrl,
                requestMethod,
                requestParameters,
                requestHeaders,
                requestBatchSize,
                requestSendInterval,
                requestTimeout,
                requestMaxRetries,
                requestSocketTimeout,
                requestConnectTimout);
    }

    /** Builder of {@link HttpRequestOptions}. */
    public static class Builder {
        private String requestUrl;
        private String requestMethod;
        private List<String> requestParameters;
        private Map<String, String> requestHeaders;
        private Long requestBatchSize;
        private Long requestSendInterval;
        private Integer requestTimeout;
        private Integer requestMaxRetries;
        private Integer requestSocketTimeout;
        private Integer requestConnectTimout;

        public Builder setRequestUrl(String requestUrl) {
            this.requestUrl = requestUrl;
            return this;
        }

        public Builder setRequestMethod(String requestMethod) {
            this.requestMethod = requestMethod;
            return this;
        }

        public Builder setRequestParameters(List<String> requestParameters) {
            this.requestParameters = requestParameters;
            return this;
        }

        public Builder setRequestHeaders(Map<String, String> requestHeaders) {
            this.requestHeaders = requestHeaders;
            return this;
        }

        public void setRequestBatchSize(Long requestBatchSize) {
            this.requestBatchSize = requestBatchSize;
        }

        public void setRequestSendInterval(Long requestSendInterval) {
            this.requestSendInterval = requestSendInterval;
        }

        public void setRequestTimeout(Integer requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        public void setRequestMaxRetries(Integer requestMaxRetries) {
            this.requestMaxRetries = requestMaxRetries;
        }

        public void setRequestSocketTimeout(Integer requestSocketTimeout) {
            this.requestSocketTimeout = requestSocketTimeout;
        }

        public void setRequestConnectTimout(Integer requestConnectTimout) {
            this.requestConnectTimout = requestConnectTimout;
        }

        public HttpRequestOptions build() {
            return new HttpRequestOptions(
                    requestUrl,
                    requestMethod,
                    requestParameters,
                    requestHeaders,
                    requestBatchSize,
                    requestSendInterval,
                    requestTimeout,
                    requestMaxRetries,
                    requestSocketTimeout,
                    requestConnectTimout);
        }
    }
}
