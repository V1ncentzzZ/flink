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
import java.util.Map;
import java.util.Objects;

/** Options for the Http lookup. */
@Internal
public class HttpRequestOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String requestUrl;
    private final String requestMethod;
    private final Map<String, String> requestHeaders;
	private final Long requestBatchSize;

    public HttpRequestOptions(String requestUrl, String requestMethod, Map<String, String> requestHeaders, Long requestBatchSize) {
        this.requestUrl = requestUrl;
        this.requestMethod = requestMethod;
        this.requestHeaders = requestHeaders;
        this.requestBatchSize = requestBatchSize;
    }

	public String getRequestUrl() {
		return requestUrl;
	}

	public String getRequestMethod() {
		return requestMethod;
	}

	public Map<String, String> getRequestHeaders() {
		return requestHeaders;
	}

	public Long getRequestBatchSize() {
		return requestBatchSize;
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
                    && Objects.equals(requestHeaders, options.requestHeaders)
                    && Objects.equals(requestBatchSize, options.requestBatchSize);
        } else {
            return false;
        }
    }

    /** Builder of {@link HttpRequestOptions}. */
    public static class Builder {
		private String requestUrl;
		private String requestMethod;
		private Map<String, String> requestHeaders;
		private Long requestBatchSize;

        public Builder setRequestUrl(String requestUrl) {
            this.requestUrl = requestUrl;
            return this;
        }

        public Builder setRequestMethod(String requestMethod) {
            this.requestMethod = requestMethod;
            return this;
        }

        public Builder setRequestHeaders(Map<String, String> requestHeaders) {
            this.requestHeaders = requestHeaders;
            return this;
        }

		public void setRequestBatchSize(Long requestBatchSize) {
			this.requestBatchSize = requestBatchSize;
		}

		public HttpRequestOptions build() {
            return new HttpRequestOptions(requestUrl, requestMethod, requestHeaders, requestBatchSize);
        }
    }
}
