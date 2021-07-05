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

import java.io.Serializable;
import java.util.Objects;

/** Options for the Http lookup. */
@Internal
public class RedisLookupOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final boolean lookupAsync;
    private final long lookupBatchSize;
    private final long lookupSendInterval;

    public RedisLookupOptions(
            long cacheMaxSize,
            long cacheExpireMs,
            boolean lookupAsync,
            long lookupBatchSize,
            long lookupSendInterval) {
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.lookupAsync = lookupAsync;
        this.lookupBatchSize = lookupBatchSize;
        this.lookupSendInterval = lookupSendInterval;
    }

    public static Builder builder() {
        return new Builder();
    }

    public long getCacheMaxSize() {
        return cacheMaxSize;
    }

    public long getCacheExpireMs() {
        return cacheExpireMs;
    }

    public boolean getLookupAsync() {
        return lookupAsync;
    }

    public boolean isLookupAsync() {
        return lookupAsync;
    }

    public long getLookupBatchSize() {
        return lookupBatchSize;
    }

    public long getLookupSendInterval() {
        return lookupSendInterval;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RedisLookupOptions) {
            RedisLookupOptions options = (RedisLookupOptions) o;
            return Objects.equals(cacheMaxSize, options.cacheMaxSize)
                    && Objects.equals(cacheExpireMs, options.cacheExpireMs)
                    && Objects.equals(lookupAsync, options.lookupAsync);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(cacheMaxSize, cacheExpireMs, lookupAsync);
    }

    /** Builder of {@link RedisLookupOptions}. */
    public static class Builder {
        private long cacheMaxSize = -1L;
        private long cacheExpireMs = 0L;
        private boolean lookupAsync = false;
        private long lookupBatchSize = -1L;
        private long lookupSendInterval = 60000;

        /** optional, lookup cache max size, over this value, the old data will be eliminated. */
        public Builder setCacheMaxSize(long cacheMaxSize) {
            this.cacheMaxSize = cacheMaxSize;
            return this;
        }

        /** optional, lookup cache expire mills, over this time, the old data will expire. */
        public Builder setCacheExpireMs(long cacheExpireMs) {
            this.cacheExpireMs = cacheExpireMs;
            return this;
        }

        /** optional, whether to set async lookup. */
        public Builder setLookupAsync(boolean lookupAsync) {
            this.lookupAsync = lookupAsync;
            return this;
        }

        /** optional, the batch size to set async lookup. */
        public Builder setLookupBatchSize(long lookupBatchSize) {
            this.lookupBatchSize = lookupBatchSize;
            return this;
        }

        /** optional, the send interval to set async lookup. */
        public Builder setLookupSendInterval(long lookupSendInterval) {
            this.lookupSendInterval = lookupSendInterval;
            return this;
        }

        public RedisLookupOptions build() {
            return new RedisLookupOptions(
                    cacheMaxSize, cacheExpireMs, lookupAsync, lookupBatchSize, lookupSendInterval);
        }
    }
}
