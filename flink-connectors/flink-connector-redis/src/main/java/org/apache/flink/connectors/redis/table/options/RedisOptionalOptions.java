package org.apache.flink.connectors.redis.table.options;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** Options for the Redis optional. */
@Internal
public class RedisOptionalOptions {
    private static final long serialVersionUID = 1L;

    private final boolean ignoreInvokeErrors;

    public RedisOptionalOptions(boolean ignoreInvokeErrors) {
        this.ignoreInvokeErrors = ignoreInvokeErrors;
    }

    public static Builder builder() {
        return new Builder();
    }

    public boolean isIgnoreInvokeErrors() {
        return ignoreInvokeErrors;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RedisOptionalOptions) {
            RedisOptionalOptions options = (RedisOptionalOptions) o;
            return Objects.equals(ignoreInvokeErrors, options.ignoreInvokeErrors);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreInvokeErrors);
    }

    /** Builder of {@link RedisOptionalOptions}. */
    public static class Builder {
        private boolean ignoreInvokeErrors = false;

        /** optional, whether to ignore invoke errors. */
        public Builder setIgnoreInvokeErrors(boolean ignoreInvokeErrors) {
            this.ignoreInvokeErrors = ignoreInvokeErrors;
            return this;
        }

        public RedisOptionalOptions build() {
            return new RedisOptionalOptions(ignoreInvokeErrors);
        }
    }
}
