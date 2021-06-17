package org.apache.flink.connectors.http.table.options;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** Options for the Http optional. */
@Internal
public class HttpOptionalOptions {
    private static final long serialVersionUID = 1L;

    private final boolean ignoreInvokeErrors;

    public HttpOptionalOptions(boolean ignoreInvokeErrors) {
        this.ignoreInvokeErrors = ignoreInvokeErrors;
    }

    public boolean isIgnoreInvokeErrors() {
        return ignoreInvokeErrors;
    }

    public static HttpOptionalOptions.Builder builder() {
        return new HttpOptionalOptions.Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof HttpOptionalOptions) {
            HttpOptionalOptions options = (HttpOptionalOptions) o;
            return Objects.equals(ignoreInvokeErrors, options.ignoreInvokeErrors);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(ignoreInvokeErrors);
    }

    /** Builder of {@link HttpOptionalOptions}. */
    public static class Builder {
        private boolean ignoreInvokeErrors = false;

        /** optional, whether to ignore invoke errors. */
        public HttpOptionalOptions.Builder setIgnoreInvokeErrors(boolean ignoreInvokeErrors) {
            this.ignoreInvokeErrors = ignoreInvokeErrors;
            return this;
        }

        public HttpOptionalOptions build() {
            return new HttpOptionalOptions(ignoreInvokeErrors);
        }
    }
}
