package org.apache.flink.connectors.redis.table.options;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

/** Options for the Http optional. */
@Internal
public class RedisConnectionOptions {
    private static final long serialVersionUID = 1L;

    private final String nodes;
    private final Integer database;
    private final String password;
    private final String mode;
    private final String hashName;
    private final String masterName;

    public RedisConnectionOptions(
            String nodes,
            Integer database,
            String password,
            String mode,
            String hashName,
            String masterName) {
        this.nodes = nodes;
        this.database = database;
        this.password = password;
        this.mode = mode;
        this.hashName = hashName;
        this.masterName = masterName;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getNodes() {
        return nodes;
    }

    public Integer getDatabase() {
        return database;
    }

    public String getPassword() {
        return password;
    }

    public String getMode() {
        return mode;
    }

    public String getHashName() {
        return hashName;
    }

    public String getMasterName() {
        return masterName;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof RedisConnectionOptions) {
            RedisConnectionOptions options = (RedisConnectionOptions) o;
            return Objects.equals(nodes, options.nodes)
                    && Objects.equals(database, options.database)
                    && Objects.equals(password, options.password)
                    && Objects.equals(mode, options.mode)
                    && Objects.equals(hashName, options.hashName)
                    && Objects.equals(masterName, options.masterName);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes, database, password, mode, hashName, masterName);
    }

    /** Builder of {@link RedisConnectionOptions}. */
    public static class Builder {
        private String nodes;
        private Integer database;
        private String password;
        private String mode;
        private String hashName;
        private String masterName;

        public Builder setNodes(String nodes) {
            this.nodes = nodes;
            return this;
        }

        public Builder setDatabase(Integer database) {
            this.database = database;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setMode(String mode) {
            this.mode = mode;
            return this;
        }

        public Builder setHashName(String hashName) {
            this.hashName = hashName;
            return this;
        }

        public Builder setMasterName(String masterName) {
            this.masterName = masterName;
            return this;
        }

        public RedisConnectionOptions build() {
            return new RedisConnectionOptions(
                    nodes, database, password, mode, hashName, masterName);
        }
    }
}
