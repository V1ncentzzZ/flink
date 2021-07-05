package org.apache.flink.utils;

public enum RedisMode {
    /** redis modes */
    SINGLE(1),
    SENTINEL(2),
    CLUSTER(3);

    int type;

    RedisMode(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }

    public static RedisMode parse(String redisMode) {
        for (RedisMode type : RedisMode.values()) {
            if (type.name().equals(redisMode.toUpperCase())) {
                return type;
            }
        }
        throw new RuntimeException("unsupport redis mode[" + redisMode + "]");
    }
}
