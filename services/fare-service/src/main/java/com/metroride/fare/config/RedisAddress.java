package com.metroride.fare.config;

/** The {@code host:port} form the Go services accept in {@code REDIS_ADDR}. */
public record RedisAddress(String host, int port) {

    private static final int DEFAULT_PORT = 6379;

    public static RedisAddress parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("REDIS_ADDR must not be blank");
        }
        String trimmed = value.trim();
        if (trimmed.contains("://")) {
            throw new IllegalArgumentException("REDIS_ADDR must be host:port, not a URL, got '" + value + "'");
        }
        int separator = trimmed.lastIndexOf(':');
        if (separator < 0) {
            return new RedisAddress(trimmed, DEFAULT_PORT);
        }
        String host = trimmed.substring(0, separator);
        if (host.isEmpty()) {
            throw new IllegalArgumentException("REDIS_ADDR must include a host, got '" + value + "'");
        }
        try {
            int port = Integer.parseInt(trimmed.substring(separator + 1));
            if (port < 1 || port > 65535) {
                throw new IllegalArgumentException("REDIS_ADDR port out of range, got '" + value + "'");
            }
            return new RedisAddress(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("REDIS_ADDR port must be numeric, got '" + value + "'", e);
        }
    }
}
