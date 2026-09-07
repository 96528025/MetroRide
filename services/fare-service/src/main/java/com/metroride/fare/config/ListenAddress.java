package com.metroride.fare.config;

import java.util.Optional;

/**
 * The listen address the Go services accept in {@code <SERVICE>_ADDR}: {@code ":8087"} binds every
 * interface, {@code "127.0.0.1:8087"} binds one. Spring needs the two halves separately
 * ({@code server.address} and {@code server.port}), which is what this record provides.
 */
public record ListenAddress(String host, int port) {

    public static ListenAddress parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("listen address must not be blank");
        }
        int separator = value.lastIndexOf(':');
        if (separator < 0) {
            throw new IllegalArgumentException("listen address must be host:port or :port, got '" + value + "'");
        }
        String host = value.substring(0, separator).trim();
        if (host.startsWith("[") && host.endsWith("]")) {
            host = host.substring(1, host.length() - 1);
        }
        String portText = value.substring(separator + 1).trim();
        int port;
        try {
            port = Integer.parseInt(portText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("listen address port must be numeric, got '" + value + "'", e);
        }
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("listen address port out of range, got '" + value + "'");
        }
        return new ListenAddress(host, port);
    }

    /** Empty when the address binds every interface, i.e. the Go form {@code ":8087"}. */
    public Optional<String> bindHost() {
        return host.isEmpty() ? Optional.empty() : Optional.of(host);
    }
}
