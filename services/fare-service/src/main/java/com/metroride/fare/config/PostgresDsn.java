package com.metroride.fare.config;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Translates the libpq-style {@code POSTGRES_DSN} the Go services use
 * ({@code postgres://user:password@host:port/database?sslmode=disable}) into the JDBC URL and
 * credentials Spring's DataSource needs. Only the URI form is supported; the query string is
 * passed through unchanged, so it must contain parameters the PostgreSQL JDBC driver understands
 * ({@code sslmode} is one of them).
 */
public record PostgresDsn(String jdbcUrl, String username, String password) {

    private static final int DEFAULT_PORT = 5432;

    public static PostgresDsn parse(String dsn) {
        if (dsn == null || dsn.isBlank()) {
            throw new IllegalArgumentException("POSTGRES_DSN must not be blank");
        }
        URI uri;
        try {
            uri = new URI(dsn.trim());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("POSTGRES_DSN is not a valid URI: " + e.getMessage(), e);
        }
        String scheme = uri.getScheme();
        if (!"postgres".equals(scheme) && !"postgresql".equals(scheme)) {
            throw new IllegalArgumentException(
                    "POSTGRES_DSN must use the postgres:// or postgresql:// scheme, got '" + scheme + "'");
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("POSTGRES_DSN must include a host");
        }
        String path = uri.getPath();
        if (path == null || path.length() <= 1) {
            throw new IllegalArgumentException("POSTGRES_DSN must include a database name");
        }

        String username = null;
        String password = null;
        String userInfo = uri.getUserInfo();
        if (userInfo != null && !userInfo.isEmpty()) {
            int colon = userInfo.indexOf(':');
            username = colon < 0 ? userInfo : userInfo.substring(0, colon);
            password = colon < 0 ? null : userInfo.substring(colon + 1);
        }

        int port = uri.getPort() < 0 ? DEFAULT_PORT : uri.getPort();
        StringBuilder jdbc = new StringBuilder("jdbc:postgresql://")
                .append(uri.getHost())
                .append(':')
                .append(port)
                .append(path);
        if (uri.getRawQuery() != null && !uri.getRawQuery().isEmpty()) {
            jdbc.append('?').append(uri.getRawQuery());
        }
        return new PostgresDsn(jdbc.toString(), username, password);
    }
}
