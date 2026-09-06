package com.metroride.fare.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

/**
 * Maps the MetroRide environment variables (the same names and defaults as
 * {@code shared/pkg/config/config.go}) onto the Spring properties that need them:
 *
 * <pre>
 *   FARE_SERVICE_ADDR        ->  server.address, server.port
 *   POSTGRES_DSN             ->  spring.datasource.url / username / password
 *   REDIS_ADDR               ->  spring.data.redis.host / port
 *   CONSUMER_GROUP           ->  metroride.consumer.group
 *   CONSUMER_NAME            ->  metroride.consumer.name
 *   SHUTDOWN_TIMEOUT_SECONDS ->  spring.lifecycle.timeout-per-shutdown-phase
 * </pre>
 *
 * As in {@code config.getenv}, a variable that is set but empty counts as unset and takes the
 * default.
 *
 * <p>The derived properties are added with the lowest precedence, so anything set explicitly
 * (command line, {@code SPRING_DATASOURCE_URL}, test properties, Testcontainers
 * {@code @ServiceConnection}) still wins. {@code application.yml} therefore must not define
 * these keys, or it would shadow the environment.
 */
public final class MetroRideEnvironmentPostProcessor implements EnvironmentPostProcessor {

    static final String PROPERTY_SOURCE_NAME = "metroride-environment";

    static final String DEFAULT_LISTEN_ADDRESS = ":8087";
    static final String DEFAULT_POSTGRES_DSN =
            "postgres://metroride:metroride@localhost:5432/metroride?sslmode=disable";
    static final String DEFAULT_REDIS_ADDRESS = "localhost:6379";
    static final String DEFAULT_CONSUMER_GROUP = "fare-service";
    static final String DEFAULT_CONSUMER_NAME = "fare-service-1";
    static final String DEFAULT_SHUTDOWN_TIMEOUT_SECONDS = "10";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        Map<String, Object> derived = new LinkedHashMap<>();

        ListenAddress listen = ListenAddress.parse(getenv(environment, "FARE_SERVICE_ADDR", DEFAULT_LISTEN_ADDRESS));
        derived.put("server.port", listen.port());
        listen.bindHost().ifPresent(host -> derived.put("server.address", host));

        PostgresDsn postgres = PostgresDsn.parse(getenv(environment, "POSTGRES_DSN", DEFAULT_POSTGRES_DSN));
        derived.put("spring.datasource.url", postgres.jdbcUrl());
        if (postgres.username() != null) {
            derived.put("spring.datasource.username", postgres.username());
        }
        if (postgres.password() != null) {
            derived.put("spring.datasource.password", postgres.password());
        }

        RedisAddress redis = RedisAddress.parse(getenv(environment, "REDIS_ADDR", DEFAULT_REDIS_ADDRESS));
        derived.put("spring.data.redis.host", redis.host());
        derived.put("spring.data.redis.port", redis.port());

        derived.put("metroride.consumer.group", getenv(environment, "CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP));
        derived.put("metroride.consumer.name", getenv(environment, "CONSUMER_NAME", DEFAULT_CONSUMER_NAME));
        derived.put("spring.lifecycle.timeout-per-shutdown-phase",
                getenv(environment, "SHUTDOWN_TIMEOUT_SECONDS", DEFAULT_SHUTDOWN_TIMEOUT_SECONDS) + "s");

        environment.getPropertySources().addLast(new MapPropertySource(PROPERTY_SOURCE_NAME, derived));
    }

    /** Mirrors {@code config.getenv}: an unset or empty variable yields the fallback. */
    static String getenv(ConfigurableEnvironment environment, String name, String fallback) {
        String value = environment.getProperty(name);
        return value == null || value.isBlank() ? fallback : value.trim();
    }
}
