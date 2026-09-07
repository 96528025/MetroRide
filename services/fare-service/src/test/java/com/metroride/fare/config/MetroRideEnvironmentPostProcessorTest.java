package com.metroride.fare.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;

class MetroRideEnvironmentPostProcessorTest {

    private final MetroRideEnvironmentPostProcessor processor = new MetroRideEnvironmentPostProcessor();

    @Test
    void derivesSpringPropertiesFromTheComposeEnvironment() {
        StandardEnvironment environment = isolatedEnvironment(Map.of(
                "FARE_SERVICE_ADDR", ":8087",
                "POSTGRES_DSN", "postgres://metroride:metroride@postgres:5432/metroride?sslmode=disable",
                "REDIS_ADDR", "redis:6379",
                "CONSUMER_GROUP", "fare-service",
                "CONSUMER_NAME", "fare-service-2",
                "SHUTDOWN_TIMEOUT_SECONDS", "25"));

        processor.postProcessEnvironment(environment, null);

        assertThat(environment.getProperty("server.port", Integer.class)).isEqualTo(8087);
        assertThat(environment.containsProperty("server.address")).isFalse();
        assertThat(environment.getProperty("spring.datasource.url"))
                .isEqualTo("jdbc:postgresql://postgres:5432/metroride?sslmode=disable");
        assertThat(environment.getProperty("spring.datasource.username")).isEqualTo("metroride");
        assertThat(environment.getProperty("spring.datasource.password")).isEqualTo("metroride");
        assertThat(environment.getProperty("spring.data.redis.host")).isEqualTo("redis");
        assertThat(environment.getProperty("spring.data.redis.port", Integer.class)).isEqualTo(6379);
        assertThat(environment.getProperty("metroride.consumer.group")).isEqualTo("fare-service");
        assertThat(environment.getProperty("metroride.consumer.name")).isEqualTo("fare-service-2");
        assertThat(environment.getProperty("spring.lifecycle.timeout-per-shutdown-phase")).isEqualTo("25s");
    }

    @Test
    void fallsBackToTheGoDefaultsWhenNothingIsSet() {
        StandardEnvironment environment = isolatedEnvironment(Map.of());

        processor.postProcessEnvironment(environment, null);

        assertThat(environment.getProperty("server.port", Integer.class)).isEqualTo(8087);
        assertThat(environment.containsProperty("server.address")).isFalse();
        assertThat(environment.getProperty("spring.datasource.url"))
                .isEqualTo("jdbc:postgresql://localhost:5432/metroride?sslmode=disable");
        assertThat(environment.getProperty("spring.datasource.username")).isEqualTo("metroride");
        assertThat(environment.getProperty("spring.data.redis.host")).isEqualTo("localhost");
        assertThat(environment.getProperty("spring.data.redis.port", Integer.class)).isEqualTo(6379);
        assertThat(environment.getProperty("metroride.consumer.group")).isEqualTo("fare-service");
        assertThat(environment.getProperty("metroride.consumer.name")).isEqualTo("fare-service-1");
        assertThat(environment.getProperty("spring.lifecycle.timeout-per-shutdown-phase")).isEqualTo("10s");
    }

    @Test
    void emptyVariablesCountAsUnsetLikeConfigGetenv() {
        Map<String, Object> empty = new HashMap<>();
        empty.put("FARE_SERVICE_ADDR", "");
        empty.put("POSTGRES_DSN", "  ");
        empty.put("REDIS_ADDR", "");
        empty.put("CONSUMER_GROUP", "");
        empty.put("SHUTDOWN_TIMEOUT_SECONDS", "");
        StandardEnvironment environment = isolatedEnvironment(empty);

        processor.postProcessEnvironment(environment, null);

        assertThat(environment.getProperty("server.port", Integer.class)).isEqualTo(8087);
        assertThat(environment.getProperty("spring.datasource.url"))
                .isEqualTo("jdbc:postgresql://localhost:5432/metroride?sslmode=disable");
        assertThat(environment.getProperty("spring.data.redis.host")).isEqualTo("localhost");
        assertThat(environment.getProperty("metroride.consumer.group")).isEqualTo("fare-service");
        assertThat(environment.getProperty("spring.lifecycle.timeout-per-shutdown-phase")).isEqualTo("10s");
    }

    @Test
    void explicitSpringPropertiesStillWin() {
        StandardEnvironment environment = isolatedEnvironment(Map.of(
                "FARE_SERVICE_ADDR", "127.0.0.1:8087",
                "server.port", "0"));

        processor.postProcessEnvironment(environment, null);

        assertThat(environment.getProperty("server.port", Integer.class)).isZero();
        assertThat(environment.getProperty("server.address")).isEqualTo("127.0.0.1");
    }

    /**
     * A StandardEnvironment without the real process environment and JVM system properties, so
     * only the given values are visible and the test cannot be influenced by the developer's shell.
     */
    private static StandardEnvironment isolatedEnvironment(Map<String, Object> values) {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().addFirst(new MapPropertySource("test", values));
        return environment;
    }
}
