package com.metroride.fare;

import com.redis.testcontainers.RedisContainer;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared setup for the Testcontainers-backed integration tests: one PostgreSQL and one Redis
 * container (the same images docker-compose.yml uses) started once per JVM and reaped by
 * Testcontainers when the JVM exits.
 *
 * <p>The containers are deliberately not per-class {@code @Container} fields. Spring caches the
 * test application context across test classes that share a configuration, and that cached context
 * keeps the connection details of whichever containers were running when it was created; per-class
 * containers would be stopped after the first class while the second class still used the cached
 * context. With singleton containers every class sees the same live containers and the same context.
 * Tests therefore share the consumer group: use fresh IDs, assert on deltas, and acknowledge any
 * entry a test deliberately leaves pending.
 *
 * <p>{@code @AutoConfigureObservability} is required by every {@code @SpringBootTest} in this
 * service: Spring Boot switches metrics export off in tests, and {@code MetricsController} needs the
 * Prometheus registry that export provides.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
public abstract class IntegrationTestSupport {

    @ServiceConnection
    protected static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @ServiceConnection
    protected static final RedisContainer REDIS = new RedisContainer(DockerImageName.parse("redis:7-alpine"));

    static {
        POSTGRES.start();
        REDIS.start();
    }
}
