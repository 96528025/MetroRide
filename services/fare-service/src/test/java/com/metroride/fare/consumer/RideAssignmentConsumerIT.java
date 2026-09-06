package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.EnvelopeCodec;
import com.redis.testcontainers.RedisContainer;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end check of the skeleton against real PostgreSQL and Redis containers (the same images
 * docker-compose.yml uses): Flyway migrates the {@code fare} schema, the consumer group is created,
 * an envelope published the way the Go outbox relay publishes it is recorded once and acknowledged,
 * and a second delivery of the same envelope is skipped and acknowledged too.
 *
 * <p>{@code @AutoConfigureObservability} is required by every {@code @SpringBootTest} in this
 * service: Spring Boot switches metrics export off in tests, and {@code MetricsController} needs the
 * Prometheus registry that export provides.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
@Testcontainers
class RideAssignmentConsumerIT {

    @Container
    @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    @ServiceConnection
    static final RedisContainer redis = new RedisContainer(DockerImageName.parse("redis:7-alpine"));

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ConsumerProperties consumer;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    TestRestTemplate http;

    @Test
    void recordsAnEnvelopeOnceAndAcknowledgesBothDeliveries() {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        String envelope = goEnvelope(eventId, rideId);

        RecordId first = publish(envelope);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(processedRows(eventId)).isEqualTo(1);
            assertThat(lastDeliveredId()).isEqualTo(first.getValue());
            assertThat(pendingEntries()).isZero();
        });
        Map<String, Object> row = jdbc.queryForMap(
                "select stream, event_type, processed_at from fare.processed_events where event_id = ?", eventId);
        assertThat(row.get("stream")).isEqualTo(consumer.stream());
        assertThat(row.get("event_type")).isEqualTo("ride_assigned");
        assertThat(row.get("processed_at")).isNotNull();

        double duplicatesBefore = duplicateCount();
        RecordId second = publish(envelope);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(lastDeliveredId()).isEqualTo(second.getValue());
            assertThat(pendingEntries()).isZero();
            assertThat(duplicateCount()).isEqualTo(duplicatesBefore + 1);
        });
        assertThat(processedRows(eventId)).isEqualTo(1);
    }

    @Test
    void exposesTheGoHealthAndMetricsContract() {
        ResponseEntity<String> healthz = http.getForEntity("/healthz", String.class);
        assertThat(healthz.getStatusCode().value()).isEqualTo(200);
        assertThat(healthz.getBody()).isEqualTo("{\"status\":\"ok\"}");

        ResponseEntity<String> readyz = http.getForEntity("/readyz", String.class);
        assertThat(readyz.getStatusCode().value()).isEqualTo(200);
        assertThat(readyz.getBody()).isEqualTo("{\"status\":\"ready\"}");

        ResponseEntity<String> metrics = http.getForEntity("/metrics", String.class);
        assertThat(metrics.getStatusCode().value()).isEqualTo(200);
        assertThat(metrics.getHeaders().getContentType().toString()).startsWith("text/plain");
        assertThat(metrics.getBody())
                .contains("metroride_fare_events_processed_total")
                .contains("metroride_stream_consume_errors_total")
                .contains("metroride_dependency_errors_total");
    }

    private RecordId publish(String envelopeJson) {
        return redisTemplate.opsForStream().add(StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, envelopeJson))
                .withStreamKey(consumer.stream()));
    }

    private int processedRows(String eventId) {
        Integer count = jdbc.queryForObject(
                "select count(*) from fare.processed_events where event_id = ?", Integer.class, eventId);
        return count == null ? 0 : count;
    }

    private long pendingEntries() {
        return redisTemplate.opsForStream().pending(consumer.stream(), consumer.group()).getTotalPendingMessages();
    }

    private String lastDeliveredId() {
        return redisTemplate.opsForStream().groups(consumer.stream()).stream()
                .filter(group -> group.groupName().equals(consumer.group()))
                .findFirst()
                .orElseThrow()
                .lastDeliveredId();
    }

    private double duplicateCount() {
        return meterRegistry.get("metroride.fare.events.processed").tag("outcome", "duplicate").counter().count();
    }

    /** Same shape as {@code events.Publish} writes: one field named {@code event} holding the envelope JSON. */
    private static String goEnvelope(String eventId, String rideId) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-05T21:12:34.293710969Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"distance_km\":1.8612,\"eta_seconds\":223,\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }
}
