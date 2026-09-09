package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.metroride.fare.IntegrationTestSupport;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.EnvelopeCodec;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * End-to-end check of the consumer path against real PostgreSQL and Redis containers (see
 * {@link IntegrationTestSupport}): Flyway migrates the {@code fare} schema, the consumer group is
 * created, an envelope published the way the Go outbox relay publishes it is recorded once and
 * acknowledged, a second delivery of the same envelope is skipped and acknowledged too, and an
 * entry whose write was cancelled is reclaimed and recorded once the obstacle is gone.
 */
class RideAssignmentConsumerIT extends IntegrationTestSupport {

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

    /**
     * A write that cannot complete must not stall the single consumer thread, and must not lose
     * the entry either. Another transaction holds an uncommitted row with the same event ID, so
     * the consumer's insert waits for that lock; the transaction timeout has to cancel the wait,
     * leave that entry pending, and let the next entry through. Once the lock is released the
     * reclaim pass has to deliver the entry again, within {@code reclaim-interval} plus
     * {@code reclaim-min-idle} of its first delivery, and this time it is recorded and acknowledged.
     * Nobody acknowledges anything by hand.
     */
    @Test
    void aLockWaitingEventIsLeftPendingThenReclaimedOnceTheLockIsReleased() throws Exception {
        String blockedId = UUID.randomUUID().toString();
        String nextId = UUID.randomUUID().toString();
        double reclaimedBefore = reclaimedCount();
        long pendingBefore = pendingEntries();
        RecordId blocked;
        try (Connection lockHolder = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            lockHolder.setAutoCommit(false);
            try (PreparedStatement insert = lockHolder.prepareStatement(
                    "insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())")) {
                insert.setString(1, blockedId);
                insert.setString(2, consumer.stream());
                insert.setString(3, "ride_assigned");
                insert.executeUpdate();
            }
            double postgresErrorsBefore = postgresErrorCount();

            blocked = publish(goEnvelope(blockedId, UUID.randomUUID().toString()));
            RecordId next = publish(goEnvelope(nextId, UUID.randomUUID().toString()));

            // The row commits before the XACK is sent, so the pending count belongs inside the
            // wait: read too early it still includes the entry that is about to be acknowledged.
            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
                assertThat(processedRows(nextId)).isEqualTo(1);
                assertThat(lastDeliveredId()).isEqualTo(next.getValue());
                assertThat(postgresErrorCount()).isEqualTo(postgresErrorsBefore + 1);
                assertThat(pendingEntries()).isEqualTo(pendingBefore + 1);
            });
            assertThat(processedRows(blockedId)).isZero();
            assertThat(reclaimedCount()).isEqualTo(reclaimedBefore);
            lockHolder.rollback();
        }

        // The entry becomes claimable reclaim-min-idle after its first delivery and the next pass
        // runs at most reclaim-interval later; the slack covers container latency, not the design.
        // At least one reclaim: on a slow runner the rollback above can land after the entry was
        // already claimed once more, which costs a second (failing) reclaim, not correctness.
        await().atMost(consumer.reclaimInterval().plus(consumer.reclaimMinIdle()).plusSeconds(3)).untilAsserted(() -> {
            assertThat(processedRows(blockedId)).isEqualTo(1);
            assertThat(reclaimedCount()).isGreaterThanOrEqualTo(reclaimedBefore + 1);
            assertThat(pendingEntries()).isEqualTo(pendingBefore);
        });
        assertThat(isPending(blocked)).isFalse();
        assertThat(lastDeliveredId()).as("XAUTOCLAIM does not move the group's cursor").isNotEqualTo(blocked.getValue());
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
                .contains("metroride_fare_events_reclaimed_total{")
                .contains("metroride_fare_dead_letters_total{")
                .contains("reason=\"poison\"")
                .contains("reason=\"retry_budget_exhausted\"")
                .contains("metroride_fare_dead_letter_publish_failures_total{")
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

    private boolean isPending(RecordId id) {
        return !redisTemplate.opsForStream()
                .pending(consumer.stream(), consumer.group(), Range.closed(id.getValue(), id.getValue()), 1)
                .isEmpty();
    }

    private double postgresErrorCount() {
        return meterRegistry.get("metroride.dependency.errors").tag("dependency", "postgres").counter().count();
    }

    private double reclaimedCount() {
        return meterRegistry.get("metroride.fare.events.reclaimed").tag("stream", consumer.stream()).counter().count();
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
