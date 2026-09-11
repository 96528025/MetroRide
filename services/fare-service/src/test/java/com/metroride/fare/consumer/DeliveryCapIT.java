package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.IntegrationTestSupport;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.Envelope;
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
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

/**
 * A retryable failure that never clears: another session keeps the event row locked for the whole
 * test, so every delivery is cancelled by the transaction timeout. The entry must be reclaimed and
 * retried until its {@code max-deliveries}-th delivery fails, then dead-lettered with
 * {@code reason=max_deliveries_reached} and acknowledged.
 *
 * <p>The cap is shrunk through {@link TestPropertySource}, which gives this class its own Spring
 * context and therefore its own consumer, on the same containers. That consumer reads its own
 * stream so it never competes with the cached context's consumer for the other tests' entries and
 * their per-context metrics. Both configured streams get test-specific names: a context that
 * kept the default completions stream would compete with the shared context's consumer for the
 * completion tests' entries.
 */
@TestPropertySource(properties = {
        "metroride.consumer.streams[0]=events.ride.assignments.delivery-cap-test",
        "metroride.consumer.streams[1]=events.ride.completions.delivery-cap-test",
        "metroride.consumer.reclaim-interval=1s",
        "metroride.consumer.reclaim-min-idle=1s",
        "metroride.consumer.max-deliveries=3"})
class DeliveryCapIT extends IntegrationTestSupport {

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ConsumerProperties consumer;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    void anEntryThatKeepsFailingIsDeadLetteredOnItsThirdDeliveryThenAcknowledged() throws Exception {
        assertThat(consumer.streams()).containsExactly(
                "events.ride.assignments.delivery-cap-test", "events.ride.completions.delivery-cap-test");
        assertThat(consumer.maxDeliveries()).isEqualTo(3);
        DeadLetterStream deadLetters = new DeadLetterStream(redisTemplate, mapper);
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        double exhaustedBefore = deadLetterCount("max_deliveries_reached");
        double poisonBefore = deadLetterCount("poison");
        double reclaimedBefore = reclaimedCount();
        double postgresErrorsBefore = postgresErrorCount();

        try (Connection lockHolder = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            lockHolder.setAutoCommit(false);
            try (PreparedStatement insert = lockHolder.prepareStatement(
                    "insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())")) {
                insert.setString(1, eventId);
                insert.setString(2, assignments());
                insert.setString(3, "ride_assigned");
                insert.executeUpdate();
            }

            RecordId entry = publish(goEnvelope(eventId, rideId));

            // Delivery 1 is the read, deliveries 2 and 3 are reclaims; each attempt costs the 2s
            // transaction timeout and the reclaims wait for 1s of idle time, so about 8s in all.
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(deadLetterCount("max_deliveries_reached")).isEqualTo(exhaustedBefore + 1);
                assertThat(isPending(entry)).isFalse();
            });
            assertThat(reclaimedCount()).isEqualTo(reclaimedBefore + 2);
            assertThat(postgresErrorCount()).isEqualTo(postgresErrorsBefore + 3);
            assertThat(deadLetterCount("poison")).isEqualTo(poisonBefore);
            assertThat(pendingEntries()).isZero();
            assertThat(processedRows(eventId)).isZero();

            JsonNode deadLetter = deadLetters.find(eventId).orElseThrow();
            assertThat(deadLetter.get("type").asText()).isEqualTo(Envelope.TYPE_DEAD_LETTERED);
            assertThat(deadLetter.get("correlation_id").asText()).isEqualTo(rideId);
            JsonNode payload = deadLetter.get("payload");
            assertThat(payload.get("original_event_type").asText()).isEqualTo(Envelope.TYPE_RIDE_ASSIGNED);
            assertThat(payload.get("ride_id").asText()).isEqualTo(rideId);
            assertThat(payload.get("service").asText()).isEqualTo("fare-service");
            assertThat(payload.get("error").asText()).isNotBlank();
            assertThat(deadLetters.count(eventId)).as("one dead letter per exhausted entry").isEqualTo(1);

            lockHolder.rollback();
        }
        // Releasing the lock afterwards changes nothing: the entry is acknowledged, so no
        // delivery happens and the event is never recorded.
        assertThat(processedRows(eventId)).isZero();
    }

    /** The assignments stream: first in {@code metroride.consumer.streams}. */
    private String assignments() {
        return consumer.streams().get(0);
    }

    private RecordId publish(String envelopeJson) {
        return redisTemplate.opsForStream().add(StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, envelopeJson))
                .withStreamKey(assignments()));
    }

    private boolean isPending(RecordId id) {
        return !redisTemplate.opsForStream()
                .pending(assignments(), consumer.group(), Range.closed(id.getValue(), id.getValue()), 1)
                .isEmpty();
    }

    private long pendingEntries() {
        return redisTemplate.opsForStream().pending(assignments(), consumer.group()).getTotalPendingMessages();
    }

    private int processedRows(String eventId) {
        Integer count = jdbc.queryForObject(
                "select count(*) from fare.processed_events where event_id = ?", Integer.class, eventId);
        return count == null ? 0 : count;
    }

    private double deadLetterCount(String reason) {
        return meterRegistry.get("metroride.fare.dead_letters")
                .tag("stream", assignments()).tag("reason", reason).counter().count();
    }

    private double reclaimedCount() {
        return meterRegistry.get("metroride.fare.events.reclaimed").tag("stream", assignments()).counter().count();
    }

    private double postgresErrorCount() {
        return meterRegistry.get("metroride.dependency.errors").tag("dependency", "postgres").counter().count();
    }

    private static String goEnvelope(String eventId, String rideId) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-05T21:12:34.293710969Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"distance_km\":1.8612,\"eta_seconds\":223,\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }
}
