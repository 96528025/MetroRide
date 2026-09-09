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
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

/**
 * An entry's age plays no part in when it is given up on. The entry here carries a stream ID from
 * 2001, as if it had waited in the stream for a quarter of a century while this service was down;
 * its first delivery fails on a lock wait and it must be left pending, reclaimed, and recorded once
 * the lock is gone, exactly like a fresh entry. Dead-lettering an old entry on its first failure
 * would throw away precisely the backlog a restart exists to work through.
 *
 * <p>Own stream and context (see {@link DeliveryCapIT} for why). The explicit ID only works as
 * the first entry of the stream, which is why this class has a single test.
 */
@TestPropertySource(properties = {
        "metroride.consumer.stream=events.ride.assignments.old-entry-test",
        "metroride.consumer.reclaim-interval=1s",
        "metroride.consumer.reclaim-min-idle=1s"})
class OldEntryRecoveryIT extends IntegrationTestSupport {

    /** 2001-09-09T01:46:40Z; any auto-generated ID is larger, so the entry is the oldest possible. */
    private static final String ANCIENT_ID = "1000000000000-0";

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ConsumerProperties consumer;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    void anAncientEntryThatFailsOnceIsRetriedNotDeadLettered() throws Exception {
        assertThat(consumer.maxDeliveries()).as("default cap from application.yml").isEqualTo(25);
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        double deadLettersBefore = deadLetterCount("retry_budget_exhausted") + deadLetterCount("poison");
        double reclaimedBefore = reclaimedCount();

        try (Connection lockHolder = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            lockHolder.setAutoCommit(false);
            try (PreparedStatement insert = lockHolder.prepareStatement(
                    "insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())")) {
                insert.setString(1, eventId);
                insert.setString(2, consumer.stream());
                insert.setString(3, "ride_assigned");
                insert.executeUpdate();
            }
            double postgresErrorsBefore = postgresErrorCount();

            RecordId entry = redisTemplate.opsForStream().add(
                    StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, goEnvelope(eventId, rideId)))
                            .withStreamKey(consumer.stream())
                            .withId(RecordId.of(ANCIENT_ID)));
            assertThat(entry.getValue()).isEqualTo(ANCIENT_ID);

            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
                assertThat(postgresErrorCount()).isEqualTo(postgresErrorsBefore + 1);
                assertThat(isPending(entry)).isTrue();
            });
            assertThat(deadLetterCount("retry_budget_exhausted") + deadLetterCount("poison")).isEqualTo(deadLettersBefore);
            assertThat(processedRows(eventId)).isZero();
            lockHolder.rollback();
        }

        await().atMost(consumer.reclaimInterval().plus(consumer.reclaimMinIdle()).plusSeconds(3)).untilAsserted(() -> {
            assertThat(processedRows(eventId)).isEqualTo(1);
            assertThat(reclaimedCount()).isGreaterThanOrEqualTo(reclaimedBefore + 1);
            assertThat(pendingEntries()).isZero();
        });
        assertThat(deadLetterCount("retry_budget_exhausted") + deadLetterCount("poison")).isEqualTo(deadLettersBefore);
    }

    private boolean isPending(RecordId id) {
        return !redisTemplate.opsForStream()
                .pending(consumer.stream(), consumer.group(), Range.closed(id.getValue(), id.getValue()), 1)
                .isEmpty();
    }

    private long pendingEntries() {
        return redisTemplate.opsForStream().pending(consumer.stream(), consumer.group()).getTotalPendingMessages();
    }

    private int processedRows(String eventId) {
        Integer count = jdbc.queryForObject(
                "select count(*) from fare.processed_events where event_id = ?", Integer.class, eventId);
        return count == null ? 0 : count;
    }

    private double deadLetterCount(String reason) {
        return meterRegistry.get("metroride.fare.dead_letters")
                .tag("stream", consumer.stream()).tag("reason", reason).counter().count();
    }

    private double reclaimedCount() {
        return meterRegistry.get("metroride.fare.events.reclaimed").tag("stream", consumer.stream()).counter().count();
    }

    private double postgresErrorCount() {
        return meterRegistry.get("metroride.dependency.errors").tag("dependency", "postgres").counter().count();
    }

    private static String goEnvelope(String eventId, String rideId) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2001-09-09T01:46:40Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"distance_km\":1.8612,\"eta_seconds\":223,\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }
}
