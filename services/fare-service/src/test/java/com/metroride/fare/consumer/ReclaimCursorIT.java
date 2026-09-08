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
import java.util.List;
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
 * Fairness of the reclaim pass. Three entries fail their first delivery on lock waits and stay
 * pending; the first two stay locked, the third is released. With {@code batch-size} 2 a pass that
 * always restarted at the head of the pending list would claim the same two failing entries every
 * time and never reach the third. Because each pass continues from the cursor the previous one
 * returned, the third entry is claimed in its turn and recorded while the first two are still
 * pending; once they are released they are recorded too.
 *
 * <p>Own stream and context (see {@link DeliveryCapIT} for why); the cap is raised so the two
 * locked entries are not dead-lettered during the test.
 */
@TestPropertySource(properties = {
        "metroride.consumer.stream=events.ride.assignments.reclaim-cursor-test",
        "metroride.consumer.batch-size=2",
        "metroride.consumer.reclaim-interval=1s",
        "metroride.consumer.reclaim-min-idle=1s",
        "metroride.consumer.max-deliveries=100"})
class ReclaimCursorIT extends IntegrationTestSupport {

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ConsumerProperties consumer;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    RideAssignmentConsumer streamConsumer;

    @Test
    void anEntryBehindPersistentlyFailingOnesIsStillReclaimed() throws Exception {
        assertThat(consumer.batchSize()).isEqualTo(2);
        List<String> eventIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Connection[] lockHolders = new Connection[3];
        try {
            for (int i = 0; i < 3; i++) {
                lockHolders[i] = DriverManager.getConnection(
                        POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                lockHolders[i].setAutoCommit(false);
                try (PreparedStatement insert = lockHolders[i].prepareStatement(
                        "insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())")) {
                    insert.setString(1, eventIds.get(i));
                    insert.setString(2, consumer.stream());
                    insert.setString(3, "ride_assigned");
                    insert.executeUpdate();
                }
            }
            double postgresErrorsBefore = postgresErrorCount();

            RecordId first = publish(goEnvelope(eventIds.get(0)));
            RecordId second = publish(goEnvelope(eventIds.get(1)));
            RecordId third = publish(goEnvelope(eventIds.get(2)));

            // XREADGROUP returns as soon as one entry is available, so the three do not arrive as
            // one batch and "every entry failed once" cannot be read off an aggregate counter. What
            // proves the setup is the consumer's own cursor: a pass that claimed the first two and
            // stopped returns the third's ID as the place to continue, which means the third was
            // already in the pending list behind them, i.e. delivered and failed (the consumer is one
            // thread, so no pass runs while an entry is still being handled).
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(streamConsumer.reclaimCursor(consumer.stream())).isEqualTo(third.getValue());
                assertThat(processedRows(eventIds.get(2))).isZero();
            });
            assertThat(isPending(first)).isTrue();
            assertThat(isPending(second)).isTrue();
            assertThat(isPending(third)).isTrue();
            assertThat(postgresErrorCount()).isGreaterThanOrEqualTo(postgresErrorsBefore + 3);

            // Release only the third. The first two keep failing and keep filling every pass
            // that starts at the head; the third is reached because the next pass starts at it.
            lockHolders[2].rollback();
            lockHolders[2].close();
            lockHolders[2] = null;

            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(processedRows(eventIds.get(2))).isEqualTo(1);
                assertThat(isPending(third)).isFalse();
            });
            assertThat(isPending(first)).isTrue();
            assertThat(isPending(second)).isTrue();
            assertThat(processedRows(eventIds.get(0))).isZero();
            assertThat(processedRows(eventIds.get(1))).isZero();
            assertThat(deadLetterCount("retry_budget_exhausted")).isZero();

            lockHolders[0].rollback();
            lockHolders[1].rollback();
        } finally {
            for (Connection holder : lockHolders) {
                if (holder != null) {
                    holder.close();
                }
            }
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(processedRows(eventIds.get(0))).isEqualTo(1);
            assertThat(processedRows(eventIds.get(1))).isEqualTo(1);
            assertThat(pendingEntries()).isZero();
        });
        // With nothing left pending a full scan completes and the cursor rests at the head again.
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                assertThat(streamConsumer.reclaimCursor(consumer.stream())).isEqualTo(RideAssignmentConsumer.RECLAIM_FROM_START));
    }

    private RecordId publish(String envelopeJson) {
        return redisTemplate.opsForStream().add(StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, envelopeJson))
                .withStreamKey(consumer.stream()));
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

    private double postgresErrorCount() {
        return meterRegistry.get("metroride.dependency.errors").tag("dependency", "postgres").counter().count();
    }

    private static String goEnvelope(String eventId) {
        String rideId = UUID.randomUUID().toString();
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-08T12:00:00Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"distance_km\":1.8612,\"eta_seconds\":223,\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }
}
