package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.IntegrationTestSupport;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.EnvelopeCodec;
import io.lettuce.core.Consumer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XClaimArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;

/**
 * The delivery-count lookup must not miss a claimed entry. After a pass claims entries, one
 * {@code XPENDING} over the range from the first to the last claimed ID fetches their counts, and
 * that range also contains this consumer's entries that were skipped because they were not idle
 * long enough. If the lookup's row limit were smaller than the number of such entries, the last
 * claimed entry would get no count for that pass; while the pending list keeps that shape, its
 * retryable failures never count towards {@code max-deliveries}.
 *
 * <p>Seven entries fail their first delivery on lock waits and stay pending. The test then keeps
 * the five middle ones fresh ({@code XCLAIM ... IDLE 0 JUSTID}, which resets idle time without
 * counting a delivery) and the first and last one old ({@code IDLE 60000}), so every reclaim pass
 * claims exactly the first and the last with the five fresh ones between them. With
 * {@code batch-size} 2 a lookup limited to a handful of rows returns only the first claimed entry
 * and some of the fresh ones; the last claimed entry would be reported as {@code -1} on every pass
 * and not be dead-lettered while the layout lasts. The correct limit returns all seven, and both
 * claimed entries are dead-lettered on their third delivery.
 *
 * <p>{@code reclaim-min-idle} is long so that no reclaim happens on its own while the seven first
 * deliveries are still failing; only the test's {@code XCLAIM} makes entries claimable.
 */
@TestPropertySource(properties = {
        "metroride.consumer.stream=events.ride.assignments.delivery-count-test",
        "metroride.consumer.batch-size=2",
        "metroride.consumer.reclaim-interval=1s",
        "metroride.consumer.reclaim-min-idle=30s",
        "metroride.consumer.max-deliveries=3"})
class DeliveryCountLookupIT extends IntegrationTestSupport {

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    LettuceConnectionFactory connectionFactory;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ConsumerProperties consumer;

    @Autowired
    MeterRegistry meterRegistry;

    @Test
    void aClaimedEntryBehindManyFreshOnesStillGetsItsDeliveryCount() throws Exception {
        DeadLetterStream deadLetters = new DeadLetterStream(redisTemplate, mapper);
        List<String> eventIds = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            eventIds.add(UUID.randomUUID().toString());
        }
        Connection[] lockHolders = new Connection[7];
        List<RecordId> entries = new ArrayList<>();
        double exhaustedBefore = deadLetterCount("max_deliveries_reached");
        double postgresErrorsBefore = postgresErrorCount();
        AtomicBoolean refreshing = new AtomicBoolean(false);
        Thread refresher = null;
        try (StatefulRedisConnection<String, String> redis = ((RedisClient) connectionFactory.getRequiredNativeClient()).connect()) {
            for (int i = 0; i < 7; i++) {
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
            for (String eventId : eventIds) {
                entries.add(publish(goEnvelope(eventId)));
            }
            String first = entries.get(0).getValue();
            String last = entries.get(6).getValue();
            String[] middle = entries.subList(1, 6).stream().map(RecordId::getValue).toArray(String[]::new);

            // Every first delivery fails; with min-idle 30s nothing is reclaimed on its own yet.
            await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
                assertThat(postgresErrorCount()).isEqualTo(postgresErrorsBefore + 7);
                assertThat(pendingEntries()).isEqualTo(7);
            });

            RedisCommands<String, String> commands = redis.sync();
            Consumer<String> owner = Consumer.from(consumer.group(), consumer.name());
            refreshing.set(true);
            refresher = new Thread(() -> {
                while (refreshing.get()) {
                    // JUSTID: reset (or set) the idle time without counting a delivery.
                    commands.xclaim(consumer.stream(), owner, XClaimArgs.Builder.justid().minIdleTime(0).idle(0), middle);
                    commands.xclaim(consumer.stream(), owner, XClaimArgs.Builder.justid().minIdleTime(0).idle(60_000), first, last);
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }, "pel-refresher");
            refresher.start();

            // Each pass claims first and last together (deliveries 2 then 3); on the third both are
            // dead-lettered. The five fresh entries are never claimed and never counted.
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(deadLetterCount("max_deliveries_reached")).isEqualTo(exhaustedBefore + 2);
                assertThat(isPending(entries.get(0))).isFalse();
                assertThat(isPending(entries.get(6))).isFalse();
            });
            assertThat(deadLetters.find(eventIds.get(0))).isPresent();
            assertThat(deadLetters.find(eventIds.get(6))).isPresent();
            for (int i = 1; i < 6; i++) {
                assertThat(isPending(entries.get(i))).as("fresh entry " + i + " was never claimed").isTrue();
                assertThat(processedRows(eventIds.get(i))).isZero();
            }

            refreshing.set(false);
            refresher.join(Duration.ofSeconds(5).toMillis());
            for (int i = 1; i < 6; i++) {
                lockHolders[i].rollback();
            }
            // Make the five claimable now instead of after 30s of idle time.
            commands.xclaim(consumer.stream(), owner, XClaimArgs.Builder.justid().minIdleTime(0).idle(60_000), middle);
        } finally {
            refreshing.set(false);
            for (Connection holder : lockHolders) {
                if (holder != null) {
                    holder.close();
                }
            }
        }

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            for (int i = 1; i < 6; i++) {
                assertThat(processedRows(eventIds.get(i))).isEqualTo(1);
            }
            assertThat(pendingEntries()).isZero();
        });
        assertThat(deadLetterCount("max_deliveries_reached")).isEqualTo(exhaustedBefore + 2);
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
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-09T12:00:00Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"distance_km\":1.8612,\"eta_seconds\":223,\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }
}
