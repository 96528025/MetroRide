package com.metroride.fare.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.IntegrationTestSupport;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.processing.ProcessedEventRecorder;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Publication of {@code fare_settled} through {@code fare.event_outbox}, in three layers that each
 * prove one thing:
 *
 * <ul>
 *   <li>the business transaction: with Redis paused, a settlement still commits its journal
 *       entries and its outbox row together, and nothing of a refused settlement is left behind;</li>
 *   <li>the relay: committed rows are published, failures are recorded with the backoff, and
 *       after Redis is back every queued row is published;</li>
 *   <li>the whole chain: {@code ride_assigned} and {@code ride_completed} in through the streams,
 *       {@code fare_settled} out on {@code events.ride.fares} with the ledger's figures.</li>
 * </ul>
 *
 * <p>Pausing the Redis container freezes every connection to it, the consumer's and the readiness
 * check's included; both recover through Lettuce's reconnect once it is unpaused. The tests that
 * pause it assert on deltas and use their own rides. After a fault the stream may carry the same
 * envelope twice (a timed-out {@code XADD} may still have been executed, and the row is then
 * published again): those assertions require at least one entry, all with the same ID and payload,
 * never exactly one.
 */
class OutboxIT extends IntegrationTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    ObjectMapper mapper;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ConsumerProperties consumer;

    @Autowired
    ProcessedEventRecorder recorder;

    @Autowired
    EnvelopeCodec codec;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    TestRestTemplate http;

    // ---- whole chain -----------------------------------------------------------------------

    @Test
    void aSettledRideIsAnnouncedOnceOnTheFaresStreamWithTheLedgerFigures() {
        String rideId = UUID.randomUUID().toString();
        String completionId = UUID.randomUUID().toString();
        double publishedBefore = published();
        Instant before = Instant.now();

        publish(assignmentsStream(), assignment(UUID.randomUUID().toString(), rideId, 1.8612, 223));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(rideId, "quote_hold")).isEqualTo(1));
        publish(completionsStream(), completion(completionId, rideId));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(outboxRows(rideId)).hasSize(1);
            assertThat(outboxRows(rideId).get(0).get("published_at")).isNotNull();
        });
        Map<String, Object> row = outboxRows(rideId).get(0);
        assertThat(row.get("stream")).isEqualTo(Envelope.STREAM_RIDE_FARES);
        assertThat(row.get("event_type")).isEqualTo(Envelope.TYPE_FARE_SETTLED);
        assertThat(row.get("source_service")).isEqualTo("fare-service");
        assertThat(row.get("aggregate_id")).isEqualTo(rideId);
        assertThat(((Number) row.get("publish_attempts")).intValue()).isEqualTo(1);
        assertThat(row.get("last_error")).isNull();

        List<JsonNode> entries = faresFor(rideId);
        assertThat(entries).hasSize(1);
        JsonNode envelope = entries.get(0);
        assertThat(envelope.get("id").asText()).isEqualTo(row.get("id"));
        assertThat(envelope.get("type").asText()).isEqualTo("fare_settled");
        assertThat(envelope.get("source").asText()).isEqualTo("fare-service");
        assertThat(envelope.get("occurred_at").isTextual()).isTrue();
        JsonNode payload = envelope.get("payload");
        assertThat(payload.get("ride_id").asText()).isEqualTo(rideId);
        assertThat(payload.get("rider_id").asText()).isEqualTo("rider-42");
        assertThat(payload.get("driver_id").asText()).isEqualTo("driver-2");
        assertThat(payload.get("settlement_event_id").asText()).isEqualTo(completionId);
        // 2.50 + 1.20 * 1.8612 + 0.30 * 223 / 60 = 5.85; 5.85 * 0.80 = 4.68; remainder 1.17
        assertThat(payload.get("quote").asText()).isEqualTo("5.85");
        assertThat(payload.get("driver_amount").asText()).isEqualTo("4.68");
        assertThat(payload.get("platform_amount").asText()).isEqualTo("1.17");
        assertThat(payload.get("driver_share").asText()).isEqualTo("0.80");
        assertThat(Instant.parse(payload.get("settled_at").asText())).isBetween(before, Instant.now());
        // The counter moves after the commit the row assertion saw, so it is awaited, not read once.
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(published()).isEqualTo(publishedBefore + 1));

        // A redelivery of the completion and a second, distinct completion add no row.
        publish(completionsStream(), completion(completionId, rideId));
        publish(completionsStream(), completion(UUID.randomUUID().toString(), rideId));
        await().atMost(TIMEOUT).untilAsserted(() ->
                assertThat(deadLetterCount("already_settled")).isGreaterThanOrEqualTo(1));
        assertThat(outboxRows(rideId)).hasSize(1);
        assertThat(faresFor(rideId)).hasSize(1);
    }

    /** A settlement that rolls back leaves no outbox row; the reclaimed delivery writes exactly one. */
    @Test
    void aRolledBackSettlementLeavesNoOutboxRowUntilItSucceeds() throws Exception {
        String rideId = UUID.randomUUID().toString();
        publish(assignmentsStream(), assignment(UUID.randomUUID().toString(), rideId, 1.0, 60));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(rideId, "quote_hold")).isEqualTo(1));
        double postgresErrorsBefore = postgresErrorCount();

        try (Connection lockHolder = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            lockHolder.setAutoCommit(false);
            try (Statement lock = lockHolder.createStatement()) {
                lock.execute("lock table fare.postings in access exclusive mode");
            }
            publish(completionsStream(), completion(UUID.randomUUID().toString(), rideId));
            await().atMost(Duration.ofSeconds(8)).untilAsserted(() ->
                    assertThat(postgresErrorCount()).isEqualTo(postgresErrorsBefore + 1));
            assertThat(journalRows(rideId, "settlement")).isZero();
            assertThat(outboxRows(rideId)).isEmpty();
            lockHolder.rollback();
        }

        await().atMost(consumer.reclaimInterval().plus(consumer.reclaimMinIdle()).plusSeconds(5)).untilAsserted(() -> {
            assertThat(journalRows(rideId, "settlement")).isEqualTo(1);
            assertThat(outboxRows(rideId)).hasSize(1);
            assertThat(outboxRows(rideId).get(0).get("published_at")).isNotNull();
        });
        assertThat(faresFor(rideId)).hasSize(1);
    }

    @Test
    void thirtySettlementsAreAllPublishedWithDistinctEnvelopeIds() {
        List<String> rideIds = new java.util.ArrayList<>();
        for (int i = 0; i < 30; i++) {
            String rideId = UUID.randomUUID().toString();
            rideIds.add(rideId);
            recorder.record(assignmentsStream(), decode(assignment(UUID.randomUUID().toString(), rideId, 2.0, 120)));
            recorder.record(completionsStream(), decode(completion(UUID.randomUUID().toString(), rideId)));
        }

        await().atMost(TIMEOUT).untilAsserted(() -> {
            for (String rideId : rideIds) {
                assertThat(outboxRows(rideId)).hasSize(1);
                assertThat(outboxRows(rideId).get(0).get("published_at")).isNotNull();
            }
        });
        Set<String> ids = rideIds.stream()
                .flatMap(rideId -> faresFor(rideId).stream())
                .map(envelope -> envelope.get("id").asText())
                .collect(Collectors.toSet());
        assertThat(ids).hasSize(30);
    }

    // ---- business transaction and relay under a Redis fault --------------------------------

    /**
     * With Redis paused, settling still commits the journal entries and the outbox row together
     * ({@code record()} never talks to Redis); the relay records each failed attempt with the
     * backoff; once Redis is back the row is published and marked. Three rides queue up so the
     * recovery publishes a batch, not a single row.
     */
    @Test
    void settlementsCommitWhileRedisIsAwayAndAreLaterPublished() throws Exception {
        List<String> rideIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString());
        for (String rideId : rideIds) {
            recorder.record(assignmentsStream(), decode(assignment(UUID.randomUUID().toString(), rideId, 1.0, 60)));
        }
        double failuresBefore = publishFailures();
        double publishedBefore = published();

        REDIS.getDockerClient().pauseContainerCmd(REDIS.getContainerId()).exec();
        try {
            for (String rideId : rideIds) {
                recorder.record(completionsStream(), decode(completion(UUID.randomUUID().toString(), rideId)));
            }
            for (String rideId : rideIds) {
                assertThat(journalRows(rideId, "settlement")).as("the settlement does not need Redis").isEqualTo(1);
                assertThat(outboxRows(rideId)).hasSize(1);
                assertThat(outboxRows(rideId).get(0).get("published_at")).isNull();
            }

            // The relay's XADD times out (2s), the failure is recorded with the first backoff.
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
                assertThat(publishFailures()).isGreaterThanOrEqualTo(failuresBefore + 3);
                for (String rideId : rideIds) {
                    Map<String, Object> row = outboxRows(rideId).get(0);
                    assertThat(((Number) row.get("publish_attempts")).intValue()).isGreaterThanOrEqualTo(1);
                    assertThat(row.get("last_error")).isNotNull();
                    assertThat(row.get("published_at")).isNull();
                    Instant nextAttempt = ((Timestamp) row.get("next_attempt_at")).toInstant();
                    Instant createdAt = ((Timestamp) row.get("created_at")).toInstant();
                    assertThat(nextAttempt).as("the backoff pushed the retry past the insert").isAfter(createdAt);
                }
            });
            assertThat(unpublishedGauge()).isGreaterThanOrEqualTo(3);
            ResponseEntity<String> readyz = http.getForEntity("/readyz", String.class);
            assertThat(readyz.getStatusCode().value()).as("Redis is a readiness dependency; the relay is not").isEqualTo(503);
            assertThat(readyz.getBody()).contains("\"redis\"");
        } finally {
            REDIS.getDockerClient().unpauseContainerCmd(REDIS.getContainerId()).exec();
        }

        await().atMost(Duration.ofSeconds(40)).untilAsserted(() -> {
            for (String rideId : rideIds) {
                assertThat(outboxRows(rideId).get(0).get("published_at")).isNotNull();
            }
            assertThat(published()).isGreaterThanOrEqualTo(publishedBefore + 3);
        });
        for (String rideId : rideIds) {
            // At least once: a timed-out XADD may have reached Redis and the row was then
            // published again. Every copy is the same envelope.
            List<JsonNode> copies = faresFor(rideId);
            assertThat(copies).isNotEmpty();
            assertThat(copies.stream().map(JsonNode::toString).collect(Collectors.toSet())).hasSize(1);
            assertThat(copies.get(0).get("id").asText()).isEqualTo(outboxRows(rideId).get(0).get("id"));
        }
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(http.getForEntity("/readyz", String.class).getStatusCode().value()).isEqualTo(200));
    }

    /**
     * The commit is bounded too. Query timeouts do not cover {@code COMMIT}; the datasource's
     * socket timeout (5s) does. A deferred constraint trigger makes the commit of any pass that
     * marks a row of a private stream sleep on the server for 10s. Without the socket timeout the
     * relay thread would sit in that commit for 10s and then succeed, and no failure would ever be
     * counted; with it the pass fails at about 5s, while the server is still sleeping and the row
     * is still unmarked. The server finishes its commit regardless (a cut-off commit is not a
     * rolled-back commit), so the row ends up marked exactly once and the entry is on the stream.
     */
    @Test
    void aCommitThatHangsIsCutOffByTheSocketTimeoutAndTheRelayGoesOn() throws Exception {
        String stream = "events.ride.fares.commit-delay-test";
        String eventId = UUID.randomUUID().toString();
        double passFailuresBefore = passFailures();
        jdbc.execute("""
                create or replace function fare.commit_delay_for_test() returns trigger language plpgsql as $$
                begin
                    if new.stream = 'events.ride.fares.commit-delay-test' and new.published_at is not null then
                        perform pg_sleep(10);
                    end if;
                    return null;
                end $$
                """);
        jdbc.execute("""
                create constraint trigger event_outbox_commit_delay_for_test
                after update on fare.event_outbox deferrable initially deferred
                for each row execute function fare.commit_delay_for_test()
                """);
        try {
            jdbc.update("""
                    insert into fare.event_outbox (id, source_service, aggregate_id, event_type, stream, envelope, created_at)
                    values (?, 'fare-service', ?, 'fare_settled', ?, cast(? as jsonb), now())
                    """,
                    eventId, "ride-commit-delay", stream,
                    "{\"id\":\"" + eventId + "\",\"type\":\"fare_settled\",\"source\":\"fare-service\","
                            + "\"correlation_id\":\"ride-commit-delay\",\"occurred_at\":\"2026-09-12T10:00:00Z\",\"payload\":{}}");

            // Between the socket timeout (5s) and the end of the server's sleep (10s): the pass has
            // failed and the row is still unmarked.
            await().atMost(Duration.ofSeconds(9)).pollInterval(Duration.ofMillis(200)).untilAsserted(() -> {
                assertThat(passFailures()).isGreaterThanOrEqualTo(passFailuresBefore + 1);
                assertThat(publishedAt(eventId, stream)).as("the server is still in its commit").isNull();
            });
        } finally {
            jdbc.execute("drop trigger if exists event_outbox_commit_delay_for_test on fare.event_outbox");
            jdbc.execute("drop function if exists fare.commit_delay_for_test()");
        }

        // The server's commit lands (or, had it not, the next pass republishes): marked once, on the stream.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(publishedAt(eventId, stream)).isNotNull());
        Long count = redisTemplate.opsForStream().size(stream);
        assertThat(count).isNotNull().isGreaterThanOrEqualTo(1);
        assertThat(((Number) jdbc.queryForMap("select publish_attempts from fare.event_outbox where id = ? and stream = ?",
                eventId, stream).get("publish_attempts")).intValue()).isEqualTo(1);
    }

    @Test
    void metricsExposeTheOutboxSeries() {
        ResponseEntity<String> metrics = http.getForEntity("/metrics", String.class);
        assertThat(metrics.getBody())
                .contains("metroride_outbox_events_published_total{")
                .contains("metroride_outbox_publish_failures_total{")
                .contains("stream=\"events.ride.fares\"")
                .contains("metroride_fare_outbox_unpublished{")
                .contains("metroride_fare_outbox_pass_failures_total{");
    }

    // ---- helpers ---------------------------------------------------------------------------

    private String assignmentsStream() {
        return consumer.streams().get(0);
    }

    private String completionsStream() {
        return consumer.streams().get(1);
    }

    private void publish(String stream, String envelopeJson) {
        redisTemplate.opsForStream().add(StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, envelopeJson))
                .withStreamKey(stream));
    }

    private Envelope decode(String envelopeJson) {
        return codec.decode("test", Map.of(EnvelopeCodec.EVENT_FIELD, envelopeJson));
    }

    private int journalRows(String rideId, String kind) {
        Integer count = jdbc.queryForObject(
                "select count(*) from fare.journal_entries where ride_id = ? and kind = ?", Integer.class, rideId, kind);
        return count == null ? 0 : count;
    }

    private List<Map<String, Object>> outboxRows(String rideId) {
        return jdbc.queryForList("select * from fare.event_outbox where aggregate_id = ? order by created_at", rideId);
    }

    /** Every {@code fare_settled} on the stream whose payload names {@code rideId}; the stream is shared. */
    private List<JsonNode> faresFor(String rideId) {
        List<MapRecord<String, Object, Object>> records =
                redisTemplate.opsForStream().range(Envelope.STREAM_RIDE_FARES, Range.unbounded());
        if (records == null) {
            return List.of();
        }
        return records.stream()
                .map(record -> {
                    try {
                        return mapper.readTree(String.valueOf(record.getValue().get(EnvelopeCodec.EVENT_FIELD)));
                    } catch (java.io.IOException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .filter(json -> rideId.equals(json.path("correlation_id").asText()))
                .toList();
    }

    private double published() {
        return meterRegistry.get("metroride.outbox.events.published").tag("stream", Envelope.STREAM_RIDE_FARES).counter().count();
    }

    private double publishFailures() {
        return meterRegistry.get("metroride.outbox.publish.failures").tag("stream", Envelope.STREAM_RIDE_FARES).counter().count();
    }

    private Object publishedAt(String eventId, String stream) {
        return jdbc.queryForMap("select published_at from fare.event_outbox where id = ? and stream = ?", eventId, stream)
                .get("published_at");
    }

    private double passFailures() {
        return meterRegistry.get("metroride.fare.outbox.pass.failures").counter().count();
    }

    private double unpublishedGauge() {
        return meterRegistry.get("metroride.fare.outbox.unpublished").gauge().value();
    }

    private double postgresErrorCount() {
        return meterRegistry.get("metroride.dependency.errors").tag("dependency", "postgres").counter().count();
    }

    private double deadLetterCount(String reason) {
        return meterRegistry.get("metroride.fare.dead_letters").tag("reason", reason).counters().stream()
                .mapToDouble(io.micrometer.core.instrument.Counter::count).sum();
    }

    private static String assignment(String eventId, String rideId, double distanceKm, int etaSeconds) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-12T10:00:00Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"schema_version\":2,\"route_provider\":\"test-fixture\",\"route_calculated_at\":\"2026-09-11T10:00:00Z\",\"trip_distance_km\":" + distanceKm + ",\"trip_duration_seconds\":" + etaSeconds + ",\"distance_km\":" + distanceKm + ",\"eta_seconds\":" + etaSeconds + ",\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }

    private static String completion(String eventId, String rideId) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_completed\",\"source\":\"rider-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-12T10:05:00Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"assignment_id\":\"" + UUID.randomUUID() + "\",\"completed_at\":\"2026-09-12T10:05:00Z\"}}";
    }
}
