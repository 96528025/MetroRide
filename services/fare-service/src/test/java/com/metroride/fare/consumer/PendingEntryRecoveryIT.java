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
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The poison side of pending-entry recovery, against the real containers from
 * {@link IntegrationTestSupport}: an entry that can never be handled is written to
 * {@code events.dead_letter} in the Go dead-letter shape and only then acknowledged, so the
 * pending list ends empty and PostgreSQL holds nothing of it. The retryable side is covered by the
 * lock-wait tests in {@code RideAssignmentConsumerIT} and {@code QuoteLedgerIT} (recovery),
 * {@link OldEntryRecoveryIT} (an old entry is retried, not dead-lettered on sight),
 * {@link ReclaimCursorIT} (entries behind failing ones are still reclaimed) and
 * {@link DeliveryCapIT} (delivery cap reached).
 *
 * <p>The failure of the dead-letter {@code XADD} itself is not automated: it needs Redis to refuse
 * one command while still serving the consumer's reads, and with a single connection to a single
 * Redis there is no such state to arrange. See the README.
 */
class PendingEntryRecoveryIT extends IntegrationTestSupport {

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
    MeterRegistry meterRegistry;

    @Autowired
    TestRestTemplate http;

    private DeadLetterStream deadLetters;

    @BeforeEach
    void deadLetterStream() {
        deadLetters = new DeadLetterStream(redisTemplate, mapper);
    }

    /**
     * A {@code ride_assigned} with a negative distance is rejected inside the transaction, which
     * rolls back; that is a poison entry, so it is dead-lettered on its first delivery, not
     * retried.
     */
    @Test
    void aPayloadThatCannotBeQuotedIsDeadLetteredAtOnceAndAcknowledged() {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        double poisonBefore = deadLetterCount("poison");
        double quoteFailuresBefore = quoteFailureCount("calculation");
        double reclaimedBefore = reclaimedCount();
        long deadLettersBefore = deadLetters.length();
        long pendingBefore = pendingEntries();
        Instant before = Instant.now();

        RecordId poison = publish(goEnvelope(eventId, rideId, -1.0, 60));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(deadLetterCount("poison")).isEqualTo(poisonBefore + 1);
            assertThat(pendingEntries()).isEqualTo(pendingBefore);
        });
        assertThat(isPending(poison)).as("acknowledged after the dead letter was published").isFalse();
        assertThat(deadLetters.length()).isEqualTo(deadLettersBefore + 1);
        assertThat(quoteFailureCount("calculation")).isEqualTo(quoteFailuresBefore + 1);
        assertThat(processedRows(eventId)).isZero();
        assertThat(journalRows(eventId)).isZero();

        JsonNode deadLetter = deadLetters.find(eventId).orElseThrow();
        assertThat(deadLetter.get("type").asText()).isEqualTo(Envelope.TYPE_DEAD_LETTERED);
        assertThat(deadLetter.get("source").asText()).isEqualTo("fare-service");
        assertThat(deadLetter.get("correlation_id").asText()).isEqualTo(rideId);
        assertThat(deadLetter.get("id").asText()).isNotEqualTo(eventId);
        JsonNode payload = deadLetter.get("payload");
        assertThat(payload.get("original_event_id").asText()).isEqualTo(eventId);
        assertThat(payload.get("original_event_type").asText()).isEqualTo(Envelope.TYPE_RIDE_ASSIGNED);
        assertThat(payload.get("ride_id").asText()).isEqualTo(rideId);
        assertThat(payload.get("service").asText()).isEqualTo("fare-service");
        assertThat(payload.get("error").asText())
                .contains("quote for ride " + rideId + " from event " + eventId)
                .contains("distance");
        assertThat(Instant.parse(payload.get("failed_at").asText())).isBetween(before, Instant.now());

        // Poison never enters the retry path.
        assertThat(reclaimedCount()).isEqualTo(reclaimedBefore);
    }

    /**
     * An entry whose {@code event} field is not JSON has no envelope to name, so the dead letter
     * carries the stream message ID as {@code original_event_id} and {@code decode_failed} as the
     * type, exactly as dispatch-service does for its stream.
     */
    @Test
    void anUndecodableEntryIsDeadLetteredUnderItsMessageId() {
        double poisonBefore = deadLetterCount("poison");
        double consumeErrorsBefore = consumeErrorCount();
        long pendingBefore = pendingEntries();

        RecordId garbage = redisTemplate.opsForStream().add(
                StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, "not-json")).withStreamKey(assignments()));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(deadLetterCount("poison")).isEqualTo(poisonBefore + 1);
            assertThat(pendingEntries()).isEqualTo(pendingBefore);
        });
        assertThat(isPending(garbage)).isFalse();
        assertThat(consumeErrorCount()).isEqualTo(consumeErrorsBefore + 1);

        JsonNode deadLetter = deadLetters.find(garbage.getValue()).orElseThrow();
        assertThat(deadLetter.get("correlation_id").asText()).isEmpty();
        JsonNode payload = deadLetter.get("payload");
        assertThat(payload.get("original_event_id").asText()).isEqualTo(garbage.getValue());
        assertThat(payload.get("original_event_type").asText()).isEqualTo(Envelope.TYPE_DECODE_FAILED);
        assertThat(payload.has("ride_id")).isFalse();
        assertThat(payload.get("error").asText()).contains("decode event envelope from message " + garbage.getValue());
    }

    /**
     * A decodable envelope whose values PostgreSQL refuses: a NUL character in the event ID passes
     * the codec (it is not blank) and is rejected as an SQLSTATE class 22 data exception (by the
     * server as 22021, or by the driver as 22023), which Spring reports as a
     * {@code DataIntegrityViolationException}. That must be poison, not fatal: the
     * entry is dead-lettered and the consumer keeps running, instead of halting on one crafted
     * entry and halting again on it after every restart.
     */
    @Test
    void aValueTheDatabaseRefusesIsDeadLetteredNotFatal() {
        // The decoded ID ends in a NUL character; on the wire it is the JSON escape \u0000, since
        // a raw control character is not valid JSON and would be a decode failure instead.
        String eventId = "nul-" + UUID.randomUUID() + "\u0000";
        String rideId = UUID.randomUUID().toString();
        double poisonBefore = deadLetterCount("poison");
        double postgresErrorsBefore = postgresErrorCount();
        double consumeErrorsBefore = consumeErrorCount();
        long pendingBefore = pendingEntries();

        RecordId entry = publish(goEnvelope(eventId.replace("\u0000", "\\u0000"), rideId, 1.0, 60));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(deadLetterCount("poison")).isEqualTo(poisonBefore + 1);
            assertThat(pendingEntries()).isEqualTo(pendingBefore);
        });
        assertThat(isPending(entry)).isFalse();
        assertThat(postgresErrorCount()).as("the failure came from PostgreSQL, not the codec").isEqualTo(postgresErrorsBefore + 1);
        assertThat(consumeErrorCount()).isEqualTo(consumeErrorsBefore);
        assertThat(meterRegistry.get("metroride.fare.consumer.halted").gauge().value()).isZero();
        assertThat(http.getForEntity("/readyz", String.class).getStatusCode().value()).isEqualTo(200);
        JsonNode payload = deadLetters.find(eventId).orElseThrow().get("payload");
        assertThat(payload.get("original_event_type").asText()).isEqualTo(Envelope.TYPE_RIDE_ASSIGNED);
        // pgjdbc refuses a NUL in a parameter itself ("Zero bytes may not occur", 22023) unless it
        // sends the parameter in binary, in which case the server refuses it (0x00 for UTF8, 22021).
        assertThat(payload.get("error").asText()).containsAnyOf("Zero bytes", "0x00");

        // The consumer is still alive: the next entry is handled normally.
        String nextId = UUID.randomUUID().toString();
        publish(goEnvelope(nextId, UUID.randomUUID().toString(), 1.0, 60));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(processedRows(nextId)).isEqualTo(1));
    }

    /** An envelope with an ID but no type is a decode failure too, dead-lettered under the message ID. */
    @Test
    void anEnvelopeWithoutATypeIsADecodeFailure() {
        String eventId = UUID.randomUUID().toString();
        double poisonBefore = deadLetterCount("poison");

        RecordId typeless = publish("{\"id\":\"" + eventId + "\",\"source\":\"dispatch-service\",\"payload\":{}}");

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(deadLetterCount("poison")).isEqualTo(poisonBefore + 1));
        JsonNode payload = deadLetters.find(typeless.getValue()).orElseThrow().get("payload");
        assertThat(payload.get("original_event_type").asText()).isEqualTo(Envelope.TYPE_DECODE_FAILED);
        assertThat(payload.get("error").asText()).isEqualTo("event envelope " + eventId + " has no type");
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

    private int journalRows(String eventId) {
        Integer count = jdbc.queryForObject(
                "select count(*) from fare.journal_entries where source_event_id = ?", Integer.class, eventId);
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

    private double quoteFailureCount(String reason) {
        return meterRegistry.get("metroride.fare.quote.failures").tag("reason", reason).counter().count();
    }

    private double consumeErrorCount() {
        return meterRegistry.get("metroride.stream.consume.errors").tag("stream", assignments()).counter().count();
    }

    /** Same shape as {@code events.Publish} writes: one field named {@code event} holding the envelope JSON. */
    private static String goEnvelope(String eventId, String rideId, double distanceKm, int etaSeconds) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-05T21:12:34.293710969Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"distance_km\":" + distanceKm + ",\"eta_seconds\":" + etaSeconds + ",\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }
}
