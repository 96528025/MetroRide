package com.metroride.fare.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.metroride.fare.IntegrationTestSupport;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.ledger.LedgerRepository;
import com.metroride.fare.processing.ProcessedEventRecorder.Outcome;
import com.metroride.fare.processing.ProcessedEventRecorder.Result;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The quote-and-hold path against real PostgreSQL and Redis: a {@code ride_assigned} envelope
 * produces one processed event, one {@code quote_hold} journal entry and two postings that cancel
 * out; a redelivery adds nothing; two concurrent deliveries add nothing either; and a failure
 * inside the transaction leaves neither the event row nor the journal entry behind until the
 * reclaimed delivery writes both.
 *
 * <p>Containers and context come from {@link IntegrationTestSupport}.
 */
class QuoteLedgerIT extends IntegrationTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ConsumerProperties consumer;

    @Autowired
    ProcessedEventRecorder recorder;

    @Autowired
    EnvelopeCodec codec;

    @Autowired
    LedgerRepository ledger;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    TestRestTemplate http;

    @Test
    void assignmentWritesOneBalancedQuoteHoldAndRedeliveryAddsNothing() {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        String envelope = goEnvelope(eventId, rideId, 1.8612, 223);
        double quotesBefore = quoteCount();

        publish(envelope);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(processedRows(eventId)).isEqualTo(1);
            assertThat(journalRows(eventId)).isEqualTo(1);
            assertThat(quoteCount()).isEqualTo(quotesBefore + 1);
        });
        List<Map<String, Object>> postings = postings(rideId);
        assertThat(postings).hasSize(2);
        assertThat(postings).extracting(row -> row.get("account")).containsExactly("rider_receivable", "fare_hold");
        // 2.50 + 1.20 * 1.8612 + 0.30 * 223 / 60 = 5.84844 -> 5.85
        assertThat(postings).extracting(row -> (BigDecimal) row.get("amount"))
                .containsExactly(new BigDecimal("5.85"), new BigDecimal("-5.85"));
        assertThat(sumOfPostings(rideId)).isEqualByComparingTo(BigDecimal.ZERO);
        Map<String, Object> journal = jdbc.queryForMap(
                "select ride_id, kind from fare.journal_entries where source_event_id = ?", eventId);
        assertThat(journal.get("ride_id")).isEqualTo(rideId);
        assertThat(journal.get("kind")).isEqualTo("quote_hold");

        double duplicatesBefore = duplicateCount();
        RecordId second = publish(envelope);

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(lastDeliveredId()).isEqualTo(second.getValue());
            assertThat(duplicateCount()).isEqualTo(duplicatesBefore + 1);
            assertThat(pendingEntries()).isZero();
        });
        assertThat(processedRows(eventId)).isEqualTo(1);
        assertThat(journalRows(eventId)).isEqualTo(1);
        assertThat(postings(rideId)).hasSize(2);
        assertThat(quoteCount()).isEqualTo(quotesBefore + 1);
    }

    @Test
    void ledgerEndpointReturnsTheRidesEntriesInTheGoStyle() {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();

        ResponseEntity<String> missing = http.getForEntity("/v1/rides/" + rideId + "/ledger", String.class);
        assertThat(missing.getStatusCode().value()).isEqualTo(404);
        assertThat(missing.getBody()).isEqualTo("{\"error\":\"ledger not found\"}");

        publish(goEnvelope(eventId, rideId, 0, 600));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(eventId)).isEqualTo(1));

        ResponseEntity<String> found = http.getForEntity("/v1/rides/" + rideId + "/ledger", String.class);
        assertThat(found.getStatusCode().value()).isEqualTo(200);
        assertThat(found.getHeaders().getContentType().toString()).startsWith("application/json");
        // 2.50 + 0.30 * 600 / 60 = 5.50
        assertThat(found.getBody())
                .startsWith("{\"ride_id\":\"" + rideId + "\",\"entries\":[{\"id\":")
                .contains("\"kind\":\"quote_hold\",\"source_event_id\":\"" + eventId + "\",\"created_at\":\"")
                .endsWith("\"postings\":[{\"account\":\"rider_receivable\",\"amount\":\"5.50\"},"
                        + "{\"account\":\"fare_hold\",\"amount\":\"-5.50\"}]}]}");
    }

    @Test
    void envelopesOfOtherTypesAreRecordedWithoutALedgerEntry() {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        double quotesBefore = quoteCount();

        RecordId id = publish("{\"id\":\"" + eventId + "\",\"type\":\"ride_completed\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-07T10:00:00Z\",\"payload\":{}}");

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(processedRows(eventId)).isEqualTo(1);
            assertThat(lastDeliveredId()).isEqualTo(id.getValue());
            assertThat(pendingEntries()).isZero();
        });
        assertThat(journalRows(eventId)).isZero();
        assertThat(quoteCount()).isEqualTo(quotesBefore);
    }

    /**
     * Two threads call {@code record()} for the same envelope at the same moment. What keeps the
     * ledger to one entry is the primary key of {@code fare.processed_events}: PostgreSQL blocks
     * the second {@code INSERT ... ON CONFLICT DO NOTHING} until the first transaction commits,
     * then resolves it as a conflict, so the second call returns DUPLICATE and never reaches the
     * journal insert. The unique key on {@code journal_entries (source_event_id, kind)} is not what
     * stops it; it would only fire for a writer that skipped the idempotency insert.
     *
     * <p>This test shows the outcome under real concurrency; it cannot force the two transactions
     * to overlap at the conflict point. {@link #aRecordBlockedByAnUncommittedEventRowReturnsDuplicate}
     * pins the mechanism itself.
     */
    @Test
    void concurrentDeliveriesOfOneEventWriteOneJournalEntry() throws Exception {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        Envelope envelope = codec.decode("test", Map.of(EnvelopeCodec.EVENT_FIELD, goEnvelope(eventId, rideId, 3.0, 300)));
        CountDownLatch start = new CountDownLatch(1);
        Callable<Result> call = () -> {
            start.await(5, TimeUnit.SECONDS);
            return recorder.record(consumer.stream(), envelope);
        };

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<Result>> futures = List.of(pool.submit(call), pool.submit(call));
            start.countDown();
            List<Outcome> outcomes = new ArrayList<>();
            for (Future<Result> future : futures) {
                outcomes.add(future.get(10, TimeUnit.SECONDS).outcome());
            }
            assertThat(outcomes).containsExactlyInAnyOrder(Outcome.RECORDED, Outcome.DUPLICATE);
        } finally {
            pool.shutdownNow();
        }

        assertThat(processedRows(eventId)).isEqualTo(1);
        assertThat(journalRows(eventId)).isEqualTo(1);
        assertThat(postings(rideId)).hasSize(2);
        assertThat(sumOfPostings(rideId)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    /**
     * The mechanism behind the previous test, made deterministic: another session has inserted the
     * event row and not committed. {@code record()} for the same event must block on that row (it
     * is still running well after it would otherwise have finished), and once the other session
     * commits it must return DUPLICATE and write no journal entry. The holder commits within the 2s
     * transaction timeout, so the wait is a lock wait, not a cancellation.
     */
    @Test
    void aRecordBlockedByAnUncommittedEventRowReturnsDuplicate() throws Exception {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        Envelope envelope = codec.decode("test", Map.of(EnvelopeCodec.EVENT_FIELD, goEnvelope(eventId, rideId, 3.0, 300)));
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try (Connection holder = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            holder.setAutoCommit(false);
            try (PreparedStatement insert = holder.prepareStatement(
                    "insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())")) {
                insert.setString(1, eventId);
                insert.setString(2, consumer.stream());
                insert.setString(3, "ride_assigned");
                insert.executeUpdate();
            }

            Future<Result> blocked = pool.submit(() -> recorder.record(consumer.stream(), envelope));
            assertThatThrownBy(() -> blocked.get(500, TimeUnit.MILLISECONDS))
                    .as("record() must wait on the uncommitted event row")
                    .isInstanceOf(java.util.concurrent.TimeoutException.class);
            assertThat(journalRows(eventId)).isZero();

            holder.commit();

            assertThat(blocked.get(5, TimeUnit.SECONDS).outcome()).isEqualTo(Outcome.DUPLICATE);
        } finally {
            pool.shutdownNow();
        }
        assertThat(processedRows(eventId)).isEqualTo(1);
        assertThat(journalRows(eventId)).isZero();
        assertThat(postings(rideId)).isEmpty();
    }

    /**
     * Reads must not hide a journal entry that has no postings: the left join surfaces it and the
     * {@code JournalEntry} constructor refuses it, so the endpoint fails instead of answering 404.
     */
    @Test
    void anEntryWithoutPostingsIsRefusedOnReadNotHidden() {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        jdbc.update("insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())",
                eventId, consumer.stream(), "ride_assigned");
        jdbc.update("insert into fare.journal_entries (ride_id, kind, source_event_id, created_at) values (?, ?, ?, now())",
                rideId, "quote_hold", eventId);

        // The constructor's IllegalArgumentException, translated by @Repository into Spring's
        // data-access hierarchy.
        assertThatThrownBy(() -> ledger.findByRideId(rideId))
                .isInstanceOf(InvalidDataAccessApiUsageException.class)
                .hasMessageContaining("at least one posting");
        ResponseEntity<String> response = http.getForEntity("/v1/rides/" + rideId + "/ledger", String.class);
        assertThat(response.getStatusCode().value()).isEqualTo(500);
    }

    /**
     * The event row and the journal entry are one transaction. Another session holds an exclusive
     * lock on {@code fare.journal_entries}, so the consumer's event insert succeeds, its journal
     * insert waits, and the 2s transaction timeout cancels it. Afterwards neither the event row nor
     * the journal entry exists, and the entry stays pending. Once the lock is released the reclaim
     * pass delivers the entry again and both rows are written by that delivery; nothing is
     * acknowledged by hand. The poison counterpart (a payload that can never be quoted) is in
     * {@code PendingEntryRecoveryIT}.
     */
    @Test
    void aFailedJournalInsertRollsBackTheEventRowTooAndIsReclaimed() throws Exception {
        String eventId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        double quotesBefore = quoteCount();
        double reclaimedBefore = reclaimedCount();
        long pendingBefore = pendingEntries();
        try (Connection lockHolder = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            lockHolder.setAutoCommit(false);
            try (Statement lock = lockHolder.createStatement()) {
                lock.execute("lock table fare.journal_entries in access exclusive mode");
            }
            double postgresErrorsBefore = postgresErrorCount();

            publish(goEnvelope(eventId, rideId, 1.0, 60));

            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
                assertThat(postgresErrorCount()).isEqualTo(postgresErrorsBefore + 1);
                assertThat(pendingEntries()).isEqualTo(pendingBefore + 1);
            });
            lockHolder.rollback();

            assertThat(processedRows(eventId)).isZero();
            assertThat(journalRows(eventId)).isZero();
            assertThat(postings(rideId)).isEmpty();
            assertThat(quoteCount()).isEqualTo(quotesBefore);
        }

        await().atMost(consumer.reclaimInterval().plus(consumer.reclaimMinIdle()).plusSeconds(3)).untilAsserted(() -> {
            assertThat(reclaimedCount()).isGreaterThanOrEqualTo(reclaimedBefore + 1);
            assertThat(processedRows(eventId)).isEqualTo(1);
            assertThat(journalRows(eventId)).isEqualTo(1);
            assertThat(pendingEntries()).isEqualTo(pendingBefore);
        });
        assertThat(postings(rideId)).hasSize(2);
        assertThat(sumOfPostings(rideId)).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(quoteCount()).isEqualTo(quotesBefore + 1);
    }

    @Test
    void metricsExposeTheQuoteSeries() {
        ResponseEntity<String> metrics = http.getForEntity("/metrics", String.class);
        assertThat(metrics.getBody())
                .contains("metroride_fare_quotes_total{")
                .contains("metroride_fare_quote_failures_total{")
                .contains("reason=\"payload\"")
                .contains("reason=\"calculation\"");
    }

    private RecordId publish(String envelopeJson) {
        return redisTemplate.opsForStream().add(StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, envelopeJson))
                .withStreamKey(consumer.stream()));
    }

    private int processedRows(String eventId) {
        return count("select count(*) from fare.processed_events where event_id = ?", eventId);
    }

    private int journalRows(String eventId) {
        return count("select count(*) from fare.journal_entries where source_event_id = ?", eventId);
    }

    private int count(String sql, String arg) {
        Integer count = jdbc.queryForObject(sql, Integer.class, arg);
        return count == null ? 0 : count;
    }

    private List<Map<String, Object>> postings(String rideId) {
        return jdbc.queryForList("""
                select p.account, p.amount from fare.postings p
                join fare.journal_entries j on j.id = p.journal_entry_id
                where j.ride_id = ? order by p.id
                """, rideId);
    }

    private BigDecimal sumOfPostings(String rideId) {
        return jdbc.queryForObject("""
                select coalesce(sum(p.amount), 0) from fare.postings p
                join fare.journal_entries j on j.id = p.journal_entry_id
                where j.ride_id = ?
                """, BigDecimal.class, rideId);
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

    private double postgresErrorCount() {
        return meterRegistry.get("metroride.dependency.errors").tag("dependency", "postgres").counter().count();
    }

    private double duplicateCount() {
        return meterRegistry.get("metroride.fare.events.processed").tag("outcome", "duplicate").counter().count();
    }

    private double quoteCount() {
        return meterRegistry.get("metroride.fare.quotes").tag("kind", "quote_hold").counter().count();
    }

    private double reclaimedCount() {
        return meterRegistry.get("metroride.fare.events.reclaimed").tag("stream", consumer.stream()).counter().count();
    }

    /** Same shape as {@code events.Publish} writes: one field named {@code event} holding the envelope JSON. */
    private static String goEnvelope(String eventId, String rideId, double distanceKm, int etaSeconds) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-05T21:12:34.293710969Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"distance_km\":" + distanceKm + ",\"eta_seconds\":" + etaSeconds + ",\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }
}
