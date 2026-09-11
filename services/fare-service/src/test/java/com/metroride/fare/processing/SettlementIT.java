package com.metroride.fare.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.IntegrationTestSupport;
import com.metroride.fare.config.ConsumerProperties;
import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.consumer.DeadLetterReason;
import com.metroride.fare.consumer.FailureClass;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.ledger.LedgerConflictException;
import com.metroride.fare.ledger.LedgerRepository;
import com.metroride.fare.ledger.Money;
import com.metroride.fare.processing.ProcessedEventRecorder.Outcome;
import com.metroride.fare.processing.ProcessedEventRecorder.Result;
import io.micrometer.core.instrument.MeterRegistry;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Settlement on {@code ride_completed} against real PostgreSQL and Redis, on the containers and
 * the shared context from {@link IntegrationTestSupport} (default reclaim settings, so a retried
 * completion comes back within {@code reclaim-interval + reclaim-min-idle}). Assignments are
 * published on the first configured stream and completions on the second, as the Go relays do.
 *
 * <p>Every dead-letter assertion checks both the metric label and {@code payload.error}, because
 * the cross-language dead-letter JSON carries no {@code reason} field.
 */
class SettlementIT extends IntegrationTestSupport {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);
    /** 2.50 + 1.20 * 1.8612 + 0.30 * 223 / 60 = 5.84844 -> 5.85; driver 4.68, platform 1.17 at share 0.80. */
    private static final BigDecimal X = new BigDecimal("5.85");

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
    LedgerRepository ledger;

    @Autowired
    PlatformTransactionManager transactionManager;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    TestRestTemplate http;

    @Test
    void aCompletionAfterItsAssignmentReversesTheHoldAndSettlesByQuote() {
        String rideId = UUID.randomUUID().toString();
        String assignedId = UUID.randomUUID().toString();
        String completedId = UUID.randomUUID().toString();
        double holdsBefore = journalEntries("quote_hold");
        double reversalsBefore = journalEntries("hold_reversal");
        double settlementsBefore = journalEntries("settlement");
        double quotesBefore = quoteCount();

        publishAssignment(goAssignment(assignedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(assignedId)).isEqualTo(1));

        RecordId completed = publishCompletion(goCompletion(completedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(journalRows(completedId)).isEqualTo(2);
            assertThat(isPending(completions(), completed)).isFalse();
        });

        List<Map<String, Object>> entries = entries(rideId);
        assertThat(entries).extracting(row -> row.get("kind")).containsExactly("quote_hold", "hold_reversal", "settlement");
        assertThat(entries).extracting(row -> row.get("source_event_id")).containsExactly(assignedId, completedId, completedId);
        List<Map<String, Object>> postings = postings(rideId);
        assertThat(postings).hasSize(7);
        assertThat(postings).extracting(row -> row.get("account") + " " + row.get("amount")).containsExactly(
                "rider_receivable 5.85", "fare_hold -5.85",
                "rider_receivable -5.85", "fare_hold 5.85",
                "rider_receivable 5.85", "driver_payable -4.68", "platform_revenue -1.17");
        assertThat(sumOfPostings(rideId)).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(balance(rideId, "driver_payable").add(balance(rideId, "platform_revenue"))).isEqualByComparingTo(X.negate());
        assertThat(balance(rideId, "fare_hold")).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(balance(rideId, "rider_receivable")).isEqualByComparingTo(X);
        assertThat(journalEntries("quote_hold")).isEqualTo(holdsBefore + 1);
        assertThat(journalEntries("hold_reversal")).isEqualTo(reversalsBefore + 1);
        assertThat(journalEntries("settlement")).isEqualTo(settlementsBefore + 1);
        assertThat(quoteCount()).as("quotes count only holds").isEqualTo(quotesBefore + 1);

        ResponseEntity<String> ledger = http.getForEntity("/v1/rides/" + rideId + "/ledger", String.class);
        assertThat(ledger.getStatusCode().value()).isEqualTo(200);
        String body = ledger.getBody();
        int hold = body.indexOf("\"kind\":\"quote_hold\"");
        int reversal = body.indexOf("\"kind\":\"hold_reversal\"");
        int settlement = body.indexOf("\"kind\":\"settlement\"");
        assertThat(hold).isNotNegative();
        assertThat(reversal).isGreaterThan(hold);
        assertThat(settlement).isGreaterThan(reversal);
        assertThat(body).contains("{\"account\":\"driver_payable\",\"amount\":\"-4.68\"},{\"account\":\"platform_revenue\",\"amount\":\"-1.17\"}");

        // The same completion again: a duplicate, acknowledged, nothing written.
        double duplicatesBefore = duplicateCount(completions());
        RecordId again = publishCompletion(goCompletion(completedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(duplicateCount(completions())).isEqualTo(duplicatesBefore + 1);
            assertThat(isPending(completions(), again)).isFalse();
        });
        assertThat(entries(rideId)).hasSize(3);
        assertThat(postings(rideId)).hasSize(7);
        assertThat(journalEntries("settlement")).isEqualTo(settlementsBefore + 1);
    }

    /**
     * The ordering the two relays cannot promise: the completion lands before the assignment. It
     * must fail as a missing hold, stay pending on the completions stream, and settle on the
     * reclaimed delivery after the assignment has been recorded. This is the case the reclaim
     * pass exists for.
     */
    @Test
    void aCompletionThatArrivesBeforeItsAssignmentWaitsPendingUntilTheHoldExists() {
        String rideId = UUID.randomUUID().toString();
        String assignedId = UUID.randomUUID().toString();
        String completedId = UUID.randomUUID().toString();
        double missingBefore = settlementFailures("missing_hold");
        double reclaimedBefore = reclaimedCount(completions());
        long pendingBefore = pendingEntries(completions());

        RecordId completed = publishCompletion(goCompletion(completedId, rideId));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(settlementFailures("missing_hold")).isEqualTo(missingBefore + 1);
            assertThat(isPending(completions(), completed)).isTrue();
        });
        assertThat(processedRows(completedId)).isZero();
        assertThat(journalRows(completedId)).isZero();
        assertThat(pendingEntries(completions())).isEqualTo(pendingBefore + 1);

        publishAssignment(goAssignment(assignedId, rideId));

        await().atMost(consumer.reclaimInterval().plus(consumer.reclaimMinIdle()).plusSeconds(5)).untilAsserted(() -> {
            assertThat(journalRows(completedId)).isEqualTo(2);
            assertThat(isPending(completions(), completed)).isFalse();
            assertThat(pendingEntries(completions())).isEqualTo(pendingBefore);
        });
        assertThat(reclaimedCount(completions())).isGreaterThanOrEqualTo(reclaimedBefore + 1);
        assertThat(settlementFailures("missing_hold")).isGreaterThanOrEqualTo(missingBefore + 1);
        assertThat(entries(rideId)).extracting(row -> row.get("kind")).containsExactly("quote_hold", "hold_reversal", "settlement");
        assertThat(sumOfPostings(rideId)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    /**
     * Two different completion events for one ride, recorded at the same moment by two threads
     * (two instances, in production). Their event rows do not collide, so the idempotency insert
     * does not serialise them; the {@code select ... for update} on the ride's {@code quote_hold}
     * does. The second holds the lock only after the first has committed, then finds the
     * settlement and fails as {@code already_settled}, and the ledger has exactly one settlement.
     */
    @Test
    void twoDifferentCompletionEventsForOneRideSettleOnceThroughTheHoldLock() throws Exception {
        String rideId = UUID.randomUUID().toString();
        String assignedId = UUID.randomUUID().toString();
        publishAssignment(goAssignment(assignedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(assignedId)).isEqualTo(1));

        List<String> completionIds = List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Result>> calls = new ArrayList<>();
        for (String completionId : completionIds) {
            Envelope envelope = codec.decode("test", Map.of(EnvelopeCodec.EVENT_FIELD, goCompletion(completionId, rideId)));
            calls.add(() -> {
                start.await(5, TimeUnit.SECONDS);
                return recorder.record(completions(), envelope);
            });
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<Result>> futures = List.of(pool.submit(calls.get(0)), pool.submit(calls.get(1)));
            start.countDown();
            int settled = 0;
            int refused = 0;
            for (Future<Result> future : futures) {
                try {
                    Result result = future.get(10, TimeUnit.SECONDS);
                    assertThat(result.outcome()).isEqualTo(Outcome.RECORDED);
                    assertThat(result.entries()).hasSize(2);
                    settled++;
                } catch (ExecutionException failed) {
                    assertThat(failed.getCause()).isInstanceOf(SettlementException.class);
                    assertThat(((SettlementException) failed.getCause()).reason())
                            .isEqualTo(SettlementException.Reason.ALREADY_SETTLED);
                    refused++;
                }
            }
            assertThat(settled).isEqualTo(1);
            assertThat(refused).isEqualTo(1);
        } finally {
            pool.shutdownNow();
        }

        assertThat(entries(rideId)).extracting(row -> row.get("kind")).containsExactly("quote_hold", "hold_reversal", "settlement");
        assertThat(postings(rideId)).hasSize(7);
        assertThat(sumOfPostings(rideId)).isEqualByComparingTo(BigDecimal.ZERO);
        // The refused event was rolled back entirely: no event row, no journal rows.
        int recordedCompletions = 0;
        for (String completionId : completionIds) {
            recordedCompletions += processedRows(completionId);
            assertThat(journalRows(completionId)).isIn(0, 2);
        }
        assertThat(recordedCompletions).isEqualTo(1);
    }

    /**
     * A second, distinct {@code ride_assigned} for a ride whose hold is committed. Nothing in the
     * application orders a completion against a second assignment: the hold lock is on a row the
     * assignment never touches, and a completion that has read one hold cannot see an assignment
     * committing a second one behind it. That is the database's job. The partial unique index
     * {@code journal_entries_one_quote_hold_per_ride} refuses the second hold, the transaction
     * rolls back whole (no event row either), and the assignment entry is quarantined as
     * {@code duplicate_hold} with the ledger exactly as it was. The ride then settles as usual.
     */
    @Test
    void aSecondAssignmentForARideWithACommittedHoldIsQuarantinedAsDuplicateHold() {
        String rideId = UUID.randomUUID().toString();
        String assignedId = UUID.randomUUID().toString();
        String secondAssignedId = UUID.randomUUID().toString();
        String completedId = UUID.randomUUID().toString();
        publishAssignment(goAssignment(assignedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(assignedId)).isEqualTo(1));
        double duplicatesBefore = deadLetterCount(assignments(), "duplicate_hold");
        double holdsBefore = journalEntries("quote_hold");
        long pendingBefore = pendingEntries(assignments());

        RecordId second = publishAssignment(goAssignment(secondAssignedId, rideId));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(deadLetterCount(assignments(), "duplicate_hold")).isEqualTo(duplicatesBefore + 1);
            assertThat(isPending(assignments(), second)).as("acknowledged after the dead letter").isFalse();
        });
        assertThat(pendingEntries(assignments())).isEqualTo(pendingBefore);
        assertThat(processedRows(secondAssignedId)).as("rolled back with the refused hold").isZero();
        assertThat(journalRows(secondAssignedId)).isZero();
        assertThat(entries(rideId)).extracting(row -> row.get("source_event_id")).containsExactly(assignedId);
        assertThat(postings(rideId)).hasSize(2);
        assertThat(journalEntries("quote_hold")).isEqualTo(holdsBefore);
        assertThat(meterRegistry.get("metroride.fare.consumer.halted").gauge().value()).isZero();
        assertThat(http.getForEntity("/readyz", String.class).getStatusCode().value()).isEqualTo(200);
        JsonNode payload = deadLetter(secondAssignedId).get("payload");
        assertThat(payload.get("original_event_type").asText()).isEqualTo(Envelope.TYPE_RIDE_ASSIGNED);
        assertThat(payload.get("ride_id").asText()).isEqualTo(rideId);
        assertThat(payload.get("error").asText()).contains("already has a quote_hold").contains("journal_entries_one_quote_hold_per_ride");
        assertThat(payload.has("reason")).isFalse();

        publishCompletion(goCompletion(completedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(completedId)).isEqualTo(2));
        assertThat(entries(rideId)).extracting(row -> row.get("kind")).containsExactly("quote_hold", "hold_reversal", "settlement");
        assertThat(sumOfPostings(rideId)).isEqualByComparingTo(BigDecimal.ZERO);
    }

    /**
     * The same conflict with the first hold still uncommitted when the second assignment arrives,
     * the interleaving a completion-versus-assignment race can produce. The consumer's insert
     * waits on the unique index for the other transaction; when that one commits, the wait ends
     * in a conflict and the entry is quarantined as {@code duplicate_hold}. Only the committed
     * hold remains. (The holder commits well inside the 2s transaction timeout, so what is
     * observed is the index wait, not a cancelled statement.)
     */
    @Test
    void aSecondAssignmentWaitsOnAnUncommittedHoldAndIsQuarantinedOnceItCommits() throws Exception {
        String rideId = UUID.randomUUID().toString();
        String firstEventId = UUID.randomUUID().toString();
        String secondEventId = UUID.randomUUID().toString();
        double duplicatesBefore = deadLetterCount(assignments(), "duplicate_hold");
        RecordId second;

        try (Connection holder = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            holder.setAutoCommit(false);
            try (PreparedStatement event = holder.prepareStatement(
                    "insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())")) {
                event.setString(1, firstEventId);
                event.setString(2, assignments());
                event.setString(3, "ride_assigned");
                event.executeUpdate();
            }
            long journalId;
            try (PreparedStatement journal = holder.prepareStatement(
                    "insert into fare.journal_entries (ride_id, kind, source_event_id, created_at) values (?, 'quote_hold', ?, now()) returning id")) {
                journal.setString(1, rideId);
                journal.setString(2, firstEventId);
                try (var rs = journal.executeQuery()) {
                    rs.next();
                    journalId = rs.getLong(1);
                }
            }
            try (PreparedStatement posting = holder.prepareStatement(
                    "insert into fare.postings (journal_entry_id, account, amount) values (?, ?, ?)")) {
                posting.setLong(1, journalId);
                posting.setString(2, "rider_receivable");
                posting.setBigDecimal(3, X);
                posting.executeUpdate();
                posting.setString(2, "fare_hold");
                posting.setBigDecimal(3, X.negate());
                posting.executeUpdate();
            }

            second = publishAssignment(goAssignment(secondEventId, rideId));

            // Delivered at once, then waiting on the index for the uncommitted hold.
            Thread.sleep(700);
            assertThat(isPending(assignments(), second)).isTrue();
            assertThat(deadLetterCount(assignments(), "duplicate_hold")).isEqualTo(duplicatesBefore);
            assertThat(processedRows(secondEventId)).isZero();

            holder.commit();
        }

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(deadLetterCount(assignments(), "duplicate_hold")).isEqualTo(duplicatesBefore + 1);
            assertThat(isPending(assignments(), second)).isFalse();
        });
        assertThat(processedRows(secondEventId)).isZero();
        assertThat(entries(rideId)).extracting(row -> row.get("source_event_id")).containsExactly(firstEventId);
        assertThat(postings(rideId)).hasSize(2);
        assertThat(deadLetter(secondEventId).get("payload").get("error").asText()).contains("journal_entries_one_quote_hold_per_ride");
    }

    /**
     * The settlement side of V3, exercised at the repository since the recorder's own check
     * (under the hold's lock) reaches the index only in a race: a second {@code settlement} for a
     * ride is refused, reported as a {@link LedgerConflictException} that the recorder turns into
     * {@code already_settled}, and the transaction that hit it rolls back whole.
     */
    @Test
    void theSettlementIndexRefusesASecondSettlementForARide() {
        String rideId = UUID.randomUUID().toString();
        String first = UUID.randomUUID().toString();
        String second = UUID.randomUUID().toString();
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
            recordEvent(first, completions(), "ride_completed");
            recordEvent(second, completions(), "ride_completed");
            ledger.append(JournalEntry.settlement(rideId, first, Money.of(X), new BigDecimal("0.80")), Instant.now());
            ledger.append(JournalEntry.settlement(rideId, second, Money.of(X), new BigDecimal("0.80")), Instant.now());
        })).isInstanceOfSatisfying(LedgerConflictException.class, conflict -> {
            assertThat(conflict.conflict()).isEqualTo(LedgerConflictException.Conflict.DUPLICATE_SETTLEMENT);
            assertThat(conflict.deadLetterReason()).contains(DeadLetterReason.ALREADY_SETTLED);
            assertThat(conflict.getCause()).isInstanceOf(DuplicateKeyException.class);
            assertThat(FailureClass.of(conflict)).isEqualTo(FailureClass.QUARANTINE);
        });
        assertThat(entries(rideId)).as("the whole transaction rolled back").isEmpty();
        assertThat(processedRows(first)).isZero();
    }

    /**
     * Only the two per-ride indexes become a ledger conflict. The {@code (source_event_id, kind)}
     * key of V2 is still reported as Spring's {@link DuplicateKeyException} and classified fatal:
     * a writer hitting it bypassed the idempotency insert, which is a deployment fault.
     */
    @Test
    void anUnrelatedUniqueViolationKeepsSpringsExceptionAndStaysFatal() {
        String eventId = UUID.randomUUID().toString();
        TransactionTemplate transaction = new TransactionTemplate(transactionManager);

        assertThatThrownBy(() -> transaction.executeWithoutResult(status -> {
            recordEvent(eventId, assignments(), "ride_assigned");
            // Two rides, one source event: violates (source_event_id, kind) and nothing else.
            ledger.append(JournalEntry.quoteHold(UUID.randomUUID().toString(), eventId, Money.of(X)), Instant.now());
            ledger.append(JournalEntry.quoteHold(UUID.randomUUID().toString(), eventId, Money.of(X)), Instant.now());
        })).isInstanceOf(DuplicateKeyException.class)
                .isNotInstanceOf(LedgerConflictException.class)
                .satisfies(failure -> assertThat(FailureClass.of(failure)).isEqualTo(FailureClass.FATAL));
        assertThat(journalRows(eventId)).isZero();
    }

    @Test
    void aHoldOnTheWrongAccountQuarantinesTheCompletionAsCorrupt() {
        String rideId = UUID.randomUUID().toString();
        String completedId = UUID.randomUUID().toString();
        // Balanced, so the JournalEntry constructor accepts it on read; the credit is on the wrong account.
        insertHold(rideId, List.of(posting("rider_receivable", "5.85"), posting("driver_payable", "-5.85")));

        JsonNode payload = expectQuarantine(completedId, rideId, "corrupt_hold");

        assertThat(payload.get("error").asText()).contains("driver_payable").contains(rideId);
        assertThat(entries(rideId)).hasSize(1);
    }

    /** The read path refuses an entry without postings; that refusal is quarantine, not a halt. */
    @Test
    void aHoldWithoutPostingsQuarantinesTheCompletionAsCorrupt() {
        String rideId = UUID.randomUUID().toString();
        String completedId = UUID.randomUUID().toString();
        insertHold(rideId, List.of());

        JsonNode payload = expectQuarantine(completedId, rideId, "corrupt_hold");

        assertThat(payload.get("error").asText()).contains("at least one posting");
    }

    /** The last check before a rider is charged twice: a second, distinct completion event after settlement. */
    @Test
    void aSecondCompletionEventAfterSettlementIsQuarantinedAsAlreadySettled() {
        String rideId = UUID.randomUUID().toString();
        String assignedId = UUID.randomUUID().toString();
        String completedId = UUID.randomUUID().toString();
        String replayedId = UUID.randomUUID().toString();
        publishAssignment(goAssignment(assignedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(assignedId)).isEqualTo(1));
        publishCompletion(goCompletion(completedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(completedId)).isEqualTo(2));
        double settlementsBefore = journalEntries("settlement");

        JsonNode payload = expectQuarantine(replayedId, rideId, "already_settled");

        assertThat(payload.get("error").asText()).contains("already settled").contains(rideId);
        assertThat(entries(rideId)).extracting(row -> row.get("kind")).containsExactly("quote_hold", "hold_reversal", "settlement");
        assertThat(postings(rideId)).hasSize(7);
        assertThat(journalEntries("settlement")).isEqualTo(settlementsBefore);
    }

    /** No {@code ride_id}: poison, dead-lettered at once, and never counted or retried as a missing hold. */
    @Test
    void aCompletionWithoutARideIdIsPoisonNotAMissingHold() {
        String completedId = UUID.randomUUID().toString();
        String rideId = UUID.randomUUID().toString();
        double poisonBefore = deadLetterCount(completions(), "poison");
        double missingBefore = settlementFailures("missing_hold");
        double reclaimedBefore = reclaimedCount(completions());
        long pendingBefore = pendingEntries(completions());

        RecordId poison = publishCompletion("{\"id\":\"" + completedId + "\",\"type\":\"ride_completed\",\"source\":\"rider-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-10T21:20:00Z\","
                + "\"payload\":{\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\"}}");

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(deadLetterCount(completions(), "poison")).isEqualTo(poisonBefore + 1);
            assertThat(isPending(completions(), poison)).isFalse();
        });
        assertThat(pendingEntries(completions())).isEqualTo(pendingBefore);
        assertThat(settlementFailures("missing_hold")).isEqualTo(missingBefore);
        assertThat(reclaimedCount(completions())).isEqualTo(reclaimedBefore);
        assertThat(processedRows(completedId)).isZero();
        JsonNode payload = deadLetter(completedId).get("payload");
        assertThat(payload.get("original_event_type").asText()).isEqualTo(Envelope.TYPE_RIDE_COMPLETED);
        assertThat(payload.get("ride_id").asText()).as("falls back to the correlation id").isEqualTo(rideId);
        assertThat(payload.get("error").asText()).contains("has no ride_id");
    }

    /**
     * Rollback of a settlement that cannot finish. Another session holds {@code fare.postings} in
     * {@code EXCLUSIVE} mode, which lets reads through (the hold lookup and the settled check
     * succeed) but blocks inserts: the {@code hold_reversal} journal row is written, its first
     * posting waits, and the 2s transaction timeout cancels it. Afterwards nothing of the event
     * exists (no event row, no reversal, no settlement) and the hold is untouched; once the lock is
     * released the reclaim pass delivers the completion again and it settles.
     */
    @Test
    void aSettlementBlockedPastTheTimeoutRollsBackEverythingAndIsReclaimed() throws Exception {
        String rideId = UUID.randomUUID().toString();
        String assignedId = UUID.randomUUID().toString();
        String completedId = UUID.randomUUID().toString();
        publishAssignment(goAssignment(assignedId, rideId));
        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(journalRows(assignedId)).isEqualTo(1));
        double reversalsBefore = journalEntries("hold_reversal");
        double settlementsBefore = journalEntries("settlement");
        double reclaimedBefore = reclaimedCount(completions());
        long pendingBefore = pendingEntries(completions());
        RecordId completed;

        try (Connection lockHolder = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            lockHolder.setAutoCommit(false);
            try (Statement lock = lockHolder.createStatement()) {
                lock.execute("lock table fare.postings in exclusive mode");
            }
            double postgresErrorsBefore = postgresErrorCount();

            completed = publishCompletion(goCompletion(completedId, rideId));

            await().atMost(Duration.ofSeconds(8)).untilAsserted(() -> {
                assertThat(postgresErrorCount()).isEqualTo(postgresErrorsBefore + 1);
                assertThat(pendingEntries(completions())).isEqualTo(pendingBefore + 1);
            });
            assertThat(isPending(completions(), completed)).isTrue();
            lockHolder.rollback();

            assertThat(processedRows(completedId)).isZero();
            assertThat(journalRows(completedId)).isZero();
            assertThat(entries(rideId)).extracting(row -> row.get("kind")).containsExactly("quote_hold");
            assertThat(postings(rideId)).hasSize(2);
            assertThat(journalEntries("hold_reversal")).isEqualTo(reversalsBefore);
            assertThat(journalEntries("settlement")).isEqualTo(settlementsBefore);
        }

        await().atMost(consumer.reclaimInterval().plus(consumer.reclaimMinIdle()).plusSeconds(3)).untilAsserted(() -> {
            assertThat(reclaimedCount(completions())).isGreaterThanOrEqualTo(reclaimedBefore + 1);
            assertThat(journalRows(completedId)).isEqualTo(2);
            assertThat(pendingEntries(completions())).isEqualTo(pendingBefore);
        });
        assertThat(isPending(completions(), completed)).isFalse();
        assertThat(postings(rideId)).hasSize(7);
        assertThat(sumOfPostings(rideId)).isEqualByComparingTo(BigDecimal.ZERO);
        assertThat(journalEntries("hold_reversal")).isEqualTo(reversalsBefore + 1);
        assertThat(journalEntries("settlement")).isEqualTo(settlementsBefore + 1);
    }

    @Test
    void metricsExposeTheSettlementSeries() {
        ResponseEntity<String> metrics = http.getForEntity("/metrics", String.class);
        assertThat(metrics.getBody())
                .contains("metroride_fare_journal_entries_total{")
                .contains("metroride_fare_settlement_failures_total{")
                .contains("reason=\"missing_hold\"")
                .contains("reason=\"ambiguous_hold\"")
                .contains("reason=\"corrupt_hold\"")
                .contains("reason=\"already_settled\"")
                .contains("metroride_fare_dead_letters_total{reason=\"duplicate_hold\"")
                .doesNotContain("metroride_fare_dead_letters_total{reason=\"ambiguous_hold\"");
        assertThatThrownBy(() -> meterRegistry.get("metroride.fare.quotes").tag("kind", "settlement").counter())
                .as("quotes count holds only").isInstanceOf(io.micrometer.core.instrument.search.MeterNotFoundException.class);
    }

    // ---- helpers ------------------------------------------------------------------------------

    /**
     * Publishes a completion for {@code rideId}, waits for it to be dead-lettered under
     * {@code reason} and acknowledged, and asserts what quarantine promises: the settlement failure
     * counter for the reason moved, nothing of the event was recorded, the consumer is alive and
     * ready. Returns the dead letter's payload for the caller's {@code error} assertions.
     */
    private JsonNode expectQuarantine(String completedId, String rideId, String reason) {
        double deadLettersBefore = deadLetterCount(completions(), reason);
        double failuresBefore = settlementFailures(reason);
        double missingBefore = settlementFailures("missing_hold");
        long pendingBefore = pendingEntries(completions());

        RecordId completed = publishCompletion(goCompletion(completedId, rideId));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            assertThat(deadLetterCount(completions(), reason)).isEqualTo(deadLettersBefore + 1);
            assertThat(isPending(completions(), completed)).as("acknowledged after the dead letter").isFalse();
        });
        assertThat(pendingEntries(completions())).isEqualTo(pendingBefore);
        assertThat(settlementFailures(reason)).isEqualTo(failuresBefore + 1);
        assertThat(settlementFailures("missing_hold")).isEqualTo(missingBefore);
        assertThat(processedRows(completedId)).isZero();
        assertThat(journalRows(completedId)).isZero();
        assertThat(meterRegistry.get("metroride.fare.consumer.halted").gauge().value()).isZero();
        assertThat(http.getForEntity("/readyz", String.class).getStatusCode().value()).isEqualTo(200);

        JsonNode deadLetter = deadLetter(completedId);
        assertThat(deadLetter.get("type").asText()).isEqualTo(Envelope.TYPE_DEAD_LETTERED);
        assertThat(deadLetter.get("correlation_id").asText()).isEqualTo(rideId);
        JsonNode payload = deadLetter.get("payload");
        assertThat(payload.get("original_event_id").asText()).isEqualTo(completedId);
        assertThat(payload.get("original_event_type").asText()).isEqualTo(Envelope.TYPE_RIDE_COMPLETED);
        assertThat(payload.get("ride_id").asText()).isEqualTo(rideId);
        assertThat(payload.get("service").asText()).isEqualTo("fare-service");
        assertThat(payload.has("reason")).as("the cross-language schema has no reason field").isFalse();
        return payload;
    }

    /** An event row written by hand, inside the caller's transaction (the journal has a foreign key to it). */
    private void recordEvent(String eventId, String stream, String type) {
        jdbc.update("insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())",
                eventId, stream, type);
    }

    /** A {@code quote_hold} written by hand, with its event row (the journal has a foreign key to it). */
    private long insertHold(String rideId, List<Object[]> postings) {
        String eventId = UUID.randomUUID().toString();
        jdbc.update("insert into fare.processed_events (event_id, stream, event_type, processed_at) values (?, ?, ?, now())",
                eventId, assignments(), "ride_assigned");
        Long journalId = jdbc.queryForObject(
                "insert into fare.journal_entries (ride_id, kind, source_event_id, created_at) values (?, 'quote_hold', ?, now()) returning id",
                Long.class, rideId, eventId);
        for (Object[] posting : postings) {
            jdbc.update("insert into fare.postings (journal_entry_id, account, amount) values (?, ?, ?)",
                    journalId, posting[0], new BigDecimal((String) posting[1]));
        }
        return journalId;
    }

    private static Object[] posting(String account, String amount) {
        return new Object[] {account, amount};
    }

    private JsonNode deadLetter(String originalEventId) {
        List<MapRecord<String, Object, Object>> records =
                redisTemplate.opsForStream().range(Envelope.STREAM_DEAD_LETTER, org.springframework.data.domain.Range.unbounded());
        Optional<JsonNode> found = records == null ? Optional.empty() : records.stream()
                .map(record -> {
                    try {
                        return mapper.readTree(String.valueOf(record.getValue().get(EnvelopeCodec.EVENT_FIELD)));
                    } catch (java.io.IOException e) {
                        throw new IllegalStateException("dead letter " + record.getId() + " is not JSON", e);
                    }
                })
                .filter(json -> originalEventId.equals(json.path("payload").path("original_event_id").asText()))
                .findFirst();
        return found.orElseThrow(() -> new AssertionError("no dead letter for " + originalEventId));
    }

    private RecordId publishAssignment(String envelopeJson) {
        return redisTemplate.opsForStream().add(StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, envelopeJson))
                .withStreamKey(assignments()));
    }

    private RecordId publishCompletion(String envelopeJson) {
        return redisTemplate.opsForStream().add(StreamRecords.string(Map.of(EnvelopeCodec.EVENT_FIELD, envelopeJson))
                .withStreamKey(completions()));
    }

    /** The assignments stream: first in {@code metroride.consumer.streams}. */
    private String assignments() {
        return consumer.streams().get(0);
    }

    /** The completions stream: second in {@code metroride.consumer.streams}. */
    private String completions() {
        return consumer.streams().get(1);
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

    private List<Map<String, Object>> entries(String rideId) {
        return jdbc.queryForList("select kind, source_event_id from fare.journal_entries where ride_id = ? order by id", rideId);
    }

    private List<Map<String, Object>> postings(String rideId) {
        return jdbc.queryForList("""
                select p.account, p.amount from fare.postings p
                join fare.journal_entries j on j.id = p.journal_entry_id
                where j.ride_id = ? order by j.id, p.id
                """, rideId);
    }

    private BigDecimal sumOfPostings(String rideId) {
        return jdbc.queryForObject("""
                select coalesce(sum(p.amount), 0) from fare.postings p
                join fare.journal_entries j on j.id = p.journal_entry_id
                where j.ride_id = ?
                """, BigDecimal.class, rideId);
    }

    private BigDecimal balance(String rideId, String account) {
        return jdbc.queryForObject("""
                select coalesce(sum(p.amount), 0) from fare.postings p
                join fare.journal_entries j on j.id = p.journal_entry_id
                where j.ride_id = ? and p.account = ?
                """, BigDecimal.class, rideId, account);
    }

    private long pendingEntries(String stream) {
        return redisTemplate.opsForStream().pending(stream, consumer.group()).getTotalPendingMessages();
    }

    private boolean isPending(String stream, RecordId id) {
        return !redisTemplate.opsForStream()
                .pending(stream, consumer.group(), Range.closed(id.getValue(), id.getValue()), 1)
                .isEmpty();
    }

    private double journalEntries(String kind) {
        return meterRegistry.get("metroride.fare.journal.entries").tag("kind", kind).counter().count();
    }

    private double quoteCount() {
        return meterRegistry.get("metroride.fare.quotes").tag("kind", "quote_hold").counter().count();
    }

    private double settlementFailures(String reason) {
        return meterRegistry.get("metroride.fare.settlement.failures").tag("reason", reason).counter().count();
    }

    private double deadLetterCount(String stream, String reason) {
        return meterRegistry.get("metroride.fare.dead_letters").tag("stream", stream).tag("reason", reason).counter().count();
    }

    private double reclaimedCount(String stream) {
        return meterRegistry.get("metroride.fare.events.reclaimed").tag("stream", stream).counter().count();
    }

    private double duplicateCount(String stream) {
        return meterRegistry.get("metroride.fare.events.processed").tag("stream", stream).tag("outcome", "duplicate").counter().count();
    }

    private double postgresErrorCount() {
        return meterRegistry.get("metroride.dependency.errors").tag("dependency", "postgres").counter().count();
    }

    /** A {@code ride_assigned} as dispatch-service publishes it: 1.8612 km and 223 s quote 5.85. */
    private static String goAssignment(String eventId, String rideId) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_assigned\",\"source\":\"dispatch-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-10T21:12:34.293710969Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"distance_km\":1.8612,\"eta_seconds\":223,\"assignment_id\":\"" + UUID.randomUUID() + "\"}}";
    }

    /** A {@code ride_completed} as rider-service publishes it through its outbox. */
    private static String goCompletion(String eventId, String rideId) {
        return "{\"id\":\"" + eventId + "\",\"type\":\"ride_completed\",\"source\":\"rider-service\","
                + "\"correlation_id\":\"" + rideId + "\",\"occurred_at\":\"2026-09-10T21:20:00.293710969Z\","
                + "\"payload\":{\"ride_id\":\"" + rideId + "\",\"rider_id\":\"rider-42\",\"driver_id\":\"driver-2\","
                + "\"assignment_id\":\"" + UUID.randomUUID() + "\",\"completed_at\":\"2026-09-10T21:20:00.293710969Z\"}}";
    }
}
