package com.metroride.fare.processing;

import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.events.FareSettled;
import com.metroride.fare.events.RideAssigned;
import com.metroride.fare.events.RideCompleted;
import com.metroride.fare.events.RideCancelled;
import com.metroride.fare.ledger.JournalKind;
import java.math.BigDecimal;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.metroride.fare.ledger.Account;
import com.metroride.fare.ledger.CorruptLedgerException;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.ledger.LedgerConflictException;
import com.metroride.fare.ledger.LedgerRepository;
import com.metroride.fare.ledger.Money;
import com.metroride.fare.ledger.Posting;
import com.metroride.fare.ledger.StoredJournalEntry;
import com.metroride.fare.outbox.OutboxRepository;
import com.metroride.fare.pricing.FareCalculator;
import com.metroride.fare.pricing.FareProperties;
import com.metroride.fare.pricing.FareQuoteException;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional unit of work for one consumed envelope: record the event ID, and then by
 * type, all in one PostgreSQL transaction:
 *
 * <ul>
 *   <li>{@code ride_assigned}: quote the fare and append the {@code quote_hold} entry;</li>
 *   <li>{@code ride_completed}: lock the ride state and {@code quote_hold}, check that it is the one
 *       entry the service writes and that the ride is not settled yet, then append the
 *       {@code hold_reversal} and the {@code settlement}, and enqueue the {@code fare_settled}
 *       event for {@code events.ride.fares} in {@code fare.event_outbox};</li>
 *   <li>{@code ride_cancelled}: record cancellation and reverse an existing hold without a fee;</li>
 *   <li>anything else: recorded, nothing more.</li>
 * </ul>
 *
 * <p>Order inside the transaction matters. The idempotency insert runs first; when it inserts
 * nothing the envelope was fully handled by an earlier delivery, and the method returns
 * {@link Outcome#DUPLICATE} without touching the ledger.
 */
@Service
public class ProcessedEventRecorder {

    public enum Outcome {
        /** First time this event ID was seen; the row was inserted. */
        RECORDED,
        /** The event ID was already present; nothing was written. */
        DUPLICATE
    }

    /**
     * What one call did. {@code entries} are the journal entries written in the same transaction,
     * in the order written: one {@code quote_hold} for a first-seen {@code ride_assigned}, a
     * {@code hold_reversal} then a {@code settlement} for a first-seen {@code ride_completed},
     * a {@code cancellation_reversal} when canceling an existing hold, nothing otherwise.
     */
    public record Result(Outcome outcome, List<JournalEntry> entries) {

        public Result {
            entries = List.copyOf(entries);
        }

        static Result recorded() {
            return new Result(Outcome.RECORDED, List.of());
        }

        static Result recorded(List<JournalEntry> entries) {
            return new Result(Outcome.RECORDED, entries);
        }

        static Result duplicate() {
            return new Result(Outcome.DUPLICATE, List.of());
        }
    }

    private final ProcessedEventRepository repository;
    private final LedgerRepository ledger;
    private final EnvelopeCodec codec;
    private final FareCalculator calculator;
    private final FareProperties rates;
    private final OutboxRepository outbox;
    private final ObjectMapper mapper;
    private final Clock clock;
    private final RideContextRepository context;

    public ProcessedEventRecorder(
            ProcessedEventRepository repository,
            LedgerRepository ledger,
            EnvelopeCodec codec,
            FareCalculator calculator,
            FareProperties rates,
            OutboxRepository outbox,
            ObjectMapper mapper,
            Clock clock,
            RideContextRepository context) {
        this.repository = repository;
        this.ledger = ledger;
        this.codec = codec;
        this.calculator = calculator;
        this.rates = rates;
        this.outbox = outbox;
        this.mapper = mapper;
        this.clock = clock;
        this.context = context;
    }

    /**
     * Runs in its own transaction. The caller acknowledges the stream entry only after this
     * method returns, i.e. after the transaction has committed; if the process dies in between,
     * the redelivered entry lands on the conflict clause and is acknowledged as a duplicate.
     *
     * <p>The transaction is bounded by {@code metroride.postgres.timeout-seconds}. Spring passes
     * the remaining transaction time to every JDBC statement as its query timeout, whether it is
     * issued by Hibernate or by the ledger's {@code JdbcClient}, so a statement stuck waiting for a
     * lock is cancelled by the driver, the whole transaction rolls back (event row and journal
     * entries alike), and the caller sees a {@code DataAccessException}: the entry stays pending
     * and the consumer moves on instead of stalling on one event.
     *
     * <p>Two deliveries of the same envelope that arrive at once are serialised by the primary key
     * of {@code fare.processed_events}: the second insert waits for the first transaction to
     * commit, then hits the conflict clause and returns {@link Outcome#DUPLICATE} without ever
     * reaching the ledger. The unique key on {@code journal_entries (source_event_id, kind)} is a
     * backstop for writers that bypass this method, not the mechanism relied on here. Two
     * different events for one ride are serialized by a persistent fare.ride_state row. This
     * also orders a cancellation that arrives before an assignment: the cancellation tombstone
     * prevents that late assignment from creating a hold. Existing per-ride unique indexes remain
     * a backstop against duplicate holds and settlements.
     *
     * @throws FareQuoteException     when a {@code ride_assigned} payload cannot be quoted; the
     *                                transaction rolls back and the event is not recorded
     * @throws SettlementException    when a {@code ride_completed} cannot be settled, with the
     *                                reason that decides whether it is retried, quarantined or
     *                                dead-lettered as poison
     * @throws CorruptLedgerException when the ride's {@code quote_hold} rows are not a valid hold
     * @throws LedgerConflictException when the database refused a second {@code quote_hold} for
     *                                 the ride; the transaction rolls back and the event is not
     *                                 recorded
     */
    @Transactional(timeoutString = "${metroride.postgres.timeout-seconds}")
    public Result record(String stream, Envelope envelope) {
        int inserted = repository.insertIfAbsent(envelope.id(), stream, envelope.type(), clock.instant());
        if (inserted == 0) {
            return Result.duplicate();
        }
        return switch (envelope.type()) {
            case Envelope.TYPE_RIDE_ASSIGNED -> {
                JournalEntry quoteHold = quoteHold(envelope);
                if (quoteHold == null) yield Result.recorded();
                ledger.append(quoteHold, clock.instant());
                context.save(codec.decodePayload(envelope, RideAssigned.class), envelope.id(), rates);
                yield Result.recorded(List.of(quoteHold));
            }
            case Envelope.TYPE_RIDE_CANCELLED -> Result.recorded(cancel(envelope));
            case Envelope.TYPE_RIDE_COMPLETED -> Result.recorded(settle(envelope));
            default -> Result.recorded();
        };
    }

    private JournalEntry quoteHold(Envelope envelope) {
        RideAssigned assignment;
        try {
            assignment = codec.decodePayload(envelope, RideAssigned.class);
        } catch (EnvelopeDecodeException e) {
            throw new FareQuoteException(FareQuoteException.Reason.PAYLOAD, e.getMessage(), e);
        }
        if (assignment.rideId() == null || assignment.rideId().isBlank()) {
            throw new FareQuoteException(FareQuoteException.Reason.PAYLOAD,
                    "ride_assigned payload of event " + envelope.id() + " has no ride_id", null);
        }
        try {
            if ("cancelled".equals(context.lock(assignment.rideId()))) return null;
            if (!Integer.valueOf(2).equals(assignment.schemaVersion())
                    || assignment.routeProvider() == null || assignment.routeProvider().isBlank()
                    || assignment.routeCalculatedAt() == null) {
                throw new IllegalArgumentException("version 2 passenger route evidence is required; approach fields cannot price a trip");
            }
            try { Instant.parse(assignment.routeCalculatedAt()); }
            catch (java.time.format.DateTimeParseException e) { throw new IllegalArgumentException("invalid route_calculated_at", e); }
            Money quote = calculator.quote(assignment.tripDistanceKm(), assignment.tripDurationSeconds());
            return JournalEntry.quoteHold(assignment.rideId(), envelope.id(), quote);
        } catch (IllegalArgumentException e) {
            throw new FareQuoteException(FareQuoteException.Reason.CALCULATION,
                    "quote for ride " + assignment.rideId() + " from event " + envelope.id() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Settlement by quote: the amount settled is the amount held, not a recomputed fare. In order:
     *
     * <ol>
     *   <li>Decode the payload; an unusable one is poison ({@link SettlementException.Reason#PAYLOAD}),
     *       never a missing hold, or an entry that can never name its ride would be retried
     *       forever as "assignment not here yet".</li>
     *   <li>Lock the persistent ride state, reject cancellation, then lock {@code quote_hold} rows ({@code select ... for update}). Zero rows: the
     *       assignment has not arrived, retryable. More than one: a state the V3 unique index makes
     *       impossible, so the index is gone or the schema has drifted; fatal, the consumer halts
     *       rather than settle against the first of several holds.</li>
     *   <li>Check the one hold has the shape {@link JournalEntry#quoteHold} writes; anything else
     *       is a {@link CorruptLedgerException}, quarantined.</li>
     *   <li>With the lock held, check the ride is not settled yet; if it is, quarantined as
     *       {@code already_settled}. This is what stops two different completion events for one
     *       ride, from a replayed dead letter for instance, from settling the ride twice: the
     *       second waits on the ride-state row lock and then sees the first's settlement.</li>
     *   <li>Append the {@code hold_reversal} and the {@code settlement}, both under this event's ID.
     *       Should a second settlement slip past the check above, the per-ride unique index on
     *       settlements refuses it and the whole transaction, reversal included, rolls back; that
     *       is reported as {@code already_settled} too.</li>
     *   <li>Enqueue one {@code fare_settled} envelope for {@code events.ride.fares} in
     *       {@code fare.event_outbox}. Same transaction, so the event exists exactly when the two
     *       entries exist; the relay publishes it after the commit.</li>
     * </ol>
     */
    private List<JournalEntry> settle(Envelope envelope) {
        RideCompleted completion;
        try {
            completion = codec.decodePayload(envelope, RideCompleted.class);
        } catch (EnvelopeDecodeException e) {
            throw new SettlementException(SettlementException.Reason.PAYLOAD, e.getMessage(), e);
        }
        if (completion.rideId() == null || completion.rideId().isBlank()) {
            throw new SettlementException(SettlementException.Reason.PAYLOAD,
                    "ride_completed payload of event " + envelope.id() + " has no ride_id");
        }
        String rideId = completion.rideId();
        if ("cancelled".equals(context.lock(rideId))) {
            throw new SettlementException(SettlementException.Reason.CANCELLED_RIDE, "ride " + rideId + " was cancelled");
        }

        List<StoredJournalEntry> holds = ledger.lockQuoteHolds(rideId);
        if (holds.isEmpty()) {
            throw new SettlementException(SettlementException.Reason.MISSING_HOLD,
                    "no quote_hold for ride " + rideId + " yet (event " + envelope.id() + "); its ride_assigned may not have arrived");
        }
        if (holds.size() > 1) {
            throw new SettlementException(SettlementException.Reason.AMBIGUOUS_HOLD,
                    "ride " + rideId + " has " + holds.size() + " quote_hold entries, which the unique index"
                            + " journal_entries_one_quote_hold_per_ride should make impossible (event " + envelope.id() + ")");
        }
        Money quote = QuoteHoldShape.amountOf(rideId, holds.get(0).entry().postings());
        if (ledger.hasSettlement(rideId)) {
            throw new SettlementException(SettlementException.Reason.ALREADY_SETTLED,
                    "ride " + rideId + " is already settled; event " + envelope.id() + " would settle it again");
        }

        JournalEntry reversal = JournalEntry.holdReversal(rideId, envelope.id(), quote);
        // A historical hold has no recorded split: its amount is preserved and the current
        // driver-share policy applies. Version 2 holds always carry their original split.
        BigDecimal driverShare = context.driverShare(rideId).orElse(rates.driverShare());
        JournalEntry settlement = JournalEntry.settlement(rideId, envelope.id(), quote, driverShare);
        Instant now = clock.instant();
        try {
            ledger.append(reversal, now);
            ledger.append(settlement, now);
        } catch (LedgerConflictException conflict) {
            if (conflict.conflict() != LedgerConflictException.Conflict.DUPLICATE_SETTLEMENT) {
                throw conflict;
            }
            throw new SettlementException(SettlementException.Reason.ALREADY_SETTLED,
                    "ride " + rideId + " was settled by another transaction; event " + envelope.id()
                            + " refused by " + conflict.conflict().indexName(), conflict);
        }
        outbox.enqueue(Envelope.STREAM_RIDE_FARES, fareSettled(envelope, completion, quote, settlement, driverShare, now), now);
        context.mark(rideId, "completed");
        return List.of(reversal, settlement);
    }

    private List<JournalEntry> cancel(Envelope envelope) {
        RideCancelled cancellation;
        try { cancellation = codec.decodePayload(envelope, RideCancelled.class); }
        catch (EnvelopeDecodeException e) {
            throw new SettlementException(SettlementException.Reason.PAYLOAD, e.getMessage(), e);
        }
        String rideId = cancellation.rideId();
        if (rideId == null || rideId.isBlank()) {
            throw new SettlementException(SettlementException.Reason.PAYLOAD, "ride_cancelled requires ride_id");
        }
        String state = context.lock(rideId);
        if ("cancelled".equals(state)) return List.of();
        if ("completed".equals(state) || ledger.hasSettlement(rideId)) {
            throw new SettlementException(SettlementException.Reason.ALREADY_SETTLED, "cannot cancel settled ride " + rideId);
        }
        List<StoredJournalEntry> holds = ledger.lockQuoteHolds(rideId);
        if (holds.size() > 1) {
            throw new SettlementException(SettlementException.Reason.AMBIGUOUS_HOLD, "multiple quote holds for " + rideId);
        }
        List<JournalEntry> entries = List.of();
        if (!holds.isEmpty()) {
            Money amount = QuoteHoldShape.amountOf(rideId, holds.get(0).entry().postings());
            JournalEntry reversal = new JournalEntry(rideId, JournalKind.CANCELLATION_REVERSAL,
                    envelope.id(), JournalEntry.holdReversal(rideId, envelope.id(), amount).postings());
            ledger.append(reversal, clock.instant());
            entries = List.of(reversal);
        }
        context.mark(rideId, "cancelled");
        return entries;
    }

    /** The {@code fare_settled} envelope: figures copied from the settlement entry, never recomputed. */
    private Envelope fareSettled(Envelope completion, RideCompleted payload, Money quote, JournalEntry settlement, BigDecimal driverShare, Instant now) {
        FareSettled settled = new FareSettled(
                payload.rideId(),
                payload.riderId(),
                payload.driverId(),
                payload.assignmentId(),
                completion.id(),
                quote.toString(),
                credited(settlement, Account.DRIVER_PAYABLE).toString(),
                credited(settlement, Account.PLATFORM_REVENUE).toString(),
                driverShare.toPlainString(),
                DateTimeFormatter.ISO_INSTANT.format(now));
        return new Envelope(
                UUID.randomUUID().toString(),
                Envelope.TYPE_FARE_SETTLED,
                com.metroride.fare.FareServiceApplication.SERVICE_NAME,
                payload.rideId(),
                now,
                mapper.valueToTree(settled));
    }

    /** The amount credited to {@code account} in {@code entry}, as a positive figure; zero when there is no such posting. */
    private static Money credited(JournalEntry entry, Account account) {
        for (Posting posting : entry.postings()) {
            if (posting.account() == account) {
                return posting.amount().negate();
            }
        }
        return Money.ZERO;
    }
}
