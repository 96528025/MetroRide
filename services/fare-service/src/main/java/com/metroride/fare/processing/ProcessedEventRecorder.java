package com.metroride.fare.processing;

import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.events.RideAssigned;
import com.metroride.fare.events.RideCompleted;
import com.metroride.fare.ledger.CorruptLedgerException;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.ledger.LedgerRepository;
import com.metroride.fare.ledger.Money;
import com.metroride.fare.ledger.StoredJournalEntry;
import com.metroride.fare.pricing.FareCalculator;
import com.metroride.fare.pricing.FareProperties;
import com.metroride.fare.pricing.FareQuoteException;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional unit of work for one consumed envelope: record the event ID, and then by
 * type, all in one PostgreSQL transaction:
 *
 * <ul>
 *   <li>{@code ride_assigned}: quote the fare and append the {@code quote_hold} entry;</li>
 *   <li>{@code ride_completed}: lock the ride's {@code quote_hold}, check that it is the one
 *       entry the service writes and that the ride is not settled yet, then append the
 *       {@code hold_reversal} and the {@code settlement};</li>
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
     * nothing otherwise.
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
    private final Clock clock;

    public ProcessedEventRecorder(
            ProcessedEventRepository repository,
            LedgerRepository ledger,
            EnvelopeCodec codec,
            FareCalculator calculator,
            FareProperties rates,
            Clock clock) {
        this.repository = repository;
        this.ledger = ledger;
        this.codec = codec;
        this.calculator = calculator;
        this.rates = rates;
        this.clock = clock;
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
     * different completion events for one ride are a different case: their event rows do not
     * collide, so the row lock on the ride's {@code quote_hold} serialises them and the second one
     * finds the ride settled (see {@link #settle}).
     *
     * @throws FareQuoteException     when a {@code ride_assigned} payload cannot be quoted; the
     *                                transaction rolls back and the event is not recorded
     * @throws SettlementException    when a {@code ride_completed} cannot be settled, with the
     *                                reason that decides whether it is retried, quarantined or
     *                                dead-lettered as poison
     * @throws CorruptLedgerException when the ride's {@code quote_hold} rows are not a valid hold
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
                ledger.append(quoteHold, clock.instant());
                yield Result.recorded(List.of(quoteHold));
            }
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
            Money quote = calculator.quote(assignment.distanceKm(), assignment.etaSeconds());
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
     *   <li>Lock the ride's {@code quote_hold} rows ({@code select ... for update}). Zero rows: the
     *       assignment has not arrived, retryable. More than one: the ledger is ambiguous,
     *       quarantined.</li>
     *   <li>Check the one hold has the shape {@link JournalEntry#quoteHold} writes; anything else
     *       is a {@link CorruptLedgerException}, quarantined.</li>
     *   <li>With the lock held, check the ride is not settled yet; if it is, quarantined as
     *       {@code already_settled}. This is what stops two different completion events for one
     *       ride, from a replayed dead letter for instance, from settling the ride twice: the
     *       second waits on the hold's row lock and then sees the first's settlement.</li>
     *   <li>Append the {@code hold_reversal} and the {@code settlement}, both under this event's ID.</li>
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

        List<StoredJournalEntry> holds = ledger.lockQuoteHolds(rideId);
        if (holds.isEmpty()) {
            throw new SettlementException(SettlementException.Reason.MISSING_HOLD,
                    "no quote_hold for ride " + rideId + " yet (event " + envelope.id() + "); its ride_assigned may not have arrived");
        }
        if (holds.size() > 1) {
            throw new SettlementException(SettlementException.Reason.AMBIGUOUS_HOLD,
                    "ambiguous ledger: ride " + rideId + " has " + holds.size() + " quote_hold entries (event " + envelope.id() + ")");
        }
        Money quote = QuoteHoldShape.amountOf(rideId, holds.get(0).entry().postings());
        if (ledger.hasSettlement(rideId)) {
            throw new SettlementException(SettlementException.Reason.ALREADY_SETTLED,
                    "ride " + rideId + " is already settled; event " + envelope.id() + " would settle it again");
        }

        JournalEntry reversal = JournalEntry.holdReversal(rideId, envelope.id(), quote);
        JournalEntry settlement = JournalEntry.settlement(rideId, envelope.id(), quote, rates.driverShare());
        Instant now = clock.instant();
        ledger.append(reversal, now);
        ledger.append(settlement, now);
        return List.of(reversal, settlement);
    }
}
