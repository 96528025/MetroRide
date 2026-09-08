package com.metroride.fare.processing;

import com.metroride.fare.events.Envelope;
import com.metroride.fare.events.EnvelopeCodec;
import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.events.RideAssigned;
import com.metroride.fare.ledger.JournalEntry;
import com.metroride.fare.ledger.LedgerRepository;
import com.metroride.fare.ledger.Money;
import com.metroride.fare.pricing.FareCalculator;
import com.metroride.fare.pricing.FareQuoteException;
import com.metroride.fare.pricing.FareQuoteException.Reason;
import java.time.Clock;
import java.util.Optional;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional unit of work for one consumed envelope: record the event ID, and for a
 * {@code ride_assigned} envelope also quote the fare and append the {@code quote_hold} journal
 * entry, all in one PostgreSQL transaction.
 *
 * <p>Order inside the transaction matters. The idempotency insert runs first; when it inserts
 * nothing the envelope was fully handled by an earlier delivery, and the method returns
 * {@link Outcome#DUPLICATE} without touching the ledger. Envelopes of any other type are recorded
 * and nothing else happens to them.
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
     * What one call did. {@code quoteHold} is present only when the envelope was a first-seen
     * {@code ride_assigned} and its journal entry was written in the same transaction.
     */
    public record Result(Outcome outcome, Optional<JournalEntry> quoteHold) {

        static Result recorded() {
            return new Result(Outcome.RECORDED, Optional.empty());
        }

        static Result quoted(JournalEntry quoteHold) {
            return new Result(Outcome.RECORDED, Optional.of(quoteHold));
        }

        static Result duplicate() {
            return new Result(Outcome.DUPLICATE, Optional.empty());
        }
    }

    private final ProcessedEventRepository repository;
    private final LedgerRepository ledger;
    private final EnvelopeCodec codec;
    private final FareCalculator calculator;
    private final Clock clock;

    public ProcessedEventRecorder(
            ProcessedEventRepository repository,
            LedgerRepository ledger,
            EnvelopeCodec codec,
            FareCalculator calculator,
            Clock clock) {
        this.repository = repository;
        this.ledger = ledger;
        this.codec = codec;
        this.calculator = calculator;
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
     * entry alike), and the caller sees a {@code DataAccessException}: the entry stays pending and
     * the consumer moves on instead of stalling on one event.
     *
     * <p>Two deliveries of the same envelope that arrive at once are serialised by the primary key
     * of {@code fare.processed_events}: the second insert waits for the first transaction to
     * commit, then hits the conflict clause and returns {@link Outcome#DUPLICATE} without ever
     * reaching the ledger. The unique key on {@code journal_entries (source_event_id, kind)} is a
     * backstop for writers that bypass this method, not the mechanism relied on here.
     *
     * @throws FareQuoteException when a {@code ride_assigned} payload cannot be quoted; the
     *                            transaction rolls back and the event is not recorded
     */
    @Transactional(timeoutString = "${metroride.postgres.timeout-seconds}")
    public Result record(String stream, Envelope envelope) {
        int inserted = repository.insertIfAbsent(envelope.id(), stream, envelope.type(), clock.instant());
        if (inserted == 0) {
            return Result.duplicate();
        }
        if (!Envelope.TYPE_RIDE_ASSIGNED.equals(envelope.type())) {
            return Result.recorded();
        }
        JournalEntry quoteHold = quoteHold(envelope);
        ledger.append(quoteHold, clock.instant());
        return Result.quoted(quoteHold);
    }

    private JournalEntry quoteHold(Envelope envelope) {
        RideAssigned assignment;
        try {
            assignment = codec.decodePayload(envelope, RideAssigned.class);
        } catch (EnvelopeDecodeException e) {
            throw new FareQuoteException(Reason.PAYLOAD, e.getMessage(), e);
        }
        if (assignment.rideId() == null || assignment.rideId().isBlank()) {
            throw new FareQuoteException(Reason.PAYLOAD,
                    "ride_assigned payload of event " + envelope.id() + " has no ride_id", null);
        }
        try {
            Money quote = calculator.quote(assignment.distanceKm(), assignment.etaSeconds());
            return JournalEntry.quoteHold(assignment.rideId(), envelope.id(), quote);
        } catch (IllegalArgumentException e) {
            throw new FareQuoteException(Reason.CALCULATION,
                    "quote for ride " + assignment.rideId() + " from event " + envelope.id() + ": " + e.getMessage(), e);
        }
    }
}
