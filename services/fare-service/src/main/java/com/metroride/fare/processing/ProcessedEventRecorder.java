package com.metroride.fare.processing;

import com.metroride.fare.events.Envelope;
import java.time.Clock;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The transactional unit of work for one consumed envelope. Today it only records the event ID.
 *
 * <p>Boundary for later work: fare calculation and ledger postings belong inside this same
 * transaction, keyed off the {@link Outcome}. When {@link Outcome#DUPLICATE} is returned the
 * envelope was fully handled by an earlier delivery and nothing else should run.
 */
@Service
public class ProcessedEventRecorder {

    public enum Outcome {
        /** First time this event ID was seen; the row was inserted. */
        RECORDED,
        /** The event ID was already present; nothing was written. */
        DUPLICATE
    }

    private final ProcessedEventRepository repository;
    private final Clock clock;

    public ProcessedEventRecorder(ProcessedEventRepository repository, Clock clock) {
        this.repository = repository;
        this.clock = clock;
    }

    /**
     * Runs in its own transaction. The caller acknowledges the stream entry only after this
     * method returns, i.e. after the transaction has committed; if the process dies in between,
     * the redelivered entry lands on the conflict clause and is acknowledged as a duplicate.
     *
     * <p>The transaction is bounded by {@code metroride.postgres.timeout-seconds}. Hibernate
     * passes the remaining transaction time to every JDBC statement as its query timeout, so a
     * statement stuck waiting for a row lock is cancelled by the driver, the transaction rolls
     * back, and the caller sees a {@code DataAccessException}: the entry stays pending and the
     * consumer moves on instead of stalling on one event. Work added here later shares the same
     * budget.
     */
    @Transactional(timeoutString = "${metroride.postgres.timeout-seconds}")
    public Outcome record(String stream, Envelope envelope) {
        int inserted = repository.insertIfAbsent(envelope.id(), stream, envelope.type(), clock.instant());
        return inserted == 1 ? Outcome.RECORDED : Outcome.DUPLICATE;
    }
}
