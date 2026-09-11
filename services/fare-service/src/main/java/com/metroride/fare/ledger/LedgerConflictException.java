package com.metroride.fare.ledger;

import com.metroride.fare.consumer.ClassifiedFailure;
import com.metroride.fare.consumer.DeadLetterReason;
import com.metroride.fare.consumer.FailureClass;
import java.util.Optional;

/**
 * The database refused a journal entry because the ride already has one of that kind: the
 * partial unique indexes of {@code V3__one_hold_and_one_settlement_per_ride.sql} allow one
 * {@code quote_hold} and one {@code settlement} per ride. The transaction that hit the index rolls
 * back whole (event row, any entry already written, this one), so the ledger is exactly as it was.
 *
 * <p>Quarantined, not fatal: unlike every other integrity violation (see {@code FailureClass}),
 * these two indexes are hit by a well-formed entry for a ride whose ledger is fine; the entry is
 * the odd one out. The recorder maps the settlement case to {@code SettlementException}
 * ({@code already_settled}), so only {@link Conflict#DUPLICATE_HOLD} reaches the consumer as this
 * type. It is counted on {@code metroride_fare_dead_letters_total} alone, under its reason.
 */
public class LedgerConflictException extends RuntimeException implements ClassifiedFailure {

    public enum Conflict {
        /** A second {@code quote_hold} for a ride; the offending event is a second {@code ride_assigned}. */
        DUPLICATE_HOLD("journal_entries_one_quote_hold_per_ride", DeadLetterReason.DUPLICATE_HOLD),
        /** A second {@code settlement} for a ride; the offending event is a second {@code ride_completed}. */
        DUPLICATE_SETTLEMENT("journal_entries_one_settlement_per_ride", DeadLetterReason.ALREADY_SETTLED);

        private final String indexName;
        private final DeadLetterReason deadLetterReason;

        Conflict(String indexName, DeadLetterReason deadLetterReason) {
            this.indexName = indexName;
            this.deadLetterReason = deadLetterReason;
        }

        /** The unique index whose violation means this conflict, as PostgreSQL names it in the error. */
        public String indexName() {
            return indexName;
        }

        static Optional<Conflict> forConstraint(String constraintName) {
            for (Conflict conflict : values()) {
                if (conflict.indexName.equals(constraintName)) {
                    return Optional.of(conflict);
                }
            }
            return Optional.empty();
        }
    }

    private final Conflict conflict;

    public LedgerConflictException(Conflict conflict, String message, Throwable cause) {
        super(message, cause);
        this.conflict = conflict;
    }

    public Conflict conflict() {
        return conflict;
    }

    @Override
    public FailureClass failureClass() {
        return FailureClass.QUARANTINE;
    }

    @Override
    public Optional<DeadLetterReason> deadLetterReason() {
        return Optional.of(conflict.deadLetterReason);
    }
}
