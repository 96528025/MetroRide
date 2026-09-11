package com.metroride.fare.processing;

import com.metroride.fare.consumer.ClassifiedFailure;
import com.metroride.fare.consumer.DeadLetterReason;
import com.metroride.fare.consumer.FailureClass;

/**
 * A {@code ride_completed} envelope could not be settled. Thrown inside the recording
 * transaction, so nothing of the event is recorded; the consumer disposes of the entry according
 * to the reason's {@link FailureClass}, which the exception carries itself.
 */
public class SettlementException extends RuntimeException implements ClassifiedFailure {

    public enum Reason {
        /**
         * The payload was missing, not a {@code RideCompleted}, or had no {@code ride_id}. Poison:
         * the bytes will not change. Never filed as {@link #MISSING_HOLD}, or an entry that can
         * never name its ride would be retried as if its assignment were merely late.
         */
        PAYLOAD("payload", FailureClass.POISON, DeadLetterReason.POISON),
        /**
         * The ride has no {@code quote_hold} yet. Retryable: the assignment travels on another
         * stream through another outbox relay and may simply not have arrived; the reclaim pass
         * delivers the completion again and it settles once the hold is there.
         */
        MISSING_HOLD("missing_hold", FailureClass.RETRYABLE, DeadLetterReason.MAX_DELIVERIES_REACHED),
        /**
         * The ride has more than one {@code quote_hold}. Quarantined: the entry may be fine but the
         * ride's ledger is not, retrying cannot fix an append-only ledger, and halting would let
         * one ride stop every other ride.
         */
        AMBIGUOUS_HOLD("ambiguous_hold", FailureClass.QUARANTINE, DeadLetterReason.AMBIGUOUS_HOLD),
        /**
         * The ride already has a {@code settlement} from a different completion event. Quarantined:
         * the status guard in rider-service never publishes a second completion, but a replayed
         * dead letter can, and this is the last check before the ride would be settled twice.
         */
        ALREADY_SETTLED("already_settled", FailureClass.QUARANTINE, DeadLetterReason.ALREADY_SETTLED);

        private final String label;
        private final FailureClass failureClass;
        private final DeadLetterReason deadLetterReason;

        Reason(String label, FailureClass failureClass, DeadLetterReason deadLetterReason) {
            this.label = label;
            this.failureClass = failureClass;
            this.deadLetterReason = deadLetterReason;
        }

        /** Value of the {@code reason} label on {@code metroride_fare_settlement_failures_total}. */
        public String label() {
            return label;
        }

        public FailureClass failureClass() {
            return failureClass;
        }

        public DeadLetterReason deadLetterReason() {
            return deadLetterReason;
        }
    }

    private final Reason reason;

    public SettlementException(Reason reason, String message) {
        this(reason, message, null);
    }

    public SettlementException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason reason() {
        return reason;
    }

    @Override
    public FailureClass failureClass() {
        return reason.failureClass;
    }

    @Override
    public DeadLetterReason deadLetterReason() {
        return reason.deadLetterReason;
    }
}
