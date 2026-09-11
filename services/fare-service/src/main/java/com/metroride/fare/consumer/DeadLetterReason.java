package com.metroride.fare.consumer;

/**
 * Value of the {@code reason} label on {@code metroride_fare_dead_letters_total}. The label is the
 * only place the reason is recorded: the cross-language {@code DeadLetter} payload has no
 * {@code reason} field, so the specific cause is written into its {@code error} text.
 */
public enum DeadLetterReason {
    /** The entry was {@link FailureClass#POISON}: dead-lettered on its first failure. */
    POISON("poison"),
    /** The entry was {@link FailureClass#RETRYABLE} and failed on its {@code max-deliveries}-th delivery. */
    MAX_DELIVERIES_REACHED("max_deliveries_reached"),
    /**
     * {@link FailureClass#QUARANTINE}: a {@code ride_assigned} for a ride that already has a
     * {@code quote_hold}; refused by the per-ride unique index, so the ledger is intact and only
     * this second assignment event is set aside.
     */
    DUPLICATE_HOLD("duplicate_hold"),
    /** {@link FailureClass#QUARANTINE}: the ride's {@code quote_hold} rows do not form the entry the service writes. */
    CORRUPT_HOLD("corrupt_hold"),
    /** {@link FailureClass#QUARANTINE}: the ride already has a {@code settlement} from another event. */
    ALREADY_SETTLED("already_settled");

    private final String label;

    DeadLetterReason(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
