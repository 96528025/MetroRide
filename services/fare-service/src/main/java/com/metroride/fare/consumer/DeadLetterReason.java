package com.metroride.fare.consumer;

/** Value of the {@code reason} label on {@code metroride_fare_dead_letters_total}. */
public enum DeadLetterReason {
    /** The entry was {@link FailureClass#POISON}: dead-lettered on its first failure. */
    POISON("poison"),
    /** The entry was {@link FailureClass#RETRYABLE} and failed on its {@code max-deliveries}-th delivery. */
    MAX_DELIVERIES_REACHED("max_deliveries_reached");

    private final String label;

    DeadLetterReason(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
