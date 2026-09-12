package com.metroride.fare.outbox;

import java.time.Duration;

/**
 * The delay before the next attempt of a failed outbox row, the same function as
 * {@code retryBackoff} in {@code shared/pkg/outbox/outbox.go}: start at the poll interval, double
 * per previous attempt, and jump to the cap as soon as the doubled value would reach half of it.
 * With the defaults (250ms, 30s): 250ms, 500ms, 1s, 2s, 4s, 8s, 16s, then 30s for every further
 * attempt. {@code RetryBackoffTest} pins those values against the Go ones.
 */
public final class RetryBackoff {

    private RetryBackoff() {
    }

    /**
     * @param previousAttempts how many times the row has been tried so far ({@code publish_attempts}
     *                         before this failure was recorded)
     * @param initial          the first delay; the relay's poll interval
     * @param max              the cap
     */
    public static Duration delay(int previousAttempts, Duration initial, Duration max) {
        Duration delay = initial;
        Duration halfMax = max.dividedBy(2);
        for (int attempt = 0; attempt < previousAttempts; attempt++) {
            if (delay.compareTo(halfMax) >= 0) {
                return max;
            }
            delay = delay.multipliedBy(2);
        }
        return delay.compareTo(max) > 0 ? max : delay;
    }
}
