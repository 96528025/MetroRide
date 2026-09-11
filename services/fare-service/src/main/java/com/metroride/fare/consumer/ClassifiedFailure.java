package com.metroride.fare.consumer;

/**
 * An exception that knows its own {@link FailureClass}, and the dead-letter reason to file it
 * under if the consumer gives up on the entry. {@link FailureClass#of} consults this before any of
 * its rules for Spring's data-access hierarchy, so a failure raised by this service's own code is
 * classified by the code that raised it, not by guesswork over exception types.
 */
public interface ClassifiedFailure {

    FailureClass failureClass();

    /**
     * The {@code reason} the entry is dead-lettered under. For a {@link FailureClass#QUARANTINE}
     * or {@link FailureClass#POISON} failure that is the reason of the immediate dead letter; for
     * a {@link FailureClass#RETRYABLE} failure it is {@link DeadLetterReason#MAX_DELIVERIES_REACHED},
     * the only way a retryable entry is ever given up on.
     */
    DeadLetterReason deadLetterReason();
}
