package com.metroride.fare.consumer;

import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.pricing.FareQuoteException;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;

/**
 * What the consumer does with a stream entry whose handling threw. The mapping lives here, not in
 * the consumer, so the next failure modes (a completion whose hold is not in the ledger yet, a
 * completion whose ride has more than one hold) are added by extending {@link #of} and its unit
 * test, without touching the consumer loop.
 */
public enum FailureClass {

    /**
     * Retrying the same entry later can succeed: the entry stays in the pending entry list and the
     * reclaim pass delivers it again until its age exceeds the retry budget.
     */
    RETRYABLE,

    /**
     * Retrying can never succeed: the entry is dead-lettered on the spot and acknowledged once the
     * dead letter is on {@code events.dead_letter}.
     */
    POISON;

    /**
     * Pure mapping from the exceptions {@code handle()} can see to a class.
     *
     * <ul>
     *   <li>{@link EnvelopeDecodeException} (the entry is not an envelope) and
     *       {@link FareQuoteException} (the payload cannot be quoted) are {@link #POISON}: the
     *       entry's bytes will not change, so a second delivery fails the same way.</li>
     *   <li>{@link DataAccessException} and {@link TransactionException} are {@link #RETRYABLE}:
     *       every PostgreSQL failure, whether a cancelled lock wait, a lost connection or a
     *       timed-out commit, arrives as one of these two, and none of them says anything about
     *       the entry itself.</li>
     *   <li>Anything else is {@link #POISON}. All PostgreSQL access in this service goes through
     *       Spring's exception translation, so a failure of any other type is a programming error;
     *       retrying it for the length of the budget would only postpone the same dead letter.</li>
     * </ul>
     */
    public static FailureClass of(Throwable failure) {
        if (failure instanceof EnvelopeDecodeException || failure instanceof FareQuoteException) {
            return POISON;
        }
        if (failure instanceof DataAccessException || failure instanceof TransactionException) {
            return RETRYABLE;
        }
        return POISON;
    }
}
