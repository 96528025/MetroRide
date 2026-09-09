package com.metroride.fare.consumer;

import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.pricing.FareQuoteException;
import java.sql.SQLException;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
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
     * reclaim pass delivers it again, up to {@code max-deliveries} times in total.
     */
    RETRYABLE,

    /**
     * The entry itself can never be handled: it is dead-lettered on the spot and acknowledged once
     * the dead letter is on {@code events.dead_letter}.
     */
    POISON,

    /**
     * The failure is not the entry's but the deployment's: wrong SQL, a schema that does not match
     * the code, misuse of a data-access API, or a broken database invariant. Every entry would fail
     * the same way, so neither retrying nor dead-lettering is right; the entry stays pending and the
     * consumer stops until the deployment is fixed.
     */
    FATAL;

    /**
     * Pure mapping from the exceptions {@code handle()} can see to a class.
     *
     * <ul>
     *   <li>{@link EnvelopeDecodeException} (the entry is not an envelope) and
     *       {@link FareQuoteException} (the payload cannot be quoted) are {@link #POISON}: the
     *       entry's bytes will not change, so a second delivery fails the same way.</li>
     *   <li>{@link DataIntegrityViolationException} is two things in Spring's hierarchy. When its
     *       root cause is an SQL "data exception" (SQLSTATE class 22: a NUL character in a text
     *       value, an invalid byte sequence, a numeric overflow) the entry's own values were
     *       rejected, so it is {@link #POISON}: PostgreSQL will reject the same bytes on every
     *       delivery, and the entry must not be able to halt the service. Otherwise (class 23, an
     *       integrity constraint, or no SQLSTATE at all) it is {@link #FATAL}: a constraint the
     *       writer cannot violate unless the schema or the data is already wrong; in this service
     *       the only unique key the recorder could hit implies a committed event row, which the
     *       idempotency insert would have returned as a duplicate first.</li>
     *   <li>{@link InvalidDataAccessResourceUsageException} (bad SQL grammar, a column of the wrong
     *       type) and {@link InvalidDataAccessApiUsageException} (the code misused a repository or a
     *       constructor threw inside one) are {@link #FATAL}.</li>
     *   <li>Every other {@link DataAccessException} (a cancelled lock wait, a lost connection, a
     *       deadlock) and every {@link TransactionException} is {@link #RETRYABLE}: they say
     *       nothing about the entry. Spring's own transient/non-transient split is not used because
     *       it files a refused connection under non-transient.</li>
     *   <li>Anything else is {@link #RETRYABLE} too. It is most likely a bug, but a bounded number
     *       of deliveries costs little, while dead-lettering on the first sight of an unforeseen
     *       exception would throw away work a fix could have recovered.</li>
     * </ul>
     */
    public static FailureClass of(Throwable failure) {
        if (failure instanceof EnvelopeDecodeException || failure instanceof FareQuoteException) {
            return POISON;
        }
        if (failure instanceof DataIntegrityViolationException) {
            return isDataException(failure) ? POISON : FATAL;
        }
        if (failure instanceof InvalidDataAccessResourceUsageException
                || failure instanceof InvalidDataAccessApiUsageException) {
            return FATAL;
        }
        return RETRYABLE;
    }

    /** SQLSTATE class 22 ("data exception") at the root of the chain: the values, not the schema, were refused. */
    private static boolean isDataException(Throwable failure) {
        Throwable root = NestedExceptionUtils.getMostSpecificCause(failure);
        return root instanceof SQLException sql
                && sql.getSQLState() != null
                && sql.getSQLState().startsWith("22");
    }
}
