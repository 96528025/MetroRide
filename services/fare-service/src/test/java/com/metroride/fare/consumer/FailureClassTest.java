package com.metroride.fare.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import com.metroride.fare.events.EnvelopeDecodeException;
import com.metroride.fare.pricing.FareQuoteException;
import com.metroride.fare.pricing.FareQuoteException.Reason;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;

/** One case per branch of {@link FailureClass#of}, including the subclasses the consumer really sees. */
class FailureClassTest {

    @Test
    void anUndecodableEntryIsPoison() {
        assertThat(FailureClass.of(new EnvelopeDecodeException("event envelope in message 1-0 has no id")))
                .isEqualTo(FailureClass.POISON);
    }

    @Test
    void aPayloadThatCannotBeQuotedIsPoisonWhateverTheReason() {
        assertThat(FailureClass.of(new FareQuoteException(Reason.PAYLOAD, "no ride_id", null)))
                .isEqualTo(FailureClass.POISON);
        assertThat(FailureClass.of(new FareQuoteException(Reason.CALCULATION, "distance must not be negative", null)))
                .isEqualTo(FailureClass.POISON);
    }

    @Test
    void aDataAccessFailureIsRetryable() {
        // The lock-wait cancellation the transaction timeout produces...
        assertThat(FailureClass.of(new QueryTimeoutException("canceling statement due to user request")))
                .isEqualTo(FailureClass.RETRYABLE);
        // ...and a connection that cannot be obtained: Spring files it under "non-transient",
        // but for this consumer it is exactly the case a later retry fixes.
        assertThat(FailureClass.of(new DataAccessResourceFailureException("connection refused")))
                .isEqualTo(FailureClass.RETRYABLE);
    }

    @Test
    void aTransactionFailureIsRetryable() {
        assertThat(FailureClass.of(new CannotCreateTransactionException("pool exhausted")))
                .isEqualTo(FailureClass.RETRYABLE);
        assertThat(FailureClass.of(new TransactionSystemException("commit failed")))
                .isEqualTo(FailureClass.RETRYABLE);
    }

    @Test
    void anythingElseIsAProgrammingErrorAndPoison() {
        assertThat(FailureClass.of(new NullPointerException())).isEqualTo(FailureClass.POISON);
        assertThat(FailureClass.of(new IllegalStateException("bug"))).isEqualTo(FailureClass.POISON);
    }
}
